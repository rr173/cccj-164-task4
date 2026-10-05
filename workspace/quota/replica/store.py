"""灾备侧持久化 —— SQLite 任务状态。

灾备端有两类可见状态，严格分层：

1. **已确认（confirmed）**：通过全部链校验、在单个 SQLite 事务内与
   分段行一起提交的事件水位。只读查询投影只允许暴露这个水位的截面，
   因此绝不会出现「事件已进入、余额/租约/草案/命令结果只更新一部分」。
2. **隔离区（quarantine）**：未通过校验或不被当前任期接受的分段，
   原样留存、附原因，不参与投影、不推进边界。

所有任务状态（确认边界、任期、拉取游标、暂停标志、演练生命周期、
审计日志）都落盘；进程重启后从最后确认边界继续，演练地址与隔离
记录都保留。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any

from ..ledger import canonical

SCHEMA = """
CREATE TABLE IF NOT EXISTS replica_state (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    term          INTEGER NOT NULL DEFAULT 0,      -- 已确认主站任期
    confirmed_idx INTEGER NOT NULL DEFAULT 0,      -- 最后确认的分段序号
    confirmed_seq INTEGER NOT NULL DEFAULT 0,      -- 最后确认的事件水位
    last_digest   TEXT NOT NULL,                   -- 最后确认段摘要
    last_hash     TEXT NOT NULL,                   -- 最后确认事件哈希
    status        TEXT NOT NULL DEFAULT 'BOOTSTRAP',
    paused        INTEGER NOT NULL DEFAULT 0,      -- 拉取暂停（演练锁定期间）
    fence         INTEGER NOT NULL DEFAULT 0,      -- 单调递增的接管/换届令牌
    lag_limit     INTEGER NOT NULL DEFAULT 1000,
    max_event_seq INTEGER NOT NULL DEFAULT 0,      -- 主站回报的最新事件水位
    lag_events    INTEGER NOT NULL DEFAULT 0,
    updated_at    REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS seg_seen (
    idx           INTEGER PRIMARY KEY,             -- 收到过的分段序号
    term          INTEGER NOT NULL,
    digest        TEXT NOT NULL,                   -- 该 idx 收到的摘要
    applied       INTEGER NOT NULL,                -- 1=已应用 0=重复但内容不同(冲突)
    seen_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq          INTEGER PRIMARY KEY,
    event_type   TEXT NOT NULL,
    payload      TEXT NOT NULL,
    command_id   TEXT NOT NULL,
    fingerprint  TEXT NOT NULL,
    event_hash   TEXT NOT NULL,
    prev_hash    TEXT NOT NULL,
    recorded_at  TEXT NOT NULL,
    seg_index    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    idx          INTEGER PRIMARY KEY,
    term         INTEGER NOT NULL,
    batch_id     TEXT NOT NULL,
    from_seq     INTEGER NOT NULL,
    to_seq       INTEGER NOT NULL,
    prev_digest  TEXT NOT NULL,
    digest       TEXT NOT NULL,
    sealed_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS seg_commands (
    command_id   TEXT PRIMARY KEY,
    seg_index    INTEGER NOT NULL,
    result       TEXT,
    recorded_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    seg_index    INTEGER,
    term         INTEGER,
    reason       TEXT NOT NULL,
    detail       TEXT NOT NULL,
    body         TEXT NOT NULL,
    received_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS drills (
    id            TEXT PRIMARY KEY,
    idem_key      TEXT NOT NULL UNIQUE,
    status        TEXT NOT NULL,                   -- verifying/issued/failed/cancelled/expired
    term          INTEGER NOT NULL,
    locked_idx    INTEGER NOT NULL,
    locked_seq    INTEGER NOT NULL,
    locked_digest TEXT NOT NULL,
    token         TEXT,
    read_path     TEXT,
    ttl_seconds   INTEGER NOT NULL,
    issued_at     REAL,
    expires_at    REAL,
    created_at    REAL NOT NULL,
    finished_at   REAL,
    report        TEXT,
    fail_reason   TEXT
);
CREATE INDEX IF NOT EXISTS idx_drills_status ON drills(status);

CREATE TABLE IF NOT EXISTS audit_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    at      REAL NOT NULL,
    actor   TEXT NOT NULL,
    action  TEXT NOT NULL,
    detail  TEXT NOT NULL
);
"""

VALID_STATUS = {"BOOTSTRAP", "OK", "LAGGING", "QUARANTINED", "HALTED"}


class ReplicaStore:
    def __init__(self, path: str, lag_limit: int = 1000) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        row = self.conn.execute("SELECT id FROM replica_state WHERE id=1").fetchone()
        if row is None:
            from .protocol import GENESIS_DIGEST
            from ..ledger import GENESIS_HASH

            self.conn.execute(
                "INSERT INTO replica_state(id, term, confirmed_idx, confirmed_seq,"
                " last_digest, last_hash, status, lag_limit, updated_at)"
                " VALUES (1,0,0,0,?,?, 'BOOTSTRAP', ?, ?)",
                (GENESIS_DIGEST, GENESIS_HASH, lag_limit, time.time()),
            )
            self.conn.commit()
        else:
            self.conn.execute(
                "UPDATE replica_state SET lag_limit=? WHERE id=1 AND lag_limit <> ?",
                (lag_limit, lag_limit),
            )
            self.conn.commit()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ------------------------------------------------------------------
    # 任务状态
    # ------------------------------------------------------------------

    def state_row(self) -> sqlite3.Row:
        return self.conn.execute(
            "SELECT * FROM replica_state WHERE id=1"
        ).fetchone()

    def state(self) -> dict[str, Any]:
        r = self.state_row()
        return {
            "term": r["term"],
            "confirmed_index": r["confirmed_idx"],
            "confirmed_seq": r["confirmed_seq"],
            "last_digest": r["last_digest"],
            "last_hash": r["last_hash"],
            "status": r["status"],
            "paused": bool(r["paused"]),
            "fence": r["fence"],
            "lag_limit": r["lag_limit"],
            "primary_event_seq": r["max_event_seq"],
            "lag_events": r["lag_events"],
            "updated_at": r["updated_at"],
        }

    def update_state(self, **fields: Any) -> None:
        if not fields:
            return
        if "status" in fields and fields["status"] not in VALID_STATUS:
            raise ValueError(f"非法复制器状态: {fields['status']}")
        sets, params = [], []
        for k, v in fields.items():
            sets.append(f"{k}=?")
            params.append(int(v) if isinstance(v, bool) else v)
        params.append(1)
        self.conn.execute(
            f"UPDATE replica_state SET {', '.join(sets)} WHERE id=?", params
        )

    def set_paused(self, paused: bool) -> None:
        self.update_state(paused=1 if paused else 0)

    def bump_fence(self) -> int:
        self.conn.execute(
            "UPDATE replica_state SET fence=fence+1 WHERE id=1"
        )
        return self.state_row()["fence"]

    # ------------------------------------------------------------------
    # 去重 / 隔离
    # ------------------------------------------------------------------

    def seen(self, idx: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM seg_seen WHERE idx=?", (idx,)
        ).fetchone()

    def record_seen(self, idx: int, term: int, digest: str, applied: bool) -> None:
        self.conn.execute(
            "INSERT INTO seg_seen(idx, term, digest, applied, seen_at)"
            " VALUES(?,?,?,?,?) ON CONFLICT(idx) DO UPDATE SET"
            " term=excluded.term, digest=excluded.digest,"
            " applied=excluded.applied, seen_at=excluded.seen_at",
            (idx, term, digest, 1 if applied else 0, time.time()),
        )

    def quarantine(self, seg: dict[str, Any] | None, reason: str,
                   detail: str, idx: int | None = None,
                   term: int | None = None) -> int:
        idx = idx if idx is not None else (seg["index"] if seg else None)
        term = term if term is not None else (seg["term"] if seg else None)
        body = canonical(seg) if seg is not None else ""
        cur = self.conn.execute(
            "INSERT INTO quarantine(seg_index, term, reason, detail, body, received_at)"
            " VALUES(?,?,?,?,?,?)",
            (idx, term, reason, detail, body, time.time()),
        )
        return int(cur.lastrowid)

    def list_quarantine(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM quarantine ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 原子提交分段（调用方持有锁；整个方法在一个事务里）
    #
    # 投影折叠回调 ``project(events, command_results)`` 在同一事务的
    # SQLite 写入之后、提交之前执行——它只更新内存投影；若抛异常，
    # 事务回滚且内存投影由调用方保证不发生可见变化（见 replicator：
    # 投影在临界区内整体替换）。
    # ------------------------------------------------------------------

    def commit_segment(
        self,
        seg: dict[str, Any],
        project: Any,
    ) -> None:
        with self.conn:  # BEGIN .. COMMIT/ROLLBACK
            for e in seg["events"]:
                self.conn.execute(
                    "INSERT INTO events(seq,event_type,payload,command_id,"
                    "fingerprint,event_hash,prev_hash,recorded_at,seg_index)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        e["seq"],
                        e["event_type"],
                        e["payload"] if isinstance(e["payload"], str)
                        else canonical(e["payload"]),
                        e["command_id"],
                        e["fingerprint"],
                        e["event_hash"],
                        e["prev_hash"],
                        e["recorded_at"],
                        seg["index"],
                    ),
                )
            if seg.get("command_result") is not None:
                self.conn.execute(
                    "INSERT INTO seg_commands(command_id,seg_index,result,recorded_at)"
                    " VALUES(?,?,?,?)",
                    (
                        seg["batch_id"],
                        seg["index"],
                        canonical(seg["command_result"]),
                        seg["sealed_at"],
                    ),
                )
            self.conn.execute(
                "INSERT INTO segments(idx,term,batch_id,from_seq,to_seq,"
                "prev_digest,digest,sealed_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    seg["index"], seg["term"], seg["batch_id"],
                    seg["from_seq"], seg["to_seq"], seg["prev_digest"],
                    seg["digest"], seg["sealed_at"],
                ),
            )
            self.record_seen(seg["index"], seg["term"], seg["digest"], True)
            self.conn.execute(
                "UPDATE replica_state SET term=?, confirmed_idx=?, confirmed_seq=?,"
                " last_digest=?, last_hash=?, updated_at=? WHERE id=1",
                (
                    seg["term"],
                    seg["index"],
                    seg["to_seq"],
                    seg["digest"],
                    seg["events"][-1]["event_hash"],
                    time.time(),
                ),
            )
            # 内存投影折叠在事务提交临界区完成（replicator 持写锁）
            project(seg["events"], seg.get("command_result"), seg["batch_id"])

    # ------------------------------------------------------------------
    # 已确认数据读取（只读截面）
    # ------------------------------------------------------------------

    def load_confirmed_events(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT seq, event_type, payload FROM events ORDER BY seq"
        ).fetchall()
        return [
            {"seq": r["seq"], "type": r["event_type"], **json.loads(r["payload"])}
            for r in rows
        ]

    def load_confirmed_chain(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT seq,event_type,payload,command_id,fingerprint,event_hash,prev_hash,"
            "seg_index FROM events ORDER BY seq"
        ).fetchall()

    def command_results(self) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT command_id, seg_index, result FROM seg_commands"
        ).fetchall()
        return {
            r["command_id"]: {
                "seg_index": r["seg_index"],
                "result": json.loads(r["result"]) if r["result"] else None,
            }
            for r in rows
        }

    def event_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"]

    def seg_index_of_seq(self, seq: int) -> int | None:
        row = self.conn.execute(
            "SELECT seg_index FROM events WHERE seq<=? ORDER BY seq DESC LIMIT 1",
            (seq,),
        ).fetchone()
        return None if row is None else row["seg_index"]

    # ------------------------------------------------------------------
    # 演练持久化
    # ------------------------------------------------------------------

    def save_drill(self, d: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO drills(id,idem_key,status,term,locked_idx,locked_seq,"
            "locked_digest,token,read_path,ttl_seconds,issued_at,expires_at,"
            "created_at,finished_at,report,fail_reason)"
            " VALUES(:id,:idem_key,:status,:term,:locked_idx,:locked_seq,"
            " :locked_digest,:token,:read_path,:ttl_seconds,:issued_at,:expires_at,"
            " :created_at,:finished_at,:report,:fail_reason)"
            " ON CONFLICT(id) DO UPDATE SET status=excluded.status,"
            " token=excluded.token, read_path=excluded.read_path,"
            " issued_at=excluded.issued_at, expires_at=excluded.expires_at,"
            " finished_at=excluded.finished_at, report=excluded.report,"
            " fail_reason=excluded.fail_reason",
            {
                "id": d["id"],
                "idem_key": d["idem_key"],
                "status": d["status"],
                "term": d["term"],
                "locked_idx": d["locked_idx"],
                "locked_seq": d["locked_seq"],
                "locked_digest": d["locked_digest"],
                "token": d.get("token"),
                "read_path": d.get("read_path"),
                "ttl_seconds": d["ttl_seconds"],
                "issued_at": d.get("issued_at"),
                "expires_at": d.get("expires_at"),
                "created_at": d["created_at"],
                "finished_at": d.get("finished_at"),
                "report": canonical(d["report"]) if d.get("report") is not None else None,
                "fail_reason": d.get("fail_reason"),
            },
        )
        self.conn.commit()

    def get_drill(self, drill_id: str) -> dict[str, Any] | None:
        return self._drill_from_row(
            self.conn.execute("SELECT * FROM drills WHERE id=?", (drill_id,)).fetchone()
        )

    def get_drill_by_idem(self, idem_key: str) -> dict[str, Any] | None:
        return self._drill_from_row(
            self.conn.execute(
                "SELECT * FROM drills WHERE idem_key=?", (idem_key,)
            ).fetchone()
        )

    def get_drill_by_token(self, token: str) -> dict[str, Any] | None:
        return self._drill_from_row(
            self.conn.execute(
                "SELECT * FROM drills WHERE token=?", (token,)
            ).fetchone()
        )

    def list_drills(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM drills ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [self._drill_from_row(r) for r in rows]  # type: ignore[list-item]

    @staticmethod
    def _drill_from_row(r: sqlite3.Row | None) -> dict[str, Any] | None:
        if r is None:
            return None
        d = dict(r)
        d["report"] = json.loads(d["report"]) if d["report"] else None
        return d

    # ------------------------------------------------------------------
    # 审计日志
    # ------------------------------------------------------------------

    def audit_log(self, actor: str, action: str, detail: Any) -> None:
        self.conn.execute(
            "INSERT INTO audit_log(at,actor,action,detail) VALUES(?,?,?,?)",
            (time.time(), actor, action, canonical(detail) if not isinstance(detail, str)
             else detail),
        )
        self.conn.commit()

    def list_audit_log(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["detail"] = json.loads(d["detail"])
            except (json.JSONDecodeError, TypeError):
                pass
            out.append(d)
        return out

    def close(self) -> None:
        self.conn.close()
