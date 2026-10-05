"""主站侧 —— 事件分段密封器（replication feed）。

设计要点
--------
* 分段在 ``EventStore.append_batch`` 的**同一个 SQLite 事务**里写入
  ``feed_segments`` 表：分段与事件要么同时可见，要么都不可见，主站
  崩溃不会留下「事件已落账但分段缺失」的窗口（灾备侧等待即可，不
  会产生真正的缺口）。
* 一次命令批次（append_batch 的一次调用）恰好密封为一个分段，因此
  批次永远不会被拆分；同批事件 command_id 相同、seq 连续。
* 段序号 ``index`` 全局严格递增、跨任期连续；``to_seq`` 随事件水位
  单调。``term`` 在主站进程每次启动时 +1，也可显式换届。
* 段链 ``prev_digest → digest`` 与事件内部哈希链相互锚定。

仅在灾备接口需要时通过 :func:`install_feed` 安装；现有纯主站部署
（原 220 项测试）不安装，行为完全不变。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, Callable

from ..ledger import GENESIS_HASH, canonical
from .protocol import GENESIS_DIGEST, segment_digest

FEED_SCHEMA = """
CREATE TABLE IF NOT EXISTS feed_meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS feed_segments (
    term       INTEGER NOT NULL,
    idx        INTEGER PRIMARY KEY,
    body       TEXT NOT NULL,
    sealed_at  REAL NOT NULL
);
"""


class SegmentFeed:
    """拥有 feed_* 表，并作为 append_batch 的事务内钩子密封分段。"""

    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock,
                 bump_on_attach: bool = True) -> None:
        self.conn = conn
        self.lock = lock
        with self.lock:
            conn.executescript(FEED_SCHEMA)
            conn.commit()
            existed = self._meta("term") is not None
            if not existed:
                # 首次安装：任期 1
                self._put_meta("term", 1)
            elif bump_on_attach:
                # 同一数据库被新的主站进程打开：换届
                self._put_meta("term", int(self._meta("term")) + 1)
            self._term = int(self._meta("term"))
            # 故障注入开关（仅演练/测试用），序列化为 JSON list[int]
            if self._meta("tamper_digest") is None:
                self._put_meta("tamper_digest", "[]")
            if self._meta("tamper_payload") is None:
                self._put_meta("tamper_payload", "[]")
            if self._meta("drop_index") is None:
                self._put_meta("drop_index", "[]")

    # ------------------------------------------------------------------
    # meta
    # ------------------------------------------------------------------

    def _meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM feed_meta WHERE key=?", (key,)
        ).fetchone()
        return None if row is None else row["value"]

    def _put_meta(self, key: str, value: Any) -> None:
        self.conn.execute(
            "INSERT INTO feed_meta(key,value) VALUES(?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value) if not isinstance(value, str) else value),
        )

    @property
    def term(self) -> int:
        with self.lock:
            return int(self._meta("term"))

    def bump_term(self) -> int:
        """显式换届（运维 / 演练测试）。"""
        with self.lock:
            nxt = int(self._meta("term")) + 1
            self._put_meta("term", nxt)
            self._term = nxt
            return nxt

    # ------------------------------------------------------------------
    # 事务内密封钩子（由 EventStore.append_batch 在同一事务中调用）
    # ------------------------------------------------------------------

    def seal_hook(self, conn: sqlite3.Connection, seqs: list[int],
                  command_id: str, fingerprint: str, result: dict[str, Any] | None,
                  system: bool) -> None:
        first_seq, last_seq = seqs[0], seqs[-1]
        rows = conn.execute(
            "SELECT seq, event_type, payload, command_id, fingerprint,"
            " event_hash, prev_hash, recorded_at FROM events"
            " WHERE seq >= ? AND seq <= ? ORDER BY seq",
            (first_seq, last_seq),
        ).fetchall()
        events = []
        for r in rows:
            events.append(
                {
                    "seq": r["seq"],
                    "event_type": r["event_type"],
                    "payload": json.loads(r["payload"]),
                    "command_id": r["command_id"],
                    "fingerprint": r["fingerprint"],
                    "event_hash": r["event_hash"],
                    "prev_hash": r["prev_hash"],
                    "recorded_at": r["recorded_at"],
                }
            )
        # 不变量：一批事件 command_id 全相同、seq 与传入一致
        assert [e["seq"] for e in events] == seqs
        assert all(e["command_id"] == command_id for e in events)

        last = conn.execute(
            "SELECT idx, body FROM feed_segments ORDER BY idx DESC LIMIT 1"
        ).fetchone()
        if last is None:
            index = 1
            prev_digest = GENESIS_DIGEST
            expected_chain_prev = GENESIS_HASH
        else:
            index = last["idx"] + 1
            prev_body = json.loads(last["body"])
            prev_digest = prev_body["digest"]
            expected_chain_prev = prev_body["events"][-1]["event_hash"]
        # 跨段事件链锚定：本段首事件必须指向上段末事件
        assert events[0]["prev_hash"] == expected_chain_prev, (
            "feed seal: 跨段事件链不连续"
        )

        seg = {
            "term": self._term,
            "index": index,
            "batch_id": command_id,
            "from_seq": first_seq,
            "to_seq": last_seq,
            "command_result": None if system else (result or {}),
            "events": events,
            "prev_digest": prev_digest,
            "digest": "",
            "sealed_at": time.time(),
        }
        seg["digest"] = segment_digest(seg)
        conn.execute(
            "INSERT INTO feed_segments(term, idx, body, sealed_at) VALUES(?,?,?,?)",
            (self._term, index, canonical(seg), seg["sealed_at"]),
        )

    # ------------------------------------------------------------------
    # 读取（HTTP feed）
    # ------------------------------------------------------------------

    def status(self) -> dict[str, Any]:
        with self.lock:
            last = self.conn.execute(
                "SELECT idx, sealed_at FROM feed_segments ORDER BY idx DESC LIMIT 1"
            ).fetchone()
            ev = self.conn.execute(
                "SELECT MAX(seq) AS m FROM events"
            ).fetchone()
            return {
                "term": int(self._meta("term")),
                "sealed_index": 0 if last is None else last["idx"],
                "sealed_at": None if last is None else last["sealed_at"],
                "current_event_seq": ev["m"] or 0,
            }

    def fetch(self, after_index: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT body FROM feed_segments WHERE idx > ? ORDER BY idx LIMIT ?",
                (after_index, max(1, min(limit, 1000))),
            ).fetchall()
            tamper_digest = set(json.loads(self._meta("tamper_digest") or "[]"))
            tamper_payload = set(json.loads(self._meta("tamper_payload") or "[]"))
            drop_index = set(json.loads(self._meta("drop_index") or "[]"))
        out: list[dict[str, Any]] = []
        for r in rows:
            seg = json.loads(r["body"])
            if seg["index"] in drop_index:
                continue
            if seg["index"] in tamper_digest:
                seg["digest"] = _flip(seg["digest"])
            if seg["index"] in tamper_payload:
                # 篡改首事件载荷但不重算任何摘要：
                # 段摘要与事件哈希都会失配（校验在 digest 处即被拦下）
                ev0 = seg["events"][0]
                ev0["payload"] = {**ev0["payload"], "__tampered__": True}
            out.append(seg)
        return out

    def get_segment(self, index: int) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT body FROM feed_segments WHERE idx=?", (index,)
            ).fetchone()
        return None if row is None else json.loads(row["body"])

    # ------------------------------------------------------------------
    # 故障注入（仅测试/演练）
    # ------------------------------------------------------------------

    def inject(self, kind: str, index: int) -> dict[str, Any]:
        key = {"digest": "tamper_digest", "payload": "tamper_payload",
               "drop": "drop_index"}[kind]
        with self.lock:
            vals = set(json.loads(self._meta(key) or "[]"))
            vals.add(int(index))
            self._put_meta(key, canonical(sorted(vals)))
            return {"kind": kind, "index": index, "active": sorted(vals)}

    def clear_injection(self) -> dict[str, Any]:
        with self.lock:
            for key in ("tamper_digest", "tamper_payload", "drop_index"):
                self._put_meta(key, "[]")
        return {"ok": True}


def _flip(hexdigest: str) -> str:
    bad = "0" if hexdigest[0] != "0" else "1"
    return bad + hexdigest[1:]


def install_feed(service: Any, bump_on_attach: bool = True) -> SegmentFeed:
    """在一个已构造的 QuotaService 上安装分段密封器。

    把密封钩子注册进 EventStore：钩子在 append_batch 的同一事务内
    执行，因此分段与业务事件原子可见。
    """
    store = service.store
    feed = SegmentFeed(store.conn, store.lock, bump_on_attach=bump_on_attach)

    def hook(conn: sqlite3.Connection, seqs: list[int], command_id: str,
             fingerprint: str, result: dict[str, Any] | None, system: bool) -> None:
        feed.seal_hook(conn, seqs, command_id, fingerprint, result, system)

    store.batch_hook = hook  # type: ignore[attr-defined]
    service.feed = feed  # type: ignore[attr-defined]
    return feed
