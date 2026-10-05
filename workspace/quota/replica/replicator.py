"""灾备复制器核心。

拉取循环
--------
后台线程周期性执行 :meth:`Replicator.tick`：

1. ``fetch_status`` 读取主站任期与最新事件水位（算落后量）；
2. ``fetch_segments(after=confirmed_idx)`` 拉取下一批分段；
3. 对每个分段顺序做 **准入 + 双链校验**，通过的分段在单个 SQLite
   事务内落账，并在同一临界区折叠内存投影 —— 对外可见的确认边界
   只在事务提交后前进，因此读侧永远看不到「事件进了、投影只更新
   一半」的截面；
4. 任何不合法分段写入**隔离区**并把状态置为 QUARANTINED（停止推进，
   等待运维处理）；内容一致的重复拉取直接忽略（幂等，不隔离）。

任期栅栏
--------
* 主站 term 前进（换届）→ 分段隔离为 ``new_term`` 并暂停，必须运维
  显式 :meth:`adopt_term`（换届时同时取消全部已发演练地址）；
* 收到 term **落后**于已确认任期的分段 → 隔离为 ``stale_term``
  （旧任期进程迟到更新），绝不污染最近可用截面；
* 所有受控操作（换届接管、演练取消、重置）都推进持久化的 ``fence``
  单调令牌。

内存投影只折叠到*已确认*水位；演练锁定的历史截面按 locked_seq 从
已确认事件独立折叠（见 drill.py）。
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable

from ..domain import Projection, apply_event
from ..ledger import GENESIS_HASH
from . import protocol as P
from .store import ReplicaStore


class SegmentRejected(Exception):
    """分段未通过准入/校验。``reason`` 为 protocol.REASON_TEXT 的键。"""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


class _RWLock:
    """可重入读写锁：读多写一（tick 写、HTTP 读）。"""

    def __init__(self) -> None:
        self._cond = threading.Condition(threading.RLock())
        self._readers = 0

    def acquire_read(self) -> None:
        with self._cond:
            self._readers += 1

    def release_read(self) -> None:
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def acquire_write(self) -> None:
        self._cond.acquire()
        while self._readers > 0:
            self._cond.wait()

    def release_write(self) -> None:
        self._cond.release()


class ReadProjection:
    """仅折叠到「已确认边界」的内存投影；写时整体受控于读写锁。"""

    def __init__(self) -> None:
        self.rw = _RWLock()
        self.proj = Projection()

    def rebuild(self, events: list[dict[str, Any]]) -> None:
        p = Projection()
        for evt in events:
            apply_event(p, evt)
        self.proj = p

    def apply_segment(self, events: list[dict[str, Any]],
                      _result: Any, _batch_id: str) -> None:
        for e in events:
            payload = e["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            apply_event(
                self.proj,
                {"seq": e["seq"], "type": e["event_type"], **payload},
            )


# ----------------------------------------------------------------------
# 纯函数式校验：喂给它「已确认边界」与分段，返回拒绝原因或 None
# ----------------------------------------------------------------------


def validate_segment(
    seg: Any,
    *,
    confirmed_index: int,
    confirmed_seq: int,
    last_digest: str,
    last_hash: str,
    confirmed_term: int,
    accepted_term: int | None,
    seen_digest_at: Callable[[int], str | None],
) -> str | None:
    """返回 None 表示通过；否则返回 REASON_TEXT 原因码。

    顺序刻意从廉价到昂贵：结构 → 位置/去重 → 任期（旧/新任期先隔离，
    其内容自洽与否已无意义）→ 自摘要 → 段链 → 事件链 → 序号连续
    → 批次完整性。
    """
    if not P.well_formed(seg):
        return "malformed"

    idx = seg["index"]
    if idx <= confirmed_index:
        # 已应用范围：内容一致即幂等重复；不同即「同序号正文不同」
        prior = seen_digest_at(idx)
        if prior is None:
            return "out_of_order"
        if prior != seg["digest"]:
            return "same_index_diff_body"
        return "duplicate"
    if idx != confirmed_index + 1:
        return "gap"

    # 任期：旧任期迟到 / 未接管的新任期 / 空库首段先登记任期锚点。
    # 这里只决定「任期通过/拒绝」，通过后仍继续做自摘要与双链校验。
    if seg["term"] < confirmed_term:
        return "stale_term"
    if confirmed_term == 0:
        # 空库首段：主站从 term 1 起；灾备已登记预期任期时必须精确匹配
        if accepted_term is not None and seg["term"] != accepted_term:
            return "new_term"
    elif seg["term"] > confirmed_term:
        # 任期前进：只有已显式接管（accepted_term）且不越界才放行
        if accepted_term is None or seg["term"] > accepted_term:
            return "new_term"

    if P.segment_digest(seg) != seg["digest"]:
        return "digest_mismatch"

    if seg["prev_digest"] != last_digest:
        return "chain_broken"

    if seg["from_seq"] != confirmed_seq + 1:
        return "seq_mismatch"
    seqs = [e["seq"] for e in seg["events"]]
    if seqs != list(range(seg["from_seq"], seg["to_seq"] + 1)):
        return "seq_mismatch"

    ok, _bad = P.verify_event_hashes(seg, last_hash)
    if not ok:
        # 区分「载荷被改导致重算不符」与「prev_hash 断链」
        first_prev_bad = seg["events"][0]["prev_hash"] != last_hash
        return "event_chain_broken" if first_prev_bad else "event_payload_tampered"

    # 批次完整：一段内 command_id 必须全部相同，且 seq 连续已在上保证
    cids = {e["command_id"] for e in seg["events"]}
    if len(cids) != 1 or next(iter(cids)) != seg["batch_id"]:
        return "batch_split"

    return None


class Replicator:
    def __init__(
        self,
        store: ReplicaStore,
        fetcher: Any,
        *,
        poll_interval: float = 0.5,
        page_size: int = 100,
        auto_adopt_term: bool = False,
        actor: str = "replicator",
    ) -> None:
        self.store = store
        self.fetcher = fetcher
        self.poll_interval = poll_interval
        self.page_size = page_size
        self.auto_adopt_term = auto_adopt_term
        self.actor = actor
        self.read = ReadProjection()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # 已显式接管、但尚未收到该任期首段的任期号
        self._accepted_term: int | None = None
        self._rebuild()

    # ------------------------------------------------------------------
    # 启动 / 重启恢复
    # ------------------------------------------------------------------

    def _rebuild(self) -> None:
        """进程重启：从已确认事件重建投影，游标即最后确认边界。"""
        with self.store.lock:
            events = self.store.load_confirmed_events()
            self.read.rebuild(events)
            st = self.store.state()
            # 重启后没有待接管任期；若已确认过任期，沿用它作为接受上界
            self._accepted_term = st["term"] or None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="dr-replicator", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.wait(self.poll_interval):
            try:
                self.tick()
            except Exception as exc:  # 拉取失败绝不能杀死复制线程
                self._note_fetch_error(repr(exc))

    # ------------------------------------------------------------------
    # 单轮拉取（也供测试直接驱动）
    # ------------------------------------------------------------------

    def tick(self) -> dict[str, Any]:
        with self.store.lock:
            st0 = self.store.state()
            if st0["paused"]:
                return {"skipped": "paused", **self.store.state()}
            if st0["status"] == "QUARANTINED":
                # 已隔离熔断：未经运维处置不再继续推进
                return {"skipped": "quarantined", **self.store.state()}

            status = self._safe_primary_status()
            if status is None:
                return {"skipped": "primary_unreachable", **self.store.state()}

            lag = max(0, status["current_event_seq"] - st0["confirmed_seq"])
            fields: dict[str, Any] = {
                "max_event_seq": status["current_event_seq"],
                "lag_events": lag,
            }
            over_lag = lag > st0["lag_limit"]
            if over_lag:
                fields["status"] = "LAGGING"
            self.store.update_state(**fields)

            applied: list[int] = []
            duplicates = 0
            for seg in self.fetcher.fetch_segments(
                after_index=st0["confirmed_index"], limit=self.page_size
            ):
                outcome = self.ingest(seg, primary_term=status["term"])
                if outcome == "applied":
                    applied.append(seg["index"])
                elif outcome == "duplicate":
                    duplicates += 1
                # rejected 已经在 ingest 内隔离并熔断
                if self.store.state()["status"] == "QUARANTINED":
                    break

            st = self.store.state()
            if not over_lag and st["status"] == "LAGGING":
                # 已追平
                self.store.update_state(
                    status="OK" if st["confirmed_seq"] > 0 else "BOOTSTRAP"
                )
            elif st["status"] == "BOOTSTRAP" and st["confirmed_seq"] > 0:
                self.store.update_state(status="OK")
            self.store.conn.commit()
            return {
                "applied": applied,
                "duplicates": duplicates,
                "lag_events": lag,
                **self.store.state(),
            }

    def _safe_primary_status(self) -> dict[str, Any] | None:
        try:
            return self.fetcher.fetch_status()
        except Exception:
            return None

    def _note_fetch_error(self, message: str) -> None:
        with self.store.lock:
            self.store.update_state()
            self.store.audit_log(self.actor, "fetch_error", {"error": message})

    # ------------------------------------------------------------------
    # 分段准入（单段）。返回 applied / duplicate / rejected
    # ------------------------------------------------------------------

    def ingest(self, seg: Any, primary_term: int | None = None) -> str:
        with self.store.lock:
            st = self.store.state()
            accepted = self._accepted_term
            reason = validate_segment(
                seg,
                confirmed_index=st["confirmed_index"],
                confirmed_seq=st["confirmed_seq"],
                last_digest=st["last_digest"],
                last_hash=st["last_hash"],
                confirmed_term=st["term"],
                accepted_term=accepted,
                seen_digest_at=lambda i: (
                    d["digest"] if (d := self.store.seen(i)) is not None else None
                ),
            )

            if reason == "new_term" and self.auto_adopt_term and primary_term is not None:
                if (
                    isinstance(seg, dict)
                    and seg.get("term") == primary_term
                    and self._can_auto_adopt(primary_term, st)
                ):
                    self._do_adopt_term(primary_term, st, reason="auto")
                    st = self.store.state()
                    reason = validate_segment(
                        seg,
                        confirmed_index=st["confirmed_index"],
                        confirmed_seq=st["confirmed_seq"],
                        last_digest=st["last_digest"],
                        last_hash=st["last_hash"],
                        confirmed_term=st["term"],
                        accepted_term=self._accepted_term,
                        seen_digest_at=lambda i: (
                            d["digest"] if (d := self.store.seen(i)) is not None else None
                        ),
                    )

            if reason == "duplicate":
                return "duplicate"
            if reason is not None:
                self._quarantine(seg, reason)
                return "rejected"

            # 通过：事务内落账 + 临界区内折叠投影
            self.read.rw.acquire_write()
            try:
                self.store.commit_segment(seg, self.read.apply_segment)
                self.store.conn.commit()
            finally:
                self.read.rw.release_write()
            if seg["term"] == self._accepted_term:
                self._accepted_term = None  # 新任期首段已落地
            self.store.audit_log(
                self.actor,
                "segment_applied",
                {"index": seg["index"], "term": seg["term"],
                 "to_seq": seg["to_seq"], "batch_id": seg["batch_id"]},
            )
            return "applied"

    @staticmethod
    def _can_auto_adopt(primary_term: int, st: dict[str, Any]) -> bool:
        # 空库首段允许直接锚定主站当前任期（全新引导）；
        # 已复制过则只允许严格的下一个任期
        if st["confirmed_index"] == 0:
            return primary_term >= 1
        return primary_term == st["term"] + 1

    def _quarantine(self, seg: Any, reason: str) -> None:
        detail = P.REASON_TEXT.get(reason, reason)
        idx = seg.get("index") if isinstance(seg, dict) else None
        term = seg.get("term") if isinstance(seg, dict) else None
        extra = ""
        if isinstance(seg, dict):
            st = self.store.state()
            extra = (
                f"（收到 index={idx} term={term}；"
                f"已确认 index={st['confirmed_index']} term={st['term']}）"
            )
        self.store.quarantine(
            seg if isinstance(seg, dict) else None,
            reason,
            detail + extra,
            idx=idx,
            term=term,
        )
        self.store.update_state(status="QUARANTINED")
        self.store.conn.commit()
        self.store.audit_log(
            self.actor, "segment_quarantined",
            {"index": idx, "term": term, "reason": reason, "detail": detail + extra},
        )

    # ------------------------------------------------------------------
    # 运维操作
    # ------------------------------------------------------------------

    def adopt_term(self, term: int, actor: str = "operator") -> dict:
        """显式接管主站任期：设为接受上界、推进 fence、作废旧地址。

        空库时允许直接锚定主站当前任期（全新灾备引导）；已复制后只能
        接管严格更高的任期。
        """
        with self.store.lock:
            st = self.store.state()
            if st["confirmed_index"] == 0:
                if term < 1:
                    raise ValueError("空库锚定任期必须 >= 1")
            elif term <= st["term"]:
                raise ValueError(f"只能接管更高任期：当前已确认 {st['term']}")
            self._do_adopt_term(term, st, reason="manual", actor=actor)
            return self.store.state()

    def _do_adopt_term(self, term: int, st: dict[str, Any], reason: str,
                       actor: str | None = None) -> None:
        from .drill import DrillManager  # 避免循环导入

        self._accepted_term = term
        self.store.bump_fence()
        # 换届使所有已发/验证中的演练地址失效（它们绑定旧任期截面）
        DrillManager.invalidate_all_for_term(self.store, st["term"])
        if st["status"] == "QUARANTINED":
            # 若仅因 new_term 熔断，接管后解除（其它原因的隔离记录保留审计）
            if self._only_blocker_is_new_term():
                self.store.update_state(
                    status="OK" if st["confirmed_seq"] > 0 else "BOOTSTRAP"
                )
        self.store.conn.commit()
        self.store.audit_log(
            actor or self.actor, "term_adopted",
            {"term": term, "previous_term": st["term"], "reason": reason},
        )

    def _only_blocker_is_new_term(self) -> bool:
        rows = self.store.list_quarantine(limit=1)
        return bool(rows) and rows[0]["reason"] in ("new_term", "stale_term")

    def resume(self, actor: str = "operator") -> dict[str, Any]:
        """运维确认后解除隔离熔断（隔离记录保留）。"""
        with self.store.lock:
            st = self.store.state()
            if st["status"] == "QUARANTINED":
                self.store.update_state(
                    status="OK" if st["confirmed_seq"] > 0 else "BOOTSTRAP"
                )
                self.store.bump_fence()
                self.store.conn.commit()
                self.store.audit_log(actor, "replication_resumed",
                                     {"confirmed_index": st["confirmed_index"]})
            return self.store.state()

    def set_paused(self, paused: bool, actor: str = "operator") -> None:
        with self.store.lock:
            self.store.set_paused(paused)
            self.store.conn.commit()
            self.store.audit_log(actor, "replication_paused" if paused
                                 else "replication_unpaused", {})

    # ------------------------------------------------------------------
    # 只读快照
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        self.read.rw.acquire_read()
        try:
            return self.read.proj.to_state()
        finally:
            self.read.rw.release_read()

    def projection_digest(self) -> Any:
        self.read.rw.acquire_read()
        try:
            return self.read.proj.digest()
        finally:
            self.read.rw.release_read()
