"""受控切换演练管理器。

生命周期
--------
``create``（携带幂等键 ``idem_key``）
    锁定当前已确认截面 → **暂停复制拉取** → 短暂稳定窗口后再次核对
    主站水位 → 执行 :mod:`quota.replica.verify` 的全部检查 →
    通过则保存报告、状态 ``verified``，等待发放；
    「验证时又收到新分段」等任何失败 → 状态 ``failed``、恢复复制、
    不发放地址。
``issue``（再次请求）
    幂等：同一 ``idem_key`` 的演练重复请求返回原结果。发放带任期的
    只读查询地址（不透明 token），状态 ``issued``，可设 TTL。
``cancel``
    地址立即失效（状态 ``cancelled``，token 摘除），恢复复制。
TTL 到期 → 读地址返回 410（状态置 ``expired``）。

读取
----
``/ro/{token}/...`` 只能看到**锁定截面**：投影按 locked_seq 从已确认
事件独立折叠并缓存，与正在追赶的复制进度完全隔离。每个地址绑定
(term, locked_idx, locked_digest)；换届栅栏（fence）推进时全部作废旧地址。
"""

from __future__ import annotations

import secrets
import threading
import time
import uuid
from typing import Any

from ..domain import Projection, apply_event
from .verify import verify_snapshot


class DrillError(Exception):
    def __init__(self, status: int, code: str, message: str,
                 details: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.details = details


ACTIVE_DRILL_STATES = {"verifying", "verified", "issued"}


class DrillManager:
    def __init__(self, replicator: Any, *, settle_window: float = 0.15) -> None:
        self.rep = replicator
        self.store = replicator.store
        self.settle_window = settle_window
        self._pinned_lock = threading.RLock()
        # drill_id -> 折叠到 locked_seq 的不可变投影
        self._pinned: dict[str, Projection] = {}

    # ==================================================================
    # 创建 / 幂等
    # ==================================================================

    def create(self, body: dict[str, Any], actor: str = "operator") -> dict[str, Any]:
        idem_key = body.get("idem_key")
        if not idem_key or not isinstance(idem_key, str):
            raise DrillError(400, "idem_key_required",
                             "演练请求必须携带 idem_key（同一演练重复请求返回原结果）")

        with self.store.lock:
            existing = self.store.get_drill_by_idem(idem_key)
            if existing is not None:
                # 同一演练再次请求：返回原结果（绝不重复执行/重复发放）
                return self._view(existing, replayed=True)

            st = self.store.state()
            if st["status"] not in ("OK",):
                raise DrillError(
                    409, "replica_not_ready",
                    f"复制器当前状态 {st['status']}（仅 OK 截面可用于演练）",
                    details={"status": st["status"]},
                )
            if st["confirmed_index"] <= 0:
                raise DrillError(409, "empty_snapshot", "尚无已确认截面，无法演练")
            locked_index = st["confirmed_index"]
            primary_now = self._primary_index()

            drill_id = f"DR-{uuid.uuid4().hex[:12]}"
            ttl = int(body.get("ttl_seconds") or 3600)
            if ttl < 1:
                raise DrillError(400, "bad_ttl", "ttl_seconds 必须为正整数")

            drill = {
                "id": drill_id,
                "idem_key": idem_key,
                "status": "verifying",
                "term": st["term"],
                "locked_idx": locked_index,
                "locked_seq": st["confirmed_seq"],
                "locked_digest": st["last_digest"],
                "token": None,
                "read_path": None,
                "ttl_seconds": ttl,
                "issued_at": None,
                "expires_at": None,
                "created_at": time.time(),
                "finished_at": None,
                "report": None,
                "fail_reason": None,
            }
            self.store.save_drill(drill)
            self.store.audit_log(actor, "drill_create",
                                 {"drill_id": drill_id, "locked_index": locked_index})

            # 锁定截面：暂停拉取，使「验证时又收到新分段」可被检测
            self.store.set_paused(True)
            self.store.conn.commit()

            try:
                # 稳定窗口：让在途 tick 落定，再核对一次主站水位
                time.sleep(self.settle_window)
                primary_after = self._primary_index()
                effective_primary = max(primary_now, primary_after)

                # 即使主站暂时不可达，也以本地是否在暂停后仍有推进为准
                local_now = self.store.state()["confirmed_index"]
                advanced = (
                    local_now > locked_index
                    or (effective_primary or locked_index) > locked_index
                )
                if advanced:
                    raise _DrillFail(
                        "new_segments_during_verification",
                        f"演练验证时主站又密封了新分段（锁定 #{locked_index}，"
                        f"主站已到 #{effective_primary}），本次截面失效",
                    )

                report = verify_snapshot(
                    self.store,
                    self.rep.projection_digest,
                    locked_index=locked_index,
                    primary_index_now=effective_primary or locked_index,
                )
                if not report["ok"]:
                    failed = [c["name"] for c in report["checks"] if not c["ok"]]
                    raise _DrillFail(
                        "verification_failed",
                        f"截面验证未通过: {', '.join(failed)}",
                        report=report,
                    )

                drill["status"] = "verified"
                drill["locked_seq"] = report["locked_seq"]
                drill["locked_digest"] = report["locked_digest"]
                drill["report"] = report
                self.store.save_drill(drill)
                self.store.audit_log(
                    actor, "drill_verified",
                    {"drill_id": drill_id, "checks": report["checks"]},
                )
            except _DrillFail as fail:
                drill["status"] = "failed"
                drill["fail_reason"] = fail.reason
                drill["finished_at"] = time.time()
                drill["report"] = fail.report
                self.store.save_drill(drill)
                self.store.set_paused(False)
                self.store.conn.commit()
                self.store.audit_log(
                    actor, "drill_failed",
                    {"drill_id": drill_id, "reason": fail.reason},
                )
                raise DrillError(
                    409, fail.reason, str(fail),
                    details={"drill_id": drill_id, "report": fail.report},
                )
            finally:
                # 成功时保持暂停直到 issue/cancel（截面必须冻结）；
                # 失败已在上面恢复
                if drill["status"] != "verified":
                    self.store.set_paused(False)
                    self.store.conn.commit()

            return self._view(drill)

    # ==================================================================
    # 发放 / 查询 / 取消
    # ==================================================================

    def issue(self, drill_id: str, body: dict[str, Any] | None,
              actor: str = "operator") -> dict[str, Any]:
        with self.store.lock:
            drill = self._require(drill_id)
            if drill["status"] in ("cancelled", "expired"):
                raise DrillError(410, "drill_inactive",
                                 f"演练已{drill['status']}，地址不可用")
            if drill["status"] == "failed":
                raise DrillError(409, "drill_failed",
                                 "演练验证失败，不能发放地址；请新建演练")
            if drill["status"] == "issued":
                # 重复发放：返回原地址（幂等）
                return self._view(drill, replayed=True)
            if drill["status"] != "verified":
                raise DrillError(409, "drill_not_verified",
                                 f"演练状态 {drill['status']}，尚不能发放")

            token = secrets.token_hex(16)
            ttl = int((body or {}).get("ttl_seconds") or drill["ttl_seconds"])
            if ttl < 1:
                raise DrillError(400, "bad_ttl", "ttl_seconds 必须为正整数")
            now = time.time()
            drill["status"] = "issued"
            drill["token"] = token
            drill["read_path"] = f"/ro/{token}"
            drill["ttl_seconds"] = ttl
            drill["issued_at"] = now
            drill["expires_at"] = now + ttl
            self.store.save_drill(drill)
            # 发放后复制仍保持暂停：地址看到的截面必须与验证截面严格一致。
            # 取消/到期或显式 resume 后才继续追赶。
            self.store.audit_log(
                actor, "drill_issued",
                {"drill_id": drill_id, "term": drill["term"],
                 "locked_index": drill["locked_idx"], "ttl_seconds": ttl},
            )
            return self._view(drill)

    def cancel(self, drill_id: str, actor: str = "operator") -> dict[str, Any]:
        with self.store.lock:
            drill = self._require(drill_id)
            if drill["status"] in ("cancelled", "expired"):
                return self._view(drill, replayed=True)
            drill["status"] = "cancelled"
            drill["finished_at"] = time.time()
            # token 保留在库（供地址返回 410 与审计），只摘除对外 read_path
            drill["read_path"] = None
            self.store.save_drill(drill)
            self._invalidate_pinned(drill_id)
            self.store.set_paused(False)
            self.store.bump_fence()
            self.store.conn.commit()
            self.store.audit_log(actor, "drill_cancelled", {"drill_id": drill_id})
            return self._view(drill)

    def get(self, drill_id: str) -> dict[str, Any]:
        with self.store.lock:
            return self._view(self._require(drill_id))

    def list(self) -> list[dict[str, Any]]:
        with self.store.lock:
            return [self._view(d) for d in self.store.list_drills()]

    def resume_replication(self, actor: str = "operator") -> dict[str, Any]:
        """演练结束（取消/到期）后显式恢复复制。"""
        with self.store.lock:
            self.store.set_paused(False)
            self.store.conn.commit()
            self.store.audit_log(actor, "drill_replication_resumed", {})
            return self.rep.store.state()

    # ==================================================================
    # 带任期只读地址
    # ==================================================================

    def resolve(self, token: str) -> dict[str, Any]:
        with self.store.lock:
            drill = self.store.get_drill_by_token(token)
            if drill is None:
                raise DrillError(404, "address_not_found", "只读地址不存在")
            self._expire_if_needed(drill)
            if drill["status"] != "issued":
                raise DrillError(410, "address_inactive",
                                 f"只读地址已失效（演练状态 {drill['status']}）")
            if drill["expires_at"] and time.time() > drill["expires_at"]:
                self._mark_expired(drill)
                raise DrillError(410, "address_expired", "只读地址已过 TTL")
            return self._view(drill)

    def pinned_state(self, token: str) -> dict[str, Any]:
        """返回锁定截面的状态视图（绝不会看到锁定之后的事件）。"""
        view = self.resolve(token)
        drill_id = view["drill_id"]
        with self._pinned_lock:
            proj = self._pinned.get(drill_id)
            if proj is None:
                with self.store.lock:
                    drill = self.store.get_drill(drill_id)
                    locked_seq = drill["locked_seq"]
                    rows = self.store.conn.execute(
                        "SELECT seq,event_type,payload FROM events"
                        " WHERE seq<=? ORDER BY seq", (locked_seq,)
                    ).fetchall()
                import json
                proj = Projection()
                for r in rows:
                    apply_event(
                        proj,
                        {"seq": r["seq"], "type": r["event_type"],
                         **json.loads(r["payload"])},
                    )
                self._pinned[drill_id] = proj
        state = proj.to_state()
        state["snapshot"] = {
            "drill_id": view["drill_id"],
            "term": view["term"],
            "locked_index": view["locked_index"],
            "locked_seq": view["locked_seq"],
            "locked_digest": view["locked_digest"],
            "issued_at": view["issued_at"],
            "expires_at": view["expires_at"],
            "read_only": True,
        }
        return state

    def expire_due(self) -> int:
        """扫描并过期到 TTL 的已发演练（可被后台线程/请求惰性触发）。"""
        n = 0
        with self.store.lock:
            for d in self.store.list_drills(limit=200):
                if (
                    d["status"] == "issued"
                    and d["expires_at"]
                    and time.time() > d["expires_at"]
                ):
                    self._mark_expired(d)
                    n += 1
        return n

    # ==================================================================
    # 内部
    # ==================================================================

    def _primary_index(self) -> int | None:
        try:
            status = self.rep.fetcher.fetch_status()
            self.store.update_state(max_event_seq=status["current_event_seq"])
            return status["sealed_index"]
        except Exception:
            return None

    def _expire_if_needed(self, drill: dict[str, Any]) -> None:
        if (
            drill["status"] == "issued"
            and drill["expires_at"]
            and time.time() > drill["expires_at"]
        ):
            self._mark_expired(drill)

    def _mark_expired(self, drill: dict[str, Any]) -> None:
        drill["status"] = "expired"
        drill["finished_at"] = time.time()
        # token 保留在库（旧地址稳定返回 410），只摘除对外 read_path
        drill["read_path"] = None
        self.store.save_drill(drill)
        self._invalidate_pinned(drill["id"])
        self.store.set_paused(False)
        self.store.bump_fence()
        self.store.conn.commit()
        self.store.audit_log("system", "drill_expired", {"drill_id": drill["id"]})

    def _invalidate_pinned(self, drill_id: str) -> None:
        with self._pinned_lock:
            self._pinned.pop(drill_id, None)

    def _require(self, drill_id: str) -> dict[str, Any]:
        drill = self.store.get_drill(drill_id)
        if drill is None:
            raise DrillError(404, "drill_not_found", f"演练不存在: {drill_id}")
        return drill

    @staticmethod
    def _view(drill: dict[str, Any], replayed: bool = False) -> dict[str, Any]:
        out = {
            "drill_id": drill["id"],
            "idem_key": drill["idem_key"],
            "status": drill["status"],
            "term": drill["term"],
            "locked_index": drill["locked_idx"],
            "locked_seq": drill["locked_seq"],
            "locked_digest": drill["locked_digest"],
            "read_path": drill.get("read_path"),
            "ttl_seconds": drill["ttl_seconds"],
            "issued_at": drill.get("issued_at"),
            "expires_at": drill.get("expires_at"),
            "created_at": drill["created_at"],
            "finished_at": drill.get("finished_at"),
            "fail_reason": drill.get("fail_reason"),
            "report": drill.get("report"),
        }
        if replayed:
            out["replayed"] = True
        return out

    @staticmethod
    def invalidate_all_for_term(store: Any, old_term: int) -> int:
        """换届栅栏：作废所有未完成演练地址（cancel/expire 状态不动）。"""
        n = 0
        for d in store.list_drills(limit=1000):
            if d["status"] in ACTIVE_DRILL_STATES:
                d["status"] = "cancelled"
                d["finished_at"] = time.time()
                d["fail_reason"] = "invalidated_by_term_change"
                d["read_path"] = None
                store.save_drill(d)
                store.set_paused(False)
                n += 1
        return n


class _DrillFail(Exception):
    def __init__(self, reason: str, message: str, report: Any = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.report = report
