"""灾备审计工具。

两种用法：
* :func:`audit_database` —— 直接打开一个灾备 SQLite 任务库离线核验
  （命令行 ``python -m quota.replica.audit --db dr.db``）；
* :func:`audit_live` —— 对运行中的复制器做同样检查，并额外校验
  内存确认投影与库内事件重放一致。

输出结构与主站 ``/api/audit`` 风格统一：``{ok, checks[], summary}``。
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

from ..domain import Projection, apply_event
from ..ledger import GENESIS_HASH
from . import protocol as P


def _check(name: str, ok: bool, detail: str, **extra: Any) -> dict[str, Any]:
    out = {"name": name, "ok": bool(ok), "detail": detail}
    out.update(extra)
    return out


def audit_database(path: str, *, live_digest: Any = None,
                   primary_status: dict[str, Any] | None = None) -> dict[str, Any]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return _audit(conn, live_digest=live_digest, primary_status=primary_status)
    finally:
        conn.close()


def audit_live(replicator: Any) -> dict[str, Any]:
    with replicator.store.lock:
        primary = None
        try:
            primary = replicator.fetcher.fetch_status()
        except Exception:
            primary = None
        return _audit(
            replicator.store.conn,
            live_digest=replicator.projection_digest,
            primary_status=primary,
        )


def _audit(conn: sqlite3.Connection, *, live_digest: Any,
           primary_status: dict[str, Any] | None) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    st = conn.execute("SELECT * FROM replica_state WHERE id=1").fetchone()
    seg_rows = conn.execute("SELECT * FROM segments ORDER BY idx").fetchall()
    ev_rows = conn.execute(
        "SELECT seq,event_type,payload,command_id,fingerprint,event_hash,"
        "prev_hash,seg_index FROM events ORDER BY seq"
    ).fetchall()

    # 1) 边界一致：segments/events/state 三者对得上
    last_seg = seg_rows[-1] if seg_rows else None
    boundary_ok = (
        st["confirmed_idx"] == (last_seg["idx"] if last_seg else 0)
        and st["confirmed_seq"] == (last_seg["to_seq"] if last_seg else 0)
        and st["confirmed_seq"] == len(ev_rows)
        and (last_seg is None or st["last_digest"] == last_seg["digest"])
    )
    checks.append(_check(
        "confirmed_boundary", boundary_ok,
        f"state(idx={st['confirmed_idx']},seq={st['confirmed_seq']}) / "
        f"segments({len(seg_rows)}) / events({len(ev_rows)}) 一致"
        if boundary_ok
        else "确认边界与段表/事件表不一致",
    ))

    # 2) 段链完整
    seg_ok, seg_detail, prev_digest = True, "", P.GENESIS_DIGEST
    seg_by_idx = {r["idx"]: r for r in seg_rows}
    expected_idx = 1
    for r in seg_rows:
        if r["idx"] != expected_idx:
            seg_ok, seg_detail = False, f"段序号在 #{expected_idx} 处缺失/跳跃"
            break
        if r["prev_digest"] != prev_digest:
            seg_ok, seg_detail = False, f"段 #{r['idx']} prev_digest 断链"
            break
        prev_digest = r["digest"]
        expected_idx += 1
    checks.append(_check(
        "segment_chain", seg_ok,
        seg_detail or f"{len(seg_rows)} 个分段序号连续、段链完整"))

    # 3) 事件哈希链（含跨段锚定）+ 重放投影
    rebuilt = Projection()
    ev_ok, ev_detail, prev_hash = True, "", GENESIS_HASH
    last_seg_for_seq = 0
    for r in ev_rows:
        payload = json.loads(r["payload"])
        if r["prev_hash"] != prev_hash:
            ev_ok, ev_detail = False, f"事件 seq={r['seq']} prev_hash 断链"
            break
        want = P.hash_event(
            prev_hash, r["event_type"], payload,
            r["command_id"], r["fingerprint"],
        )
        if want != r["event_hash"]:
            ev_ok, ev_detail = False, f"事件 seq={r['seq']} event_hash 不符"
            break
        # 跨段：该事件所属段必须存在，且段首事件 prev_hash 等于上段末事件哈希
        if r["seg_index"] != last_seg_for_seq:
            if r["seg_index"] not in seg_by_idx:
                ev_ok, ev_detail = False, f"事件 seq={r['seq']} 所属段 #{r['seg_index']} 不存在"
                break
            last_seg_for_seq = r["seg_index"]
        prev_hash = r["event_hash"]
        apply_event(rebuilt, {"seq": r["seq"], "type": r["event_type"], **payload})
    chain_tip_ok = prev_hash == st["last_hash"]
    checks.append(_check(
        "event_hash_chain", ev_ok and chain_tip_ok,
        (ev_detail or f"{len(ev_rows)} 条事件哈希链连续，链尖与确认边界一致")
        if ev_ok and chain_tip_ok else (ev_detail or "链尖哈希与任务状态不符"),
    ))

    # 4) 内存投影（仅在线检查）
    if live_digest is not None:
        same = rebuilt.digest() == live_digest()
        checks.append(_check(
            "live_projection", same,
            "线上确认投影与库内事件重放逐字段一致"
            if same else "线上确认投影与库内事件重放不一致"))

    # 5) 守恒
    negatives = {nid: b for nid, b in rebuilt.balance.items() if b < 0}
    total_avail = sum(rebuilt.balance.values())
    conserved = total_avail + rebuilt.total_consumed
    cons_ok = not negatives and (not ev_rows or conserved == rebuilt.global_capacity)
    checks.append(_check(
        "projection_conservation", cons_ok,
        f"非负余额；Σ可用({total_avail})+总消费({rebuilt.total_consumed})"
        f"=容量({rebuilt.global_capacity})"
        if cons_ok
        else f"守恒破坏 negatives={negatives} {conserved}!={rebuilt.global_capacity}",
    ))

    # 6) 命令回执完备（用户命令）
    cmd_rows = conn.execute("SELECT command_id, seg_index FROM seg_commands").fetchall()
    user_cids = {r["command_id"] for r in ev_rows
                 if not r["command_id"].startswith("sys:")}
    reg_cids = {r["command_id"] for r in cmd_rows}
    cmd_ok = user_cids == reg_cids
    checks.append(_check(
        "command_results", cmd_ok,
        f"{len(reg_cids)} 条用户命令回执与事件批次一一对应"
        if cmd_ok
        else f"回执不一致 missing={sorted(user_cids - reg_cids)} "
             f"orphan={sorted(reg_cids - user_cids)}",
    ))

    # 7) 复制状态 / 落后量
    lag = st["lag_events"]
    lag_ok = lag <= st["lag_limit"] and st["status"] != "LAGGING"
    lag_detail = f"落后 {lag} 事件（限制 {st['lag_limit']}），状态 {st['status']}"
    if not lag_ok:
        lag_detail += "；已超落后限制"
    if primary_status is not None:
        real_lag = max(0, primary_status["current_event_seq"] - st["confirmed_seq"])
        lag_detail += f"；主站实时落后 {real_lag}"
        lag_ok = lag_ok and real_lag <= st["lag_limit"]
    checks.append(_check("replication_lag", lag_ok, lag_detail))

    # 8) 隔离区摘要（存在隔离项不算 ok=false，但显式报告）
    q_rows = conn.execute(
        "SELECT reason, COUNT(*) AS c FROM quarantine GROUP BY reason"
    ).fetchall()
    quarantined = {r["reason"]: r["c"] for r in q_rows}
    checks.append(_check(
        "quarantine", st["status"] != "QUARANTINED",
        "无熔断；隔离区统计: " + (canonical(quarantined) if quarantined else "{}")
        + ("" if st["status"] != "QUARANTINED" else "；当前处于 QUARANTINED 熔断态"),
        by_reason=quarantined,
    ))

    # 9) 演练地址：issued 地址必须未过期、绑定摘要仍等于锁定段
    drill_rows = conn.execute("SELECT * FROM drills").fetchall()
    drill_ok = True
    drill_detail = f"{len(drill_rows)} 次演练"
    now = time.time()
    for d in drill_rows:
        if d["status"] == "issued" and d["expires_at"] and d["expires_at"] < now:
            drill_ok = False
            drill_detail = f"{d['id']} 已过 TTL 但状态仍为 issued"
            break
        if d["status"] in ("issued", "verified"):
            seg = seg_by_idx.get(d["locked_idx"])
            if seg is None or seg["digest"] != d["locked_digest"]:
                drill_ok = False
                drill_detail = f"{d['id']} 锁定摘要与段链不符"
                break
    checks.append(_check("drill_addresses", drill_ok, drill_detail,
                         drill_count=len(drill_rows)))

    ok = all(c["ok"] for c in checks)
    return {
        "ok": ok,
        "checked_at": now,
        "summary": {
            "status": st["status"],
            "term": st["term"],
            "confirmed_index": st["confirmed_idx"],
            "confirmed_seq": st["confirmed_seq"],
            "segment_count": len(seg_rows),
            "event_count": len(ev_rows),
            "command_count": len(reg_cids),
            "quarantined": quarantined,
            "drill_count": len(drill_rows),
            "paused": bool(st["paused"]),
            "fence": st["fence"],
        },
        "checks": checks,
    }


def canonical(obj: Any) -> str:
    from ..ledger import canonical as _c

    return _c(obj)


def main(argv: list[str] | None = None) -> int:
    # 命令行实现放在 audit_cli，允许两种调用：
    #   python -m quota.replica.audit
    #   python -m quota.replica.audit_cli
    from .audit_cli import main as _main

    return _main(argv)


if __name__ == "__main__":
    import sys

    sys.exit(main())
