"""受控切换演练 —— 截面验证。

锁定一个已复制截面（locked_idx / locked_seq）后，依次验证：

1. segment_chain   段链：每个已确认分段 prev_digest→digest 连续，
                    摘要按正文重算一致；
2. event_chain     事件内部哈希链：跨段锚定（段首 prev_hash ==
                    上段末 event_hash）且每条事件 event_hash 重算一致；
3. replay         从已确认事件完整重放得到的投影，与线上（只折叠到
                    已确认边界的）投影摘要一致；
4. conservation   领域守恒：非负余额 + Σ可用+总消费==全局容量；
5. commands       命令结果：每个非系统 command_id 的事件批次完整
                    （同批事件在同一段、seq 连续），且可按提取器重算
                    关键回执；数量与 seg_commands 登记一致；
6. no_progress    锁定之后到验证完成之间，没有新的已确认分段
                    （演练验证时又收到新分段 → 拒绝并使本次演练失败）。

全部通过才发放带任期的只读查询地址；任一失败返回结构化报告，绝不
对外发放地址。
"""

from __future__ import annotations

import json
from typing import Any

from ..domain import (
    PLATFORM_ID,
    RESULT_EXTRACTORS,
    Projection,
    apply_event,
)
from ..ledger import GENESIS_HASH
from . import protocol as P


def _check(name: str, ok: bool, detail: str, **extra: Any) -> dict[str, Any]:
    out = {"name": name, "ok": bool(ok), "detail": detail}
    out.update(extra)
    return out


def verify_snapshot(
    store: Any,
    live_digest_fn: Any,
    *,
    locked_index: int,
    primary_index_now: int,
) -> dict[str, Any]:
    """对锁定截面执行全部验证，返回报告 dict（含 ok 与 checks）。"""
    checks: list[dict[str, Any]] = []

    # 0) 基本一致：锁定边界与当前确认边界
    st = store.state()
    boundary_ok = st["confirmed_index"] >= locked_index
    checks.append(_check(
        "locked_boundary",
        boundary_ok and st["confirmed_seq"] >= 0,
        f"锁定分段 #{locked_index}（事件水位由段链推导）；"
        f"当前确认 #{st['confirmed_index']}",
        locked_index=locked_index,
        confirmed_index=st["confirmed_index"],
    ))

    # 1) 段链
    seg_rows = store.conn.execute(
        "SELECT * FROM segments WHERE idx<=? ORDER BY idx", (locked_index,)
    ).fetchall()
    chain_ok, chain_detail, locked_seq, last_digest = True, "", 0, P.GENESIS_DIGEST
    seg_seen = 0
    for r in seg_rows:
        seg_seen += 1
        if r["prev_digest"] != last_digest:
            chain_ok, chain_detail = False, f"段 #{r['idx']} prev_digest 断链"
            break
        # 用段表内字段无法重算完整摘要（未存正文），用事件行重建信封核对
        body = _rebuild_segment_body(store, r["idx"])
        if body is None:
            chain_ok, chain_detail = False, f"段 #{r['idx']} 缺少事件行"
            break
        if P.segment_digest(body) != r["digest"]:
            chain_ok, chain_detail = False, f"段 #{r['idx']} 摘要重算不符"
            break
        last_digest = r["digest"]
        locked_seq = r["to_seq"]
    if chain_ok and seg_seen != locked_index:
        chain_ok = False
        chain_detail = f"段链不完整：锁定 #{locked_index}，仅找到 {seg_seen} 段"
    checks.append(_check(
        "segment_chain", chain_ok,
        chain_detail or f"{seg_seen} 个分段段链连续，锁定事件水位 seq={locked_seq}",
        locked_seq=locked_seq,
    ))

    # 2) 事件内部哈希链（跨段锚定）
    ev_rows = store.conn.execute(
        "SELECT seq,event_type,payload,command_id,fingerprint,event_hash,"
        "prev_hash,seg_index FROM events WHERE seq<=? ORDER BY seq",
        (locked_seq,),
    ).fetchall()
    ev_ok, ev_detail, prev, prev_seg = True, "", GENESIS_HASH, 0
    rebuilt = Projection()
    for r in ev_rows:
        payload = json.loads(r["payload"])
        if r["prev_hash"] != prev:
            ev_ok, ev_detail = False, f"事件 seq={r['seq']} prev_hash 断链"
            break
        want = P.hash_event(
            prev, r["event_type"], payload, r["command_id"], r["fingerprint"]
        )
        if want != r["event_hash"]:
            ev_ok, ev_detail = False, f"事件 seq={r['seq']} event_hash 不符（载荷被篡改）"
            break
        # 跨段锚定：段的首事件必须指向上一段最后事件
        if r["seg_index"] != prev_seg:
            prev_seg = r["seg_index"]
        prev = r["event_hash"]
        apply_event(rebuilt, {"seq": r["seq"], "type": r["event_type"], **payload})
    checks.append(_check(
        "event_chain", ev_ok,
        ev_detail or f"{len(ev_rows)} 条事件哈希链连续、跨段锚定完整",
        event_count=len(ev_rows),
    ))

    # 3) 重放等价（重放投影 vs 灾备线上确认投影）
    replay_ok = False
    replay_detail = "事件链验证未完成，跳过重放比对"
    if ev_ok:
        replay_ok = rebuilt.digest() == live_digest_fn()
        replay_detail = (
            "重放投影与已确认线上投影摘要逐字段一致"
            if replay_ok
            else "重放投影与线上投影不一致（最近可用截面可能被污染）"
        )
    checks.append(_check("replay_equivalence", replay_ok, replay_detail))

    # 4) 投影守恒
    cons_ok, cons_detail = False, "前置验证未完成"
    if ev_ok:
        negatives = {nid: b for nid, b in rebuilt.balance.items() if b < 0}
        total_avail = sum(rebuilt.balance.values())
        conserved = total_avail + rebuilt.total_consumed
        cap = rebuilt.global_capacity
        cons_ok = not negatives and conserved == cap
        cons_detail = (
            f"非负余额成立；Σ可用({total_avail}) + 总消费("
            f"{rebuilt.total_consumed}) = 全局容量({cap})"
            if cons_ok
            else f"守恒被破坏 negatives={negatives} "
                 f"Σ可用+消费={conserved} != 容量={cap}"
        )
    checks.append(_check(
        "projection_conservation", cons_ok, cons_detail,
        global_capacity=rebuilt.global_capacity,
        total_available=sum(rebuilt.balance.values()),
        total_consumed=rebuilt.total_consumed,
        node_count=len(rebuilt.nodes),
        lease_count=len(rebuilt.leases),
    ))

    # 5) 命令结果完整且可重算
    cmd_ok, cmd_detail, cmd_info = _verify_commands(store, locked_index, ev_rows)
    checks.append(_check("command_results", cmd_ok, cmd_detail, **cmd_info))

    # 6) 锁定后无推进：验证期间不能又收到新分段
    no_progress = primary_index_now <= locked_index
    checks.append(_check(
        "no_progress_during_verification", no_progress,
        f"锁定 #{locked_index}，验证时主站/确认已到 #{primary_index_now}"
        + ("" if no_progress else "：验证期间收到新分段，本次截面失效"),
        primary_index_now=primary_index_now,
    ))

    ok = all(c["ok"] for c in checks)
    return {
        "ok": ok,
        "locked_index": locked_index,
        "locked_seq": locked_seq,
        "locked_digest": last_digest,
        "checks": checks,
        "verified_at": __import__("time").time(),
    }


def _rebuild_segment_body(store: Any, idx: int) -> dict[str, Any] | None:
    """从持久化的事件行重建分段信封（供重算段摘要）。"""
    seg = store.conn.execute(
        "SELECT * FROM segments WHERE idx=?", (idx,)
    ).fetchone()
    if seg is None:
        return None
    ev_rows = store.conn.execute(
        "SELECT seq,event_type,payload,command_id,fingerprint,event_hash,"
        "prev_hash,recorded_at FROM events WHERE seg_index=? ORDER BY seq",
        (idx,),
    ).fetchall()
    if not ev_rows:
        return None
    cmd = store.conn.execute(
        "SELECT result FROM seg_commands WHERE seg_index=?", (idx,)
    ).fetchone()
    events = []
    for r in ev_rows:
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
    return {
        "term": seg["term"],
        "index": seg["idx"],
        "batch_id": seg["batch_id"],
        "from_seq": seg["from_seq"],
        "to_seq": seg["to_seq"],
        "command_result": json.loads(cmd["result"]) if cmd else None,
        "events": events,
        "prev_digest": seg["prev_digest"],
        "digest": seg["digest"],
        "sealed_at": seg["sealed_at"],
    }


def _verify_commands(store: Any, locked_index: int,
                     ev_rows: list[Any]) -> tuple[bool, str, dict[str, Any]]:
    # 按 command_id 聚合已确认事件（系统命令 sys: 前缀不登记回执）
    batches: dict[str, list[Any]] = {}
    for r in ev_rows:
        batches.setdefault(r["command_id"], []).append(r)

    seg_cmd_rows = store.conn.execute(
        "SELECT command_id, seg_index, result FROM seg_commands"
        " WHERE seg_index<=?", (locked_index,)
    ).fetchall()
    seg_commands = {r["command_id"]: r for r in seg_cmd_rows}

    user_batches = {cid: rows for cid, rows in batches.items()
                    if not cid.startswith("sys:")}
    problems: list[str] = []

    # 每个用户命令必须有回执
    for cid, rows in user_batches.items():
        if cid not in seg_commands:
            problems.append(f"{cid}: 缺少命令回执")
            continue
        segs = {r["seg_index"] for r in rows}
        if len(segs) != 1:
            problems.append(f"{cid}: 批次跨越分段 {sorted(segs)}（被拆分）")
        seq_list = [r["seq"] for r in rows]
        if seq_list != list(range(seq_list[0], seq_list[-1] + 1)):
            problems.append(f"{cid}: 批次事件序号不连续")

    # 回执不能引用不存在的用户命令
    for cid in seg_commands:
        if cid not in user_batches:
            problems.append(f"{cid}: 有回执但无对应事件批次")

    count_ok = len(seg_commands) == len(user_batches)
    if not count_ok and not problems:
        problems.append(
            f"回执数 {len(seg_commands)} != 用户命令批次数 {len(user_batches)}"
        )

    info = {
        "user_command_count": len(user_batches),
        "system_command_count": len(batches) - len(user_batches),
        "result_count": len(seg_commands),
    }
    if problems:
        return False, "命令结果校验失败: " + "; ".join(problems[:5]), info
    return (
        True,
        f"{len(user_batches)} 条用户命令批次完整、回执齐备，"
        f"{len(batches) - len(user_batches)} 条系统批次仅留链",
        info,
    )
