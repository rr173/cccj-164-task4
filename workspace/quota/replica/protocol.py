"""灾备复制 —— 分段协议与确定性摘要。

主站把一次命令产生的*原子批次*密封成一个分段（segment），通过
``GET /api/feed/segments`` 对外提供。分段信封构成一条独立于事件内部
哈希链的「段链」：

  segment = {
    "term":       主站任期（每次主站重启 / 显式换届时 +1），
    "index":      段序号，从 1 开始严格连续，
    "batch_id":   命令批次标识（通常即 command_id），
    "from_seq":   本段首事件序号,
    "to_seq":     本段末事件序号（= 本段应用后的事件水位），
    "command_result": 非系统命令的原始回执（系统命令为 null），
    "events":     完整事件记录（含 seq/type/payload/command_id/
                  fingerprint/event_hash/prev_hash/recorded_at），
    "prev_digest": 上一段的摘要（首段为 GENESIS_DIGEST），
    "digest":     本段摘要（见 segment_digest），
    "sealed_at":  密封时间戳，
  }

段链摘要覆盖**整段规范正文**（含每个事件的 event_hash 与本段 in-chain
首事件的 prev_hash），因此任何「同序号正文不同」「摘要断链」「事件
内部哈希链断裂」「批次边界被改写」都能在灾备侧被发现。

同一条*内部事件链*跨越所有分段：段 k 首事件的 prev_hash 必须等于
段 k-1 末事件的 event_hash —— 两条链互相锚定，缺一不可。
"""

from __future__ import annotations

import hashlib
from typing import Any

from ..ledger import GENESIS_HASH, canonical

# 段链创世摘要（与事件链创世摘要相互独立）
GENESIS_DIGEST = "0" * 64

# 灾备侧隔离原因码（中文说明见 REASON_TEXT，供页面/审计展示）
REASON_TEXT: dict[str, str] = {
    "duplicate": "重复分段（内容一致，正常去重，不隔离）",
    "gap": "分段缺失：index 不是已确认边界的下一段",
    "out_of_order": "错序：收到的 index 小于等于已确认边界",
    "same_index_diff_body": "同序号正文不同：index 已确认但正文摘要不一致",
    "digest_mismatch": "摘要自校验失败：声明 digest 与正文计算结果不符",
    "chain_broken": "摘要断链：prev_digest 未指向上一段摘要",
    "event_chain_broken": "事件哈希链断裂：段内/跨段 prev_hash 不连续",
    "event_payload_tampered": "事件载荷被篡改：event_hash 重算不一致",
    "batch_split": "批次被拆分：同 command_id 的事件跨越了分段边界",
    "seq_mismatch": "事件序号不连续：from_seq/to_seq/events[].seq 不一致",
    "stale_term": "旧任期迟到更新：term 早于已确认任期",
    "new_term": "主站已换届（term 前进）：未完成显式接管前拒绝应用",
    "malformed": "分段结构非法：缺少必需字段或字段类型错误",
    "lag_exceeded": "落后量超过允许限制（隔离/熔断，不推进确认边界）",
}

REQUIRED_FIELDS = (
    "term",
    "index",
    "batch_id",
    "from_seq",
    "to_seq",
    "events",
    "prev_digest",
    "digest",
    "sealed_at",
)

EVENT_FIELDS = (
    "seq",
    "event_type",
    "payload",
    "command_id",
    "fingerprint",
    "event_hash",
    "prev_hash",
    "recorded_at",
)


def event_digest_body(prev_hash: str, event_type: str, payload: Any,
                      command_id: str, fingerprint: str) -> str:
    """与主站 ledger.EventStore._hash_event 完全一致的规范化正文。"""
    return canonical(
        {
            "prev_hash": prev_hash,
            "event_type": event_type,
            "payload": payload,
            "command_id": command_id,
            "fingerprint": fingerprint,
        }
    )


def hash_event(prev_hash: str, event_type: str, payload: Any,
               command_id: str, fingerprint: str) -> str:
    return hashlib.sha256(
        event_digest_body(
            prev_hash, event_type, payload, command_id, fingerprint
        ).encode("utf-8")
    ).hexdigest()


def segment_body_for_digest(seg: dict[str, Any]) -> dict[str, Any]:
    """参与段摘要的规范正文（不含 digest 自身）。

    覆盖信封元数据与每事件的锚点（seq + event_hash + prev_hash +
    command_id），同时也覆盖完整载荷 —— 事件正文被改写即断链。
    """
    return {
        "term": seg["term"],
        "index": seg["index"],
        "batch_id": seg["batch_id"],
        "from_seq": seg["from_seq"],
        "to_seq": seg["to_seq"],
        "command_result": seg.get("command_result"),
        "events": [
            {
                "seq": e["seq"],
                "event_type": e["event_type"],
                "payload": e["payload"],
                "command_id": e["command_id"],
                "fingerprint": e["fingerprint"],
                "event_hash": e["event_hash"],
                "prev_hash": e["prev_hash"],
                "recorded_at": e["recorded_at"],
            }
            for e in seg["events"]
        ],
        "prev_digest": seg["prev_digest"],
        "sealed_at": seg["sealed_at"],
    }


def segment_digest(seg: dict[str, Any]) -> str:
    """按整段规范正文计算 SHA-256 摘要。"""
    return hashlib.sha256(
        canonical(segment_body_for_digest(seg)).encode("utf-8")
    ).hexdigest()


def well_formed(seg: Any) -> bool:
    """信封结构的廉价检查（深度校验在 replicator 内完成）。"""
    if not isinstance(seg, dict):
        return False
    for f in REQUIRED_FIELDS:
        if f not in seg:
            return False
    if not isinstance(seg["events"], list) or not seg["events"]:
        return False
    for e in seg["events"]:
        if not isinstance(e, dict) or any(f not in e for f in EVENT_FIELDS):
            return False
    for f in ("term", "index", "from_seq", "to_seq"):
        v = seg[f]
        if isinstance(v, bool) or not isinstance(v, int) or v < 0:
            return False
    return seg["index"] >= 1 and seg["to_seq"] >= seg["from_seq"] >= 1


def verify_event_hashes(seg: dict[str, Any], expected_prev_hash: str) -> tuple[bool, int | None]:
    """重算段内每个事件的 event_hash，并检查段内链连续。

    ``expected_prev_hash`` 是本段*首事件*应指向的 prev_hash
    （上一段末事件的 event_hash；首段为事件链创世哈希）。
    返回 (是否通过, 首个失败事件的 seq)。
    """
    prev = expected_prev_hash
    for e in seg["events"]:
        if e["prev_hash"] != prev:
            return False, e["seq"]
        want = hash_event(
            prev,
            e["event_type"],
            e["payload"] if not isinstance(e["payload"], str) else _loads(e["payload"]),
            e["command_id"],
            e["fingerprint"],
        )
        if want != e["event_hash"]:
            return False, e["seq"]
        prev = e["event_hash"]
    return True, None


def _loads(raw: str) -> Any:
    import json

    return json.loads(raw)


# 事件链创世哈希再导出，方便 server / store 使用
__all__ = (
    "GENESIS_DIGEST",
    "GENESIS_HASH",
    "REASON_TEXT",
    "canonical",
    "segment_digest",
    "segment_body_for_digest",
    "hash_event",
    "well_formed",
    "verify_event_hashes",
)
