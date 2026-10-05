"""灾备审计命令行：python -m quota.replica.audit --db ./dr.db [--json]"""

from __future__ import annotations

import argparse
import json
import sys

from .audit import audit_database


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="灾备复制任务离线审计")
    parser.add_argument("--db", required=True, help="灾备任务 SQLite 路径")
    parser.add_argument("--json", action="store_true", help="只输出 JSON")
    args = parser.parse_args(argv)

    report = audit_database(args.db)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if report["ok"] else 2

    s = report["summary"]
    print("=" * 68)
    print("灾备复制任务审计")
    print("=" * 68)
    print(f"  状态          : {s['status']}  (任期 {s['term']}, fence {s['fence']})")
    print(f"  确认边界      : 分段 #{s['confirmed_index']} / 事件 seq={s['confirmed_seq']}")
    print(f"  已确认分段/事件: {s['segment_count']} 段 / {s['event_count']} 事件")
    print(f"  命令回执      : {s['command_count']}")
    print(f"  隔离区        : {s['quarantined']}")
    print(f"  演练          : {s['drill_count']} 次；复制暂停={s['paused']}")
    print("-" * 68)
    for c in report["checks"]:
        mark = "PASS" if c["ok"] else "FAIL"
        print(f"  [{mark}] {c['name']}: {c['detail']}")
    print("=" * 68)
    print("总体:", "OK" if report["ok"] else "FAILED")
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    sys.exit(main())
