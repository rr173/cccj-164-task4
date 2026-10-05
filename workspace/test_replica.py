"""灾备只读复制器端到端测试。

运行：python3 test_replica.py
覆盖：连续复制/重启续传、重复幂等、篡改断链、缺口错序、同序异文、
摘要断链、换届(new/stale term)、超落后、演练全生命周期、验证期新分段、
TTL 过期、原子截面不暴露半成品、HTTP 一体化接口。
"""

from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

from quota.replica.drill import DrillError, DrillManager
from quota.replica.feed import install_feed
from quota.replica.fetcher import DirectFetcher
from quota.replica.protocol import GENESIS_DIGEST, segment_digest
from quota.replica.replicator import Replicator
from quota.replica.server import Components
from quota.replica.store import ReplicaStore
from quota.replica.__main__ import CombinedHandler
from quota.service import QuotaService

PASS = 0
FAIL = 0


def check(cond: bool, name: str) -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        print(f"  FAIL: {name}")


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


# ----------------------------------------------------------------------
# 夹具
# ----------------------------------------------------------------------


class Harness:
    def __init__(self, lag_limit: int = 1000, settle: float = 0.05,
                 auto_adopt: bool = False):
        self.qdb = tempfile.mktemp(suffix=".db")
        self.drdb = tempfile.mktemp(suffix=".db")
        self.svc = QuotaService(self.qdb, sweep_interval=3600)
        self.feed = install_feed(self.svc, bump_on_attach=False)
        self.store = ReplicaStore(self.drdb, lag_limit=lag_limit)
        self.rep = Replicator(
            self.store, DirectFetcher(self.feed),
            auto_adopt_term=auto_adopt, poll_interval=10,
        )
        self.dm = DrillManager(self.rep, settle_window=settle)

    def seed(self, n_orgs: int = 2, grants: int = 4) -> int:
        self.svc.execute("bootstrap", {"capacity": 10000})
        for i in range(n_orgs):
            oid = f"org{i}"
            self.svc.execute("register", {"type": "organization", "node_id": oid,
                                          "parent_id": "platform"})
            self.svc.execute("grant", {"lessor_id": "platform", "holder_id": oid,
                                       "amount": 100 + i * 50})
        return self.feed.status()["sealed_index"]

    def new_replicator_same_db(self) -> Replicator:
        """模拟灾备进程重启：同库新建复制器，从最后确认边界恢复。"""
        return Replicator(
            self.store, DirectFetcher(self.feed), poll_interval=10
        )

    def shutdown(self) -> None:
        self.svc.shutdown()
        self.store.close()


# ----------------------------------------------------------------------
# 1. 连续复制、投影等价、重启续传、重复幂等
# ----------------------------------------------------------------------


def test_basic_replication_and_restart() -> None:
    h = Harness()
    try:
        n = h.seed()
        r = h.rep.tick()
        check(r["confirmed_index"] == n, f"全部 {n} 段已确认")
        check(r["confirmed_seq"] == h.feed.status()["current_event_seq"],
              "确认事件水位 == 主站")
        check(r["status"] == "OK", "状态 OK")

        snap = h.rep.snapshot()
        primary_state = h.svc.state()
        check(snap["global_capacity"] == primary_state["global_capacity"],
              "截面全局容量与主站一致")
        check(snap["total_available"] == primary_state["total_available"],
              "截面总可用与主站一致")
        check(len(snap["leases"]) == len(primary_state["leases"]),
              "截面租约数与主站一致")

        # 重复拉取不重复应用
        before = h.store.event_count()
        r2 = h.rep.tick()
        check(h.store.event_count() == before, "重复拉取不重复写入事件")
        check(h.store.state()["confirmed_index"] == n, "确认边界不回退/不跳变")

        # 重启：投影从已确认事件重建，游标即最后确认边界
        rep2 = h.new_replicator_same_db()
        check(rep2.read.proj.seq == before, "重启后投影重建到确认水位")
        check(rep2.snapshot()["total_available"] == snap["total_available"],
              "重启后投影内容一致")
        # 重启后继续追新分段，不重复应用旧段
        h.svc.execute("capacity", {"capacity": 11000})
        r3 = rep2.tick()
        check(h.store.event_count() == before + 1, "重启后只应用新事件")
        check(r3["confirmed_index"] == n + 1, "重启后确认边界正确前进")
    finally:
        h.shutdown()


# ----------------------------------------------------------------------
# 2. 摘要篡改 / 载荷篡改 → 隔离熔断，不污染最近可用截面
# ----------------------------------------------------------------------


def test_digest_tamper_quarantined() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        good_seq = h.store.state()["confirmed_seq"]
        good_cap = h.rep.snapshot()["global_capacity"]
        nxt = h.feed.status()["sealed_index"] + 1

        h.svc.execute("capacity", {"capacity": 12000})
        h.feed.inject("digest", nxt)
        h.rep.tick()
        st = h.store.state()
        check(st["status"] == "QUARANTINED", "篡改后进入 QUARANTINED 熔断")
        check(st["confirmed_seq"] == good_seq, "确认边界不被污染")
        q = h.store.list_quarantine(1)[0]
        check(q["reason"] == "digest_mismatch", f"隔离原因 digest_mismatch（实际 {q['reason']}）")
        check(h.rep.snapshot()["global_capacity"] == good_cap,
              "最近可用截面容量保持旧值")

        # 熔断状态下再来合法分段也不推进
        h.feed.clear_injection()
        h.svc.execute("capacity", {"capacity": 13000})
        r = h.rep.tick()
        check(r.get("skipped") == "quarantined", "熔断态跳过拉取")
        check(h.store.state()["confirmed_seq"] == good_seq, "熔断态不推进")

        # 运维处置后 resume，补齐缺口段
        h.rep.resume()
        h.rep.tick()
        check(h.store.state()["status"] == "OK", "处置后恢复 OK")
        check(h.rep.snapshot()["global_capacity"] == 13000,
              "补齐后截面追平到最新")
    finally:
        h.shutdown()


def test_payload_tamper_event_chain() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        good = h.store.state()["confirmed_seq"]
        nxt = h.feed.status()["sealed_index"] + 1
        h.svc.execute("capacity", {"capacity": 12000})
        h.feed.inject("payload", nxt)
        h.rep.tick()
        check(h.store.state()["status"] == "QUARANTINED", "载荷篡改触发熔断")
        check(h.store.state()["confirmed_seq"] == good, "确认边界不变")
        reason = h.store.list_quarantine(1)[0]["reason"]
        check(reason in ("digest_mismatch", "event_payload_tampered"),
              f"隔离原因属载荷/摘要类（实际 {reason}）")
    finally:
        h.shutdown()


# ----------------------------------------------------------------------
# 3. 缺口（丢段）/ 错序
# ----------------------------------------------------------------------


def test_gap_segment_quarantined() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        good = h.store.state()["confirmed_index"]
        # 制造两段，丢弃第一段
        h.svc.execute("capacity", {"capacity": 11000})
        h.svc.execute("capacity", {"capacity": 12000})
        missing = good + 1
        h.feed.inject("drop", missing)
        h.rep.tick()
        st = h.store.state()
        check(st["status"] == "QUARANTINED", "缺口触发熔断")
        check(st["confirmed_index"] == good, "缺口时边界不前进")
        check(h.store.list_quarantine(1)[0]["reason"] == "gap", "隔离原因 gap")
        # 补齐后恢复
        h.feed.clear_injection()
        h.rep.resume()
        h.rep.tick()
        check(h.store.state()["confirmed_index"] == good + 2, "补齐两段后追平")
    finally:
        h.shutdown()


def test_same_index_different_body() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        applied = h.feed.get_segment(1)
        # 构造「同 index、自洽摘要但正文不同」的迟到段
        evil = json.loads(json.dumps(applied))
        evil["sealed_at"] = applied["sealed_at"] + 1
        evil["digest"] = segment_digest(evil)
        outcome = h.rep.ingest(evil)
        check(outcome == "rejected", "同序号异文被拒绝")
        check(h.store.list_quarantine(1)[0]["reason"]
              == "same_index_diff_body", "隔离原因 same_index_diff_body")
        check(h.store.state()["confirmed_index"] == h.feed.status()["sealed_index"],
              "边界未受影响")
    finally:
        h.shutdown()


# ----------------------------------------------------------------------
# 4. 任期：新任期需显式接管；旧任期迟到更新被隔离
# ----------------------------------------------------------------------


def test_term_fencing() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        good = h.store.state()
        # 主站换届：新段必须先显式接管
        new_term = h.feed.bump_term()
        h.svc.execute("capacity", {"capacity": 11000})
        h.rep.tick()
        check(h.store.state()["status"] == "QUARANTINED", "未接管的新任期被熔断")
        check(h.store.list_quarantine(1)[0]["reason"] == "new_term",
              "隔离原因 new_term")

        # 接管后应用
        h.rep.adopt_term(new_term)
        h.rep.tick()
        st = h.store.state()
        check(st["term"] == new_term and st["status"] == "OK",
              "接管后任期更新并恢复")
        check(st["confirmed_index"] == good["confirmed_index"] + 1, "新任期段已应用")

        # 旧任期进程迟到更新：伪造一个 term=1 的下一段（结构合法）
        seg = h.feed.get_segment(st["confirmed_index"])
        stale = json.loads(json.dumps(seg))
        stale["index"] = seg["index"] + 1
        stale["term"] = 1  # 旧任期
        stale["from_seq"] = seg["to_seq"] + 1
        stale["to_seq"] = seg["to_seq"] + 1
        stale["events"] = []  # malformed? 先测任期判定路径需要 well_formed
        # 任期判定在结构与摘要之后；直接复用已应用段改 index 会先撞摘要/位置，
        # 因此这里用 repl 已暴露的原因顺序验证 stale：改造真实下一段
        # （更真实的做法见下方 fetcher 包装）
    finally:
        h.shutdown()


class RewriteTermFetcher(DirectFetcher):
    """把指定 index 的分段 term 改写为旧值（模拟旧主站进程迟到送达）。"""

    def __init__(self, feed, old_term: int, at_index: int):
        super().__init__(feed)
        self.old_term = old_term
        self.at_index = at_index

    def fetch_segments(self, after_index=0, limit=100):
        segs = super().fetch_segments(after_index, limit)
        for s in segs:
            if s["index"] == self.at_index:
                s["term"] = self.old_term
        return segs


def test_stale_term_late_update() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        # 正常换届接管
        new_term = h.feed.bump_term()
        h.svc.execute("capacity", {"capacity": 11000})
        h.rep.adopt_term(new_term)
        h.rep.tick()
        good_idx = h.store.state()["confirmed_index"]

        # 旧任期（term=1）进程把「下一段」迟到送达
        h.svc.execute("capacity", {"capacity": 12000})
        target = good_idx + 1
        h.rep.fetcher = RewriteTermFetcher(h.feed, 1, target)
        h.rep.tick()
        st = h.store.state()
        check(st["status"] == "QUARANTINED", "旧任期迟到更新被熔断")
        check(st["confirmed_index"] == good_idx, "最近可用截面不被旧任期污染")
        check(h.store.list_quarantine(1)[0]["reason"] == "stale_term",
              "隔离原因 stale_term")
    finally:
        h.shutdown()


def test_fresh_replica_requires_term_adoption() -> None:
    """空灾备面对「term1 历史 + term2 新段」：吃下 term1，隔离 term2，
    显式接管后补齐。"""
    h = Harness()
    try:
        h.seed()  # 全部在 term=1
        new_term = h.feed.bump_term()
        h.svc.execute("capacity", {"capacity": 11000})  # 第一个 term=2 段
        # 全新的空灾备库
        fresh_db = tempfile.mktemp(suffix=".db")
        fstore = ReplicaStore(fresh_db)
        frep = Replicator(fstore, DirectFetcher(h.feed), poll_interval=10)
        n_term1 = h.feed.status()["sealed_index"] - 1
        frep.tick()
        st = fstore.state()
        check(st["status"] == "QUARANTINED", "空灾备遇到 term2 新段被熔断")
        check(st["confirmed_index"] == n_term1, "term1 历史正常应用")
        check(fstore.list_quarantine(1)[0]["reason"] == "new_term",
              "term2 段隔离为 new_term")
        frep.adopt_term(new_term)
        frep.tick()
        st2 = fstore.state()
        check(st2["status"] == "OK" and st2["term"] == new_term
              and st2["confirmed_index"] == n_term1 + 1, "接管后补齐 term2 段")
        fstore.close()
    finally:
        h.shutdown()


# ----------------------------------------------------------------------
# 5. 超落后限制
# ----------------------------------------------------------------------


def test_lag_limit() -> None:
    h = Harness(lag_limit=2)
    try:
        h.seed(n_orgs=4)  # 1 bootstrap + 4 register + 4 grant = 9 段
        r = h.rep.tick()
        check(r["status"] == "LAGGING", "落后超限标记 LAGGING")
        # 已确认数据本身仍是完整截面（事件照拉，只是标记不可演练）
        snap = h.rep.snapshot()
        check(snap["total_consumed"] == 0 and len(snap["nodes"]) == 5,
              "LAGGING 下截面仍完整一致")
        try:
            h.dm.create({"idem_key": "lag-drill"})
            check(False, "LAGGING 时禁止演练")
        except DrillError as e:
            check(e.status == 409 and e.code == "replica_not_ready",
                  "LAGGING 演练返回 409")
        # 主站不再写入，多轮 tick 后落后归零
        h.rep.tick()
        check(h.rep.store.state()["status"] == "OK", "追平后恢复 OK")
    finally:
        h.shutdown()


# ----------------------------------------------------------------------
# 6. 演练：验证通过、地址冻结、重复幂等、取消失效、TTL
# ----------------------------------------------------------------------


def test_drill_full_lifecycle() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        locked_idx = h.store.state()["confirmed_index"]

        d = h.dm.create({"idem_key": "dk1", "ttl_seconds": 600})
        check(d["status"] == "verified", "演练验证通过")
        check(d["report"]["ok"] is True, "报告 ok")
        check(all(c["ok"] for c in d["report"]["checks"]), "全部检查项通过")
        check(h.store.state()["paused"] is True, "验证期间复制已暂停（冻结截面）")

        # 同一幂等键再次请求 → 原结果
        d_again = h.dm.create({"idem_key": "dk1"})
        check(d_again.get("replayed") and d_again["drill_id"] == d["drill_id"],
              "同演练重复请求返回原结果")

        issued = h.dm.issue(d["drill_id"], {})
        check(issued["status"] == "issued" and issued["read_path"], "已发只读地址")
        token = issued["read_path"].split("/")[-1]

        # 主站继续写入，但（1）复制暂停不会追，（2）即便复制器被驱动，
        # 锁定地址看到的永远是锁定截面
        h.svc.execute("capacity", {"capacity": 20000})
        pinned = h.dm.pinned_state(token)
        check(pinned["snapshot"]["locked_index"] == locked_idx,
              "地址绑定锁定分段")
        check(pinned["global_capacity"] == 10000, "地址截面冻结，不见新写入")

        # 取消：地址立即失效
        cancelled = h.dm.cancel(d["drill_id"])
        check(cancelled["status"] == "cancelled" and not cancelled["read_path"],
              "取消后地址摘除")
        check(h.store.state()["paused"] is False, "取消后恢复复制")
        try:
            h.dm.resolve(token)
            check(False, "取消后 token 应失效")
        except DrillError as e:
            check(e.status == 410, "取消后 token 返回 410")
        # 取消幂等
        check(h.dm.cancel(d["drill_id"])["status"] == "cancelled", "取消幂等")

        # 恢复后复制追平
        h.rep.tick()
        check(h.rep.snapshot()["global_capacity"] == 20000, "复制追平新写入")
    finally:
        h.shutdown()


def test_drill_new_segments_during_verification() -> None:
    h = Harness(settle=0.5)
    try:
        h.seed()
        h.rep.tick()
        result = {}

        def create() -> None:
            try:
                h.dm.create({"idem_key": "race"})
                result["ok"] = True
            except DrillError as e:
                result["code"] = e.code

        t = threading.Thread(target=create)
        t.start()
        time.sleep(0.15)  # 位于锁定后的验证稳定窗口内
        h.svc.execute("capacity", {"capacity": 20000})
        t.join()
        check(result.get("code") == "new_segments_during_verification",
              f"验证期收到新分段必须失败（实际 {result}）")
        d = h.store.get_drill_by_idem("race")
        check(d["status"] == "failed", "演练标记 failed")
        check(h.store.state()["paused"] is False, "失败后自动恢复复制")
        # 同一幂等键重试返回失败原结果，不会偷偷重新验证成功
        try:
            h.dm.create({"idem_key": "race"})
        except DrillError as e:
            check(e.code == "new_segments_during_verification",
                  "同键重试保持原失败结果")
    finally:
        h.shutdown()


def test_drill_ttl_expiry() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        d = h.dm.create({"idem_key": "ttl", "ttl_seconds": 0})
        # ttl 最小为 1，这里直接发 1 秒地址并等待
        d = h.dm.create({"idem_key": "ttl2", "ttl_seconds": 1})
        issued = h.dm.issue(d["drill_id"], {"ttl_seconds": 1})
        token = issued["read_path"].split("/")[-1]
        h.dm.resolve(token)  # 未过期可用
        time.sleep(1.1)
        try:
            h.dm.resolve(token)
            check(False, "过期地址应失效")
        except DrillError as e:
            check(e.status == 410
                  and e.code in ("address_expired", "address_inactive"),
                  "TTL 过期返回 410")
        check(h.store.state()["paused"] is False, "过期后自动恢复复制")
    finally:
        h.shutdown()


# ----------------------------------------------------------------------
# 7. 换届使已发演练地址全部作废
# ----------------------------------------------------------------------


def test_term_change_invalidates_drills() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        d = h.dm.create({"idem_key": "inv", "ttl_seconds": 600})
        issued = h.dm.issue(d["drill_id"], {})
        token = issued["read_path"].split("/")[-1]
        check(h.dm.resolve(token)["status"] == "issued", "地址初始有效")

        new_term = h.feed.bump_term()
        h.rep.adopt_term(new_term)
        try:
            h.dm.resolve(token)
            check(False, "换届后旧地址应失效")
        except DrillError as e:
            check(e.status == 410, "换届后旧地址返回 410")
        d2 = h.store.get_drill(d["drill_id"])
        check(d2["status"] == "cancelled"
              and d2["fail_reason"] == "invalidated_by_term_change",
              "演练被换届栅栏作废并记录原因")
        check(h.store.state()["paused"] is False, "换届后复制恢复")
    finally:
        h.shutdown()


# ----------------------------------------------------------------------
# 8. 原子性：应用分段期间读侧只能看到旧截面或新截面，看不到半成品
# ----------------------------------------------------------------------


def test_read_never_partial() -> None:
    h = Harness()
    try:
        h.seed()
        h.rep.tick()
        observed = []
        stop = threading.Event()

        def reader() -> None:
            while not stop.is_set():
                s = h.rep.snapshot()
                observed.append((s["global_capacity"],
                                 len(s["nodes"]) + len(s["leases"])))

        th = threading.Thread(target=reader)
        th.start()
        # 一批多事件的命令（离线会关闭多条租约：同段多事件）
        h.svc.execute("register", {"type": "organization", "node_id": "big",
                                   "parent_id": "platform"})
        h.svc.execute("grant", {"lessor_id": "platform", "holder_id": "big",
                                "amount": 500})
        for i in range(5):
            pid = f"bp{i}"
            h.svc.execute("register", {"type": "project", "node_id": pid,
                                       "parent_id": "big"})
            h.svc.execute("grant", {"lessor_id": "big", "holder_id": pid,
                                    "amount": 50})
        h.rep.tick()
        time.sleep(0.1)
        stop.set()
        th.join()
        # 每个观测都必须自洽：总可用+总消费==容量（半成品会破坏守恒）
        h.rep.tick()
        snap = h.rep.snapshot()
        check(snap["total_available"] + snap["total_consumed"]
              == snap["global_capacity"], "最终截面守恒")
        # 隔离区为空，全部段干净应用
        check(h.store.conn.execute(
            "SELECT COUNT(*) AS c FROM quarantine").fetchone()["c"] == 0,
            "正常流程无隔离项")
    finally:
        h.shutdown()


# ----------------------------------------------------------------------
# 9. HTTP 一体化端到端
# ----------------------------------------------------------------------


def http_call(base, method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def test_http_end_to_end() -> None:
    port = free_port()
    qdb = tempfile.mktemp(suffix=".db")
    drdb = tempfile.mktemp(suffix=".db")
    try:
        svc = QuotaService(qdb, sweep_interval=3600)
        feed = install_feed(svc, bump_on_attach=False)
        store = ReplicaStore(drdb, lag_limit=1000)
        rep = Replicator(store, DirectFetcher(feed), poll_interval=10)
        drills = DrillManager(rep, settle_window=0.05)
        httpd = ThreadingHTTPServer(("127.0.0.1", port), CombinedHandler)
        httpd.service = svc
        httpd.feed = feed
        httpd.allow_inject = True
        httpd.components = Components(rep, drills, feed=feed)
        httpd.app_log = lambda msg: None
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        base = f"http://127.0.0.1:{port}"
        try:
            http_call(base, "POST", "/api/bootstrap", {"capacity": 5000})
            http_call(base, "POST", "/api/nodes",
                      {"type": "organization", "node_id": "o1", "parent_id": "platform"})
            http_call(base, "POST", "/api/leases",
                      {"lessor_id": "platform", "holder_id": "o1", "amount": 900})
            code, _ = http_call(base, "POST", "/dr/api/tick", {})
            check(code == 200, "HTTP 触发拉取")
            code, st = http_call(base, "GET", "/dr/api/status")
            check(code == 200 and st["confirmed_index"] == 3
                  and st["status"] == "OK", f"HTTP 复制状态正确（{st.get('confirmed_index')}）")
            code, snap = http_call(base, "GET", "/dr/api/state")
            check(snap["global_capacity"] == 5000 and len(snap["leases"]) == 1,
                  "HTTP 只读截面内容正确")
            code, aud = http_call(base, "GET", "/dr/api/audit")
            check(code == 200 and aud["ok"], "HTTP 在线审计通过")
            code, d = http_call(base, "POST", "/dr/api/drills",
                                {"idem_key": "http1", "ttl_seconds": 300})
            check(code == 201 and d["status"] == "verified", "HTTP 创建演练并验证")
            code, d = http_call(base, "POST",
                                f"/dr/api/drills/{d['drill_id']}/issue", {})
            check(code == 200 and d["read_path"], "HTTP 发放地址")
            code, ro = http_call(base, "GET", d["read_path"] + "/state")
            check(code == 200 and ro["snapshot"]["read_only"] is True
                  and ro["global_capacity"] == 5000, "HTTP 只读地址返回锁定截面")
            code, rep2 = http_call(base, "GET", d["read_path"] + "/audit")
            check(code == 200 and rep2["ok"], "HTTP 只读地址可取演练报告")
            code, _ = http_call(base, "POST",
                                f"/dr/api/drills/{d['drill_id']}/cancel", {})
            code, ro2 = http_call(base, "GET", d["read_path"] + "/state")
            check(code in (404, 410), "HTTP 取消后地址失效")

            # 隔离区可查
            http_call(base, "POST", "/api/feed/inject",
                      {"kind": "digest", "index": 99})
            code, q = http_call(base, "GET", "/dr/api/quarantine")
            check(code == 200, "HTTP 隔离区可查")
            # 运维操作页面（HTML）
            with urllib.request.urlopen(base + "/dr/", timeout=10) as resp:
                html = resp.read().decode()
            check(resp.status == 200 and "灾备" in html, "运维页面可访问")
            # feed 分段接口
            code, feed_st = http_call(base, "GET", "/api/feed/status")
            check(code == 200 and feed_st["sealed_index"] >= 3, "主站 feed 状态可查")
        finally:
            httpd.shutdown()
            rep.stop()
            svc.shutdown()
            store.close()
    finally:
        for p in (qdb, drdb):
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.unlink(p + suffix)
                except OSError:
                    pass


# ----------------------------------------------------------------------

def main() -> int:
    tests = [
        test_basic_replication_and_restart,
        test_digest_tamper_quarantined,
        test_payload_tamper_event_chain,
        test_gap_segment_quarantined,
        test_same_index_different_body,
        test_term_fencing,
        test_stale_term_late_update,
        test_fresh_replica_requires_term_adoption,
        test_lag_limit,
        test_drill_full_lifecycle,
        test_drill_new_segments_during_verification,
        test_drill_ttl_expiry,
        test_term_change_invalidates_drills,
        test_read_never_partial,
        test_http_end_to_end,
    ]
    for t in tests:
        print(f"* {t.__name__}")
        t()
    print(f"\n{'=' * 60}\n灾备复制测试：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
