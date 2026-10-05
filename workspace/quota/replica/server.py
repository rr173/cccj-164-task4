"""灾备节点 HTTP 接口（Python 标准库，零外部依赖）。

复制 / 运维
  GET  /dr/api/status              复制进度、任期、落后量、状态
  POST /dr/api/tick                立即执行一轮拉取（测试/排障）
  GET  /dr/api/segments            已确认分段列表
  GET  /dr/api/quarantine          隔离区（原因/正文/时间）
  GET  /dr/api/audit               在线一致性审计
  GET  /dr/api/audit-log           任务审计日志
  POST /dr/api/control/pause       暂停复制
  POST /dr/api/control/resume      恢复复制
  POST /dr/api/control/adopt-term  {term} 显式接管主站新任期
  GET  /dr/api/state               **最近已确认截面**（只读投影）

受控切换演练
  POST /dr/api/drills              {idem_key,ttl_seconds?} 锁定+验证
  GET  /dr/api/drills              演练列表
  GET  /dr/api/drills/{id}         演练报告
  POST /dr/api/drills/{id}/issue   发放带任期只读地址（幂等）
  POST /dr/api/drills/{id}/cancel  取消（地址立即失效）

带任期只读地址（内容固定为锁定截面）
  GET  /ro/{token}/state           锁定截面：余额/租约/草案
  GET  /ro/{token}/audit           锁定截面验证报告（演练报告）

  GET  /dr/                        灾备运维操作页面

一体化模式（演示/测试）下还会挂载主站 feed：
  GET  /api/feed/status  GET /api/feed/segments
  POST /api/feed/inject  POST /api/feed/clear-injection  POST /api/feed/bump-term
"""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .audit import audit_live
from .drill import DrillError, DrillManager
from .replicator import Replicator
from .store import ReplicaStore

DR_STATIC_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "replica_static",
)


class DRHandler(BaseHTTPRequestHandler):
    server_version = "QuotaDR/1.0"

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        self.server.app_log(f"[dr] {self.command} {self.path}")

    # ------------------------------------------------------------------
    def _json(self, code: int, obj: dict | list) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise DrillError(400, "bad_json", f"请求体不是合法 JSON: {exc}")
        if not isinstance(data, dict):
            raise DrillError(400, "bad_body", "请求体必须是 JSON 对象")
        return data

    def components(self) -> Any:
        return self.server.components  # type: ignore[attr-defined]

    def _drill_error(self, exc: DrillError) -> None:
        self._json(exc.status, {
            "error": exc.code, "message": str(exc), "details": exc.details,
        })

    # ------------------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802, C901
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)
        c = self.components()
        try:
            # 主站 feed（一体化模式）
            if c.feed is not None and path == "/api/feed/status":
                return self._json(200, c.feed.status())
            if c.feed is not None and path == "/api/feed/segments":
                after = int(qs.get("after", ["0"])[0])
                limit = int(qs.get("limit", ["100"])[0])
                return self._json(200, {
                    "segments": c.feed.fetch(after_index=after, limit=limit),
                    "after": after,
                })

            # 带任期只读地址
            if path.startswith("/ro/"):
                rest = path[len("/ro/"):]
                parts = [p for p in rest.split("/") if p]
                if len(parts) == 2 and parts[1] in ("state", "audit"):
                    token = parts[0]
                    try:
                        if parts[1] == "state":
                            return self._json(200, c.drills.pinned_state(token))
                        view = c.drills.resolve(token)
                        return self._json(200, view["report"] or {"ok": None})
                    except DrillError as exc:
                        return self._drill_error(exc)
                return self._json(404, {"error": "not_found"})

            if path == "/dr/api/status":
                return self._json(200, self._status())
            if path == "/dr/api/state":
                st = c.rep.snapshot()
                st["replication"] = self._status()
                return self._json(200, st)
            if path == "/dr/api/segments":
                limit = int(qs.get("limit", ["100"])[0])
                rows = c.rep.store.conn.execute(
                    "SELECT idx,term,batch_id,from_seq,to_seq,prev_digest,digest,"
                    "sealed_at FROM segments ORDER BY idx DESC LIMIT ?", (limit,)
                ).fetchall()
                return self._json(200, {"segments": [dict(r) for r in rows]})
            if path == "/dr/api/quarantine":
                return self._json(200, {"items": c.rep.store.list_quarantine(200)})
            if path == "/dr/api/audit":
                return self._json(200, audit_live(c.rep))
            if path == "/dr/api/audit-log":
                return self._json(200, {"items": c.rep.store.list_audit_log(200)})
            if path == "/dr/api/drills":
                return self._json(200, {"drills": c.drills.list()})
            if path.startswith("/dr/api/drills/"):
                drill_id = path[len("/dr/api/drills/"):].strip("/")
                return self._json(200, c.drills.get(drill_id))

            if path in ("/dr", "/dr/", "/dr/index.html"):
                return self._serve_static("index.html")
            if path.startswith("/dr/static/"):
                return self._serve_static(path[len("/dr/static/"):])
            self._json(404, {"error": "not_found", "path": path})
        except DrillError as exc:
            self._drill_error(exc)
        except Exception as exc:
            self.server.app_log(f"[dr] ERROR GET {path}: {exc!r}")
            self._json(500, {"error": "internal", "message": str(exc)})

    def do_POST(self) -> None:  # noqa: N802, C901
        path = urlparse(self.path).path
        c = self.components()
        try:
            # 主站 feed 故障注入（一体化测试）
            if c.feed is not None and path == "/api/feed/bump-term":
                return self._json(200, {"term": c.feed.bump_term()})
            if c.feed is not None and path == "/api/feed/inject":
                body = self._read_body()
                return self._json(200, c.feed.inject(body["kind"], int(body["index"])))
            if c.feed is not None and path == "/api/feed/clear-injection":
                return self._json(200, c.feed.clear_injection())

            if path == "/dr/api/tick":
                self._read_body()
                return self._json(200, c.rep.tick())
            if path == "/dr/api/control/pause":
                self._read_body()
                c.rep.set_paused(True)
                return self._json(200, c.rep.store.state())
            if path == "/dr/api/control/resume":
                self._read_body()
                c.rep.set_paused(False)
                return self._json(200, c.rep.store.state())
            if path == "/dr/api/control/adopt-term":
                body = self._read_body()
                term = body.get("term")
                if not isinstance(term, int) or isinstance(term, bool):
                    raise DrillError(400, "bad_term", "term 必须为整数")
                try:
                    st = c.rep.adopt_term(term)
                except ValueError as exc:
                    raise DrillError(409, "bad_term", str(exc))
                return self._json(200, st)
            if path == "/dr/api/drills":
                body = self._read_body()
                return self._json(201, c.drills.create(body))
            if path.startswith("/dr/api/drills/"):
                tail = path[len("/dr/api/drills/"):].strip("/")
                parts = [p for p in tail.split("/") if p]
                if len(parts) == 2 and parts[1] == "issue":
                    return self._json(200, c.drills.issue(parts[0], self._read_body()))
                if len(parts) == 2 and parts[1] == "cancel":
                    return self._json(200, c.drills.cancel(parts[0]))
            self._json(404, {"error": "not_found", "path": path})
        except DrillError as exc:
            self._drill_error(exc)
        except Exception as exc:
            self.server.app_log(f"[dr] ERROR POST {path}: {exc!r}")
            self._json(500, {"error": "internal", "message": str(exc)})

    # ------------------------------------------------------------------
    def _status(self) -> dict:
        c = self.components()
        with c.rep.store.lock:
            st = c.rep.store.state()
            qcount = c.rep.store.conn.execute(
                "SELECT COUNT(*) AS c FROM quarantine"
            ).fetchone()["c"]
            seg_count = c.rep.store.conn.execute(
                "SELECT COUNT(*) AS c FROM segments"
            ).fetchone()["c"]
        primary = None
        try:
            primary = c.rep.fetcher.fetch_status()
        except Exception:
            primary = None
        latency = None
        if primary is not None:
            latency = max(0, primary["current_event_seq"] - st["confirmed_seq"])
        return {
            **st,
            "segment_count": seg_count,
            "quarantine_count": qcount,
            "primary": primary,
            "live_lag_events": latency,
        }

    def _serve_static(self, rel: str, content_type: str | None = None) -> None:
        rel = rel.replace("\\", "/").lstrip("/")
        full = os.path.normpath(os.path.join(DR_STATIC_DIR, rel))
        if not full.startswith(DR_STATIC_DIR + os.sep) or not os.path.isfile(full):
            self._json(404, {"error": "not_found"})
            return
        if content_type is None:
            if full.endswith(".css"):
                content_type = "text/css; charset=utf-8"
            elif full.endswith(".js"):
                content_type = "application/javascript; charset=utf-8"
            else:
                content_type = "text/html; charset=utf-8"
        with open(full, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


class Components:
    def __init__(self, rep: Replicator, drills: DrillManager, feed: Any = None):
        self.rep = rep
        self.drills = drills
        self.feed = feed


def create_server(
    host: str,
    port: int,
    dr_db: str,
    fetcher: Any,
    *,
    poll_interval: float = 0.5,
    lag_limit: int = 1000,
    auto_adopt_term: bool = False,
    start_puller: bool = True,
    feed: Any = None,
) -> ThreadingHTTPServer:
    store = ReplicaStore(dr_db, lag_limit=lag_limit)
    rep = Replicator(
        store, fetcher,
        poll_interval=poll_interval,
        auto_adopt_term=auto_adopt_term,
    )
    drills = DrillManager(rep)
    httpd = ThreadingHTTPServer((host, port), DRHandler)
    httpd.components = Components(rep, drills, feed=feed)  # type: ignore[attr-defined]
    httpd.app_log = lambda msg: print(msg, flush=True)  # type: ignore[attr-defined]
    httpd.dr_manager = drills  # type: ignore[attr-defined]
    if start_puller:
        rep.start()
    return httpd
