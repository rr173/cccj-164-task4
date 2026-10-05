"""灾备复制器启动入口。

三种模式：

1) 主站（带复制 feed）
     python -m quota.replica --mode primary --quota-db ./quota.db --port 8765
   主站命令接口同原服务，额外提供 GET /api/feed/status|segments 与
   可选的测试故障注入（--allow-inject）。

2) 独立灾备节点（跨进程从主站 HTTP 拉取）
     python -m quota.replica --mode dr --primary-url http://主站:8765 \
         --dr-db ./dr.db --port 8770 --lag-limit 1000

3) 一体化演示/测试（主站 + feed + 灾备复制器同一进程）
     python -m quota.replica --mode demo --quota-db ./quota.db \
         --dr-db ./dr.db --port 8765
   写接口在 /api/...，灾备运维在 /dr/，只读演练地址在 /ro/{token}/...
"""

from __future__ import annotations

import argparse
import json
import os
from http.server import ThreadingHTTPServer

from ..http_server import Handler as PrimaryHandler
from ..service import QuotaService
from .drill import DrillManager
from .feed import install_feed
from .fetcher import DirectFetcher, HTTPFetcher
from .replicator import Replicator
from .server import Components, DRHandler, create_server as create_dr_server


class CombinedHandler(DRHandler, PrimaryHandler):
    """同一端口上同时承载主站写接口与灾备接口（演示/一体化模式）。"""

    def do_GET(self) -> None:  # noqa: N802
        from urllib.parse import urlparse

        path = urlparse(self.path).path
        if self._feed_route(path):
            return self._serve_feed_get()
        if self._is_dr_path(path):
            DRHandler.do_GET(self)
        else:
            PrimaryHandler.do_GET(self)

    def do_POST(self) -> None:  # noqa: N802
        from urllib.parse import urlparse

        path = urlparse(self.path).path
        if self._feed_route(path):
            return self._serve_feed_post()
        if self._is_dr_path(path):
            DRHandler.do_POST(self)
        else:
            PrimaryHandler.do_POST(self)

    def _serve_feed_get(self) -> None:
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(self.path)
        feed = self.server.feed
        if parsed.path == "/api/feed/status":
            return self._json(200, feed.status())
        qs = parse_qs(parsed.query)
        after = int(qs.get("after", ["0"])[0])
        limit = int(qs.get("limit", ["100"])[0])
        return self._json(200, {
            "segments": feed.fetch(after_index=after, limit=limit),
            "after": after,
        })

    def _serve_feed_post(self) -> None:
        from urllib.parse import urlparse

        path = urlparse(self.path).path
        feed = self.server.feed
        if not getattr(self.server, "allow_inject", False):
            return self._json(403, {"error": "forbidden",
                                    "message": "未启用 --allow-inject"})
        if path == "/api/feed/bump-term":
            return self._json(200, {"term": feed.bump_term()})
        if path == "/api/feed/clear-injection":
            return self._json(200, feed.clear_injection())
        if path == "/api/feed/inject":
            body = self._read_body()
            if body.get("kind") not in ("digest", "payload", "drop"):
                return self._json(400, {"error": "bad_kind"})
            return self._json(200, feed.inject(body["kind"], int(body["index"])))
        return self._json(404, {"error": "not_found"})

    @staticmethod
    def _feed_route(path: str) -> bool:
        return path.startswith("/api/feed") and not path.startswith("/api/feed/segments/")

    @staticmethod
    def _is_dr_path(path: str) -> bool:
        return (
            path.startswith("/dr")
            or path.startswith("/ro/")
        )


def _log(msg: str) -> None:
    print(msg, flush=True)


def run_primary(args: argparse.Namespace) -> None:
    from http.server import ThreadingHTTPServer as Srv

    service = QuotaService(args.quota_db)
    feed = install_feed(service, bump_on_attach=True)
    httpd = Srv((args.host, args.port), CombinedHandler)
    httpd.service = service
    httpd.feed = feed
    httpd.allow_inject = args.allow_inject
    httpd.components = None
    httpd.app_log = _log
    print(f"[primary+feed] http://{args.host}:{args.port} (term={feed.term})",
          flush=True)
    try:
        httpd.serve_forever()
    finally:
        service.shutdown()
        httpd.server_close()


def run_dr(args: argparse.Namespace) -> None:
    fetcher = HTTPFetcher(args.primary_url)
    httpd = create_dr_server(
        args.host, args.port, args.dr_db, fetcher,
        poll_interval=args.poll_interval,
        lag_limit=args.lag_limit,
        auto_adopt_term=args.auto_adopt_term,
    )
    print(f"[dr] http://{args.host}:{args.port}/dr/  <- {args.primary_url}",
          flush=True)
    try:
        httpd.serve_forever()
    finally:
        httpd.components.rep.stop()
        httpd.server_close()


def run_demo(args: argparse.Namespace) -> None:
    service = QuotaService(args.quota_db)
    # 一体化演示：主站与复制器同生命周期，安装 feed 不换届
    feed = install_feed(service, bump_on_attach=False)
    store_db = args.dr_db
    rep_store = __import__(
        "quota.replica.store", fromlist=["ReplicaStore"]
    ).ReplicaStore(store_db, lag_limit=args.lag_limit)
    rep = Replicator(
        rep_store, DirectFetcher(feed),
        poll_interval=args.poll_interval,
        auto_adopt_term=args.auto_adopt_term,
    )
    rep.start()
    drills = DrillManager(rep)

    httpd = ThreadingHTTPServer((args.host, args.port), CombinedHandler)
    httpd.service = service
    httpd.feed = feed
    httpd.allow_inject = True
    httpd.components = Components(rep, drills, feed=feed)
    httpd.app_log = _log
    print(
        f"[demo all-in-one] http://{args.host}:{args.port}/\n"
        f"  主站写接口 /api/...   灾备运维 /dr/   只读地址 /ro/{{token}}/...",
        flush=True,
    )
    try:
        httpd.serve_forever()
    finally:
        rep.stop()
        service.shutdown()
        httpd.server_close()


def main() -> None:
    p = argparse.ArgumentParser(description="配额服务只读灾备复制器")
    p.add_argument("--mode", choices=("primary", "dr", "demo"), default="demo")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--quota-db", default=os.path.join(os.getcwd(), "quota.db"))
    p.add_argument("--dr-db", default=os.path.join(os.getcwd(), "dr.db"))
    p.add_argument("--primary-url", default="http://127.0.0.1:8765")
    p.add_argument("--poll-interval", type=float, default=0.3)
    p.add_argument("--lag-limit", type=int, default=1000)
    p.add_argument("--auto-adopt-term", action="store_true",
                   help="主站换届时自动接管（默认必须显式 adopt-term）")
    p.add_argument("--allow-inject", action="store_true",
                   help="允许主站 feed 故障注入接口（仅测试环境）")
    args = p.parse_args()

    if args.mode == "primary":
        run_primary(args)
    elif args.mode == "dr":
        run_dr(args)
    else:
        run_demo(args)


if __name__ == "__main__":
    main()
