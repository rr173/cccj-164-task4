"""分段拉取客户端。

* :class:`HTTPFetcher` —— 跨进程灾备节点通过主站 feed HTTP 接口拉取；
* :class:`DirectFetcher` —— 一体化部署 / 测试中直接调用内存中的
  :class:`~quota.replica.feed.SegmentFeed`，语义与 HTTP 完全一致。

传输中断（连接失败 / 超时 / 非 200）一律抛异常，由复制器的 tick
吞掉并在下轮重试 —— 已确认边界不前进，天然从最后确认继续。
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


class FetchError(RuntimeError):
    pass


class HTTPFetcher:
    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str) -> Any:
        req = urllib.request.Request(self.base_url + path, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                if resp.status != 200:
                    raise FetchError(f"HTTP {resp.status}")
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise FetchError(f"HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise FetchError(str(exc)) from exc

    def fetch_status(self) -> dict[str, Any]:
        return self._get("/api/feed/status")

    def fetch_segments(self, after_index: int = 0,
                       limit: int = 100) -> list[dict[str, Any]]:
        return self._get(
            f"/api/feed/segments?after={after_index}&limit={limit}"
        )["segments"]


class DirectFetcher:
    """进程内直连（主站 feed 与灾备复制器同进程）。"""

    def __init__(self, feed: Any) -> None:
        self.feed = feed

    def fetch_status(self) -> dict[str, Any]:
        return self.feed.status()

    def fetch_segments(self, after_index: int = 0,
                       limit: int = 100) -> list[dict[str, Any]]:
        return self.feed.fetch(after_index=after_index, limit=limit)
