"""Binance spot public REST: the symbol list and single 1m bars."""

from __future__ import annotations

import gzip
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

API_BASE = "https://api.binance.com"
# Stop and wait once this much of the per-minute weight (limit 6000) is used.
WEIGHT_SOFT_LIMIT = 3000


class SpotApiError(Exception):
    pass


class RateLimited(SpotApiError):
    def __init__(self, status: int, retry_after: float):
        super().__init__(f"HTTP {status}, retry after {retry_after:.0f}s")
        self.retry_after = retry_after


class SpotApi:
    def __init__(self, proxy: str = "", timeout: float = 20.0, base: str = API_BASE):
        handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
        self._opener = urllib.request.build_opener(*handlers)
        self._timeout = timeout
        self._base = base
        self.used_weight = 0

    def _get(self, path: str, params: Optional[dict] = None, timeout: Optional[float] = None):
        url = self._base + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
        try:
            with self._opener.open(request, timeout=timeout or self._timeout) as response:
                raw = response.read()
                weight = response.headers.get("x-mbx-used-weight-1m")
        except urllib.error.HTTPError as exc:
            if exc.code in (418, 429):
                raise RateLimited(exc.code, float(exc.headers.get("Retry-After") or 60)) from exc
            raise SpotApiError(f"HTTP {exc.code} {path}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SpotApiError(f"{path}: {exc}") from exc
        if weight and weight.isdigit():
            self.used_weight = int(weight)
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise SpotApiError(f"{path}: 返回的不是 JSON") from exc

    def symbols(self, trading_only: bool, timeout: Optional[float] = None) -> list[dict]:
        """[{symbol, base, quote, status}]. Weight 20. Without the filter, delisted pairs (BREAK) are included."""
        return self._symbols({"symbolStatus": "TRADING"} if trading_only else None, timeout)

    def break_symbols(self, timeout: Optional[float] = None) -> list[dict]:
        """Pairs in BREAK (delisted or stopped), same rows as symbols(). Weight 20."""
        return self._symbols({"symbolStatus": "BREAK"}, timeout)

    def _symbols(self, params: Optional[dict], timeout: Optional[float]) -> list[dict]:
        payload = self._get("/api/v3/exchangeInfo", params, timeout)
        rows = payload.get("symbols") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise SpotApiError("exchangeInfo: 没有 symbols 字段")
        return [
            {
                "symbol": row["symbol"],
                "base": row["baseAsset"],
                "quote": row["quoteAsset"],
                "status": row["status"],
            }
            for row in rows
        ]

    def _one_bar(self, params: dict) -> Optional[int]:
        rows = self._get("/api/v3/klines", {"interval": "1m", "limit": 1, **params})
        if not rows:
            return None
        return int(rows[0][0])

    def first_bar_ms(self, symbol: str, start_ms: int = 0) -> Optional[int]:
        """Open time of the first 1m bar at or after start_ms. Weight 2."""
        return self._one_bar({"symbol": symbol, "startTime": int(start_ms)})

    def last_bar_ms(self, symbol: str) -> Optional[int]:
        """Open time of the latest 1m bar. Weight 2."""
        return self._one_bar({"symbol": symbol})

    def pause_if_busy(self) -> None:
        if self.used_weight >= WEIGHT_SOFT_LIMIT:
            logger.info("已用权重 %s，等到下一分钟", self.used_weight)
            time.sleep(61 - time.time() % 60)
            self.used_weight = 0


def trading_by_base(rows: list[dict], quotes: frozenset[str]) -> dict[str, list[tuple[str, str]]]:
    """base asset -> [(pair symbol, quote)] for pairs in TRADING, limited to `quotes` when given."""
    out: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        if row["status"] != "TRADING":
            continue
        if quotes and row["quote"] not in quotes:
            continue
        out.setdefault(row["base"], []).append((row["symbol"], row["quote"]))
    return out
