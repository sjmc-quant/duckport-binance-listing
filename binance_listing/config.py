"""Environment-driven configuration for binance-listing."""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv

_env_file = os.getenv("INGESTOR_ENV_FILE", "config.env")
load_dotenv(_env_file, override=False)
load_dotenv("/opt/duckport/server.env", override=False)

SAMPLE_PERIOD_SECONDS = 300
# A sample is written within this many seconds of its slot or dropped (known_at = the slot).
WRITE_DEADLINE_SECONDS = 90


def _derive_duckport_addr() -> str:
    raw = os.getenv("DUCKPORT_LISTEN_ADDR", "0.0.0.0:50051")
    host, _, port = raw.rpartition(":")
    host = "localhost" if host in ("0.0.0.0", "", "*") else host
    return f"{host}:{port}"


def parse_quotes(raw: str) -> frozenset[str]:
    """Empty set means every quote asset counts."""
    quotes = {part.strip().upper() for part in raw.split(",") if part.strip()}
    for quote in quotes:
        if not quote.isalnum():
            raise ValueError(f"SPOT_QUOTES 含非法计价资产: {quote!r}")
    return frozenset(quotes)


def parse_offset(raw: str) -> int:
    """Rows say "known at the slot" but are written up to WRITE_DEADLINE_SECONDS later. No 5-minute
    bar may close in between, and the slot itself may not be a close: either way a backtest would
    see the row at a close where live did not have it yet."""
    offset = int(raw)
    upper = SAMPLE_PERIOD_SECONDS - WRITE_DEADLINE_SECONDS - 1
    if not 1 <= offset <= upper:
        raise ValueError(f"SAMPLE_OFFSET_SECONDS 必须在 1..{upper} 之间（写入最多晚 {WRITE_DEADLINE_SECONDS} 秒，不能跨过 5 分钟收盘）")
    return offset


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


DUCKPORT_ADDR = _derive_duckport_addr()
DUCKPORT_SCHEMA = "data"
TABLE_NAME = "binance_listing"

PROXY_URL = os.getenv("PROXY_URL", "")
SPOT_QUOTES = parse_quotes(os.getenv("SPOT_QUOTES", ""))
SAMPLE_OFFSET_SECONDS = parse_offset(os.getenv("SAMPLE_OFFSET_SECONDS", "150"))
WATERMARK_MAX_LAG_SECONDS = int(os.getenv("WATERMARK_MAX_LAG_SECONDS", "900"))
if WATERMARK_MAX_LAG_SECONDS <= 0:
    raise ValueError("WATERMARK_MAX_LAG_SECONDS 必须为正")
