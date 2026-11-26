"""Environment-driven configuration for binance-listing."""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv

_env_file = os.getenv("INGESTOR_ENV_FILE", "config.env")
load_dotenv(_env_file, override=False)
load_dotenv("/opt/duckport/server.env", override=False)

SAMPLE_PERIOD_SECONDS = 300


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
    offset = int(raw)
    if not 0 <= offset < SAMPLE_PERIOD_SECONDS:
        raise ValueError(f"SAMPLE_OFFSET_SECONDS 必须在 0..{SAMPLE_PERIOD_SECONDS - 1} 之间")
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
