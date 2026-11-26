#!/usr/bin/env python3
import logging
import os
import signal
import sys
import time

os.environ["TZ"] = "UTC"
time.tzset()

from binance_listing.config import (  # noqa: E402
    DUCKPORT_ADDR,
    DUCKPORT_SCHEMA,
    PROXY_URL,
    SAMPLE_OFFSET_SECONDS,
    SPOT_QUOTES,
    setup_logging,
)
from binance_listing.data_jobs import ListingJobs  # noqa: E402
from binance_listing.duckport_client import DuckportClient  # noqa: E402


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info(
        "Binance listing starting offset=%ss quotes=%s proxy=%s",
        SAMPLE_OFFSET_SECONDS,
        ",".join(sorted(SPOT_QUOTES)) or "any",
        "on" if PROXY_URL else "off",
    )

    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)
    jobs: ListingJobs | None = None

    def signal_handler(signum, frame):
        logger.info("Received exit signal, shutting down...")
        if jobs is not None:
            jobs.request_stop()
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        info = client.ping()
    except Exception as exc:
        logger.error("Cannot reach duckport-rs at %s: %s", DUCKPORT_ADDR, exc)
        client.close()
        sys.exit(1)
    logger.info("duckport-rs server: %s", info)

    client.init_schema()
    if not client.read_last_states():
        logger.warning("表里还没有历史。先跑 duckport loadhist，否则现有现货都会记成刚刚才知道")
    jobs = ListingJobs(client)

    try:
        signal.pause()
    except AttributeError:
        import threading

        threading.Event().wait()


if __name__ == "__main__":
    main()
