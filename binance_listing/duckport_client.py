"""Arrow Flight client for data.binance_listing."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from importlib import metadata
from typing import Optional

import pyarrow as pa
import pyarrow.flight as flight

from binance_listing.config import TABLE_NAME, WATERMARK_MAX_LAG_SECONDS
from binance_listing.events import COLUMNS, MARKET_SPOT, SOURCE_BACKFILL, Event, LastState

logger = logging.getLogger(__name__)

# duckport-server answers a query with zero rows by closing the stream without a data
# message; pyarrow surfaces that as FlightInternalError with this text.
_ZERO_ROW_MESSAGE = "never sent a data message"

COLUMN_TYPES = {
    "known_at": "TIMESTAMP",
    "event_time": "TIMESTAMP",
    "market": "VARCHAR",
    "symbol": "VARCHAR",
    "venue_symbol": "VARCHAR",
    "state": "TINYINT",
    "source": "VARCHAR",
}
COL_DEFS = ", ".join(f"{name} {COLUMN_TYPES[name]}" for name in COLUMNS)
ARROW_SCHEMA = pa.schema(
    [
        ("known_at", pa.timestamp("us")),
        ("event_time", pa.timestamp("us")),
        ("market", pa.string()),
        ("symbol", pa.string()),
        ("venue_symbol", pa.string()),
        ("state", pa.int8()),
        ("source", pa.string()),
    ]
)
DEDUP_KEY = ("market", "symbol", "known_at")
STAGING = f"_staging_{TABLE_NAME}"
LIVE_SINCE_KEY = f"{TABLE_NAME}:live_since"
_TS = "%Y-%m-%d %H:%M:%S"
# exginfo.contract_type of a TradFi perpetual (stock, commodity, FX): never a spot coin.
TRADFI_CONTRACT = "TRADIFI_PERPETUAL"


def events_to_arrow(events: list[Event]) -> pa.Table:
    return pa.table(
        {name: [getattr(event, name) for event in events] for name in COLUMNS},
        schema=ARROW_SCHEMA,
    )


def insert_sql(target: str, staging: str, lo: datetime, hi: datetime) -> str:
    """Insert staged rows whose (market, symbol, known_at) is not in the table yet."""
    key = ", ".join(DEDUP_KEY)
    on = " AND ".join(f"s.{col} = t.{col}" for col in DEDUP_KEY)
    return (
        f"INSERT INTO {target} "
        f"SELECT s.* FROM (SELECT DISTINCT ON ({key}) * FROM {staging} ORDER BY {key}) s "
        f"ANTI JOIN (SELECT {key} FROM {target} "
        f"WHERE known_at BETWEEN TIMESTAMP '{lo.strftime(_TS)}' AND TIMESTAMP '{hi.strftime(_TS)}') t "
        f"ON {on}"
    )


class DuckportClient:
    WRITER = "binance-listing"
    _KEEPALIVE_OPTIONS = [
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    ]

    def __init__(self, addr: str = "localhost:50051", schema: str = "data"):
        self.location = addr if addr.startswith("grpc") else f"grpc://{addr}"
        self.schema = schema
        self.target = f"{schema}.{TABLE_NAME}"
        self.ingestor = os.getenv("INGESTOR_NAME") or os.getenv("INGESTOR_EXEC") or self.WRITER
        self.max_lag_seconds = WATERMARK_MAX_LAG_SECONDS
        self.client = self._connect()
        logger.info("DuckportClient connected: %s, schema=%s", self.location, schema)

    @staticmethod
    def _sql_str(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _connect(self):
        return flight.FlightClient(self.location, generic_options=self._KEEPALIVE_OPTIONS)

    def _reconnect(self) -> None:
        self.close()
        self.client = self._connect()
        logger.warning("DuckportClient reconnected: %s", self.location)

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass

    def _action(self, name: str, payload: dict) -> dict:
        action = flight.Action(name, json.dumps(payload).encode("utf-8"))
        try:
            results = list(self.client.do_action(action))
        except flight.FlightUnavailableError:
            self._reconnect()
            results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def ping(self) -> dict:
        return self._action("duckport.ping", {})

    def execute(self, sql: str) -> dict:
        return self._action("duckport.execute", {"sql": sql})

    def execute_transaction(self, statements: list[str]) -> dict:
        return self._action("duckport.execute_transaction", {"statements": statements})

    def append(self, schema: str, table: str, data: pa.Table) -> dict:
        descriptor = flight.FlightDescriptor.for_path("duckport.append", schema, table)
        try:
            writer, reader = self.client.do_put(descriptor, data.schema)
        except flight.FlightUnavailableError:
            self._reconnect()
            writer, reader = self.client.do_put(descriptor, data.schema)
        writer.write_table(data)
        writer.done_writing()
        buf = reader.read()
        writer.close()
        return json.loads(bytes(buf))

    def query(self, sql: str) -> list[dict]:
        ticket = flight.Ticket(json.dumps({"type": "duckport.query", "sql": sql}).encode("utf-8"))
        try:
            try:
                reader = self.client.do_get(ticket)
            except flight.FlightUnavailableError:
                self._reconnect()
                reader = self.client.do_get(ticket)
            return reader.read_all().to_pylist()
        except flight.FlightInternalError as exc:
            if _ZERO_ROW_MESSAGE in str(exc):
                return []
            raise

    # ---- schema -------------------------------------------------------------

    def init_schema(self) -> None:
        s = self.schema
        self.execute_transaction(
            [
                f"CREATE TABLE IF NOT EXISTS {self.target} ({COL_DEFS})",
                f"CREATE TABLE IF NOT EXISTS {STAGING} ({COL_DEFS})",
                f"INSERT OR REPLACE INTO {s}.config_dict (key, value) "
                f"VALUES ('write_mode:{TABLE_NAME}', 'anti_join')",
                f"INSERT INTO {s}.watermark "
                f"(table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
                f"VALUES ('{TABLE_NAME}', {self._sql_str(self.ingestor)}, {self.max_lag_seconds}, "
                f"'known_at', NULL, NULL, CURRENT_TIMESTAMP) "
                f"ON CONFLICT (table_name) DO UPDATE SET "
                f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
                f"time_column = excluded.time_column",
            ]
        )
        logger.info("Schema '%s' initialised for %s", s, TABLE_NAME)
        try:
            self.execute_transaction([self._spec_sql()])
        except Exception as exc:
            logger.info("table spec not registered (duckport-server < 1.2.0?): %s",
                        str(exc).splitlines()[0][:200])

    def _spec_sql(self) -> str:
        try:
            version = metadata.version("binance-listing")
        except metadata.PackageNotFoundError:
            version = ""

        def doc(value: object) -> str:
            return self._sql_str(json.dumps(value, ensure_ascii=True, sort_keys=True))

        def strings(values) -> str:
            return "[" + ", ".join(self._sql_str(item) for item in values) + "]::VARCHAR[]"

        checks = {
            "market": {"max_len": 32, "pattern": "[a-z0-9_]{1,32}"},
            "symbol": {"max_len": 64},
            "venue_symbol": {"max_len": 64},
            "source": {"max_len": 16, "pattern": "live|backfill"},
        }
        return (
            f"INSERT OR REPLACE INTO {self.schema}.table_specs (table_name, writer, writer_version, kind, "
            f"time_column, period_seconds, dedup_key, write_mode, columns, column_defaults, string_checks, "
            f"legacy_columns, updated_at) VALUES ('{TABLE_NAME}', '{self.WRITER}', {self._sql_str(version)}, "
            f"'append', 'known_at', NULL, {strings(DEDUP_KEY)}, 'anti_join', {doc(COLUMN_TYPES)}, "
            f"{doc({})}, {doc(checks)}, {strings([])}, CURRENT_TIMESTAMP)"
        )

    # ---- reads --------------------------------------------------------------

    def _usdt_perp_rows(self) -> list[dict]:
        return self.query(
            f"SELECT symbol, base_asset, contract_type FROM {self.schema}.exginfo "
            f"WHERE market = 'usdt_perp'"
        )

    def read_perps(self) -> dict[str, str]:
        """USDT perpetuals trading now (from the K-line ingestor's exginfo): symbol -> base asset."""
        return {
            row["symbol"]: row["base_asset"]
            for row in self._usdt_perp_rows()
            if row["symbol"] and row["base_asset"] and row["contract_type"] != TRADFI_CONTRACT
        }

    def read_tradfi_perps(self) -> set[str]:
        """USDT perpetuals trading now that are TradFi contracts (stocks, commodities): no spot coin."""
        return {
            row["symbol"]
            for row in self._usdt_perp_rows()
            if row["symbol"] and row["contract_type"] == TRADFI_CONTRACT
        }

    def read_last_states(self, at: Optional[datetime] = None) -> dict[str, LastState]:
        """perpetual symbol -> its last row; with `at`, its last row with known_at <= at."""
        upto = f" AND known_at <= TIMESTAMP '{at.strftime(_TS)}'" if at is not None else ""
        rows = self.query(
            f"SELECT symbol, state, venue_symbol, event_time, source FROM ("
            f"SELECT symbol, state, venue_symbol, event_time, source, "
            f"row_number() OVER (PARTITION BY symbol ORDER BY known_at DESC) AS rn "
            f"FROM {self.target} WHERE market = '{MARKET_SPOT}'{upto}) WHERE rn = 1"
        )
        return {
            row["symbol"]: LastState(int(row["state"]), row["venue_symbol"], row["event_time"], row["source"])
            for row in rows
        }

    def read_live_since(self) -> Optional[datetime]:
        rows = self.query(
            f"SELECT value FROM {self.schema}.config_dict WHERE key = '{LIVE_SINCE_KEY}'"
        )
        if not rows or not rows[0]["value"]:
            return None
        return datetime.strptime(rows[0]["value"], _TS)

    # ---- writes -------------------------------------------------------------

    def _watermark_sql(self, duck_time: datetime) -> str:
        # The watermark only moves forward.
        return (
            f"INSERT INTO {self.schema}.watermark "
            f"(table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
            f"VALUES ('{TABLE_NAME}', {self._sql_str(self.ingestor)}, {self.max_lag_seconds}, "
            f"'known_at', NULL, TIMESTAMP '{duck_time.strftime(_TS)}', CURRENT_TIMESTAMP) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
            f"duck_time = greatest(coalesce(watermark.duck_time, excluded.duck_time), excluded.duck_time), "
            f"updated_at = excluded.updated_at"
        )

    def _stage(self, events: list[Event]) -> list[str]:
        """Put events in staging; return the statements that merge them."""
        self.execute(f"TRUNCATE {STAGING}")
        if not events:
            return []
        self.append("main", STAGING, events_to_arrow(events))
        times = [event.known_at for event in events]
        return [insert_sql(self.target, STAGING, min(times), max(times))]

    def write_sample(self, events: list[Event], slot: datetime) -> None:
        """One live sample: its events (possibly none) and the watermark, atomically."""
        statements = self._stage(events)
        statements += [
            self._watermark_sql(slot),
            f"INSERT INTO {self.schema}.config_dict (key, value) "
            f"VALUES ('{LIVE_SINCE_KEY}', '{slot.strftime(_TS)}') ON CONFLICT (key) DO NOTHING",
            f"TRUNCATE {STAGING}",
        ]
        self.execute_transaction(statements)

    def replace_backfill(self, events: list[Event], duck_time: Optional[datetime]) -> None:
        """Swap every backfill row for `events` in one transaction; live rows stay."""
        merge = self._stage(events)
        statements = [f"DELETE FROM {self.target} WHERE source = '{SOURCE_BACKFILL}'"] + merge
        if duck_time is not None:
            statements.append(self._watermark_sql(duck_time))
        statements.append(f"TRUNCATE {STAGING}")
        self.execute_transaction(statements)
