"""An in-process DuckDB standing in for duckport-server, so the real SQL runs in tests."""

import os

os.environ.setdefault("INGESTOR_ENV_FILE", "/nonexistent/config.env")

import duckdb
import pytest

from binance_listing.duckport_client import DuckportClient


class LocalDuckport(DuckportClient):
    def __init__(self):
        self.schema = "data"
        self.target = "data.binance_listing"
        self.ingestor = "test"
        self.max_lag_seconds = 900
        self.con = duckdb.connect(":memory:")
        self.con.execute("CREATE SCHEMA data")
        self.con.execute("CREATE TABLE data.config_dict (key VARCHAR PRIMARY KEY, value VARCHAR)")
        self.con.execute(
            "CREATE TABLE data.watermark (table_name VARCHAR PRIMARY KEY, ingestor VARCHAR, "
            "max_lag_seconds INTEGER, time_column VARCHAR NOT NULL, start_time TIMESTAMP, "
            "duck_time TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        self.con.execute(
            "CREATE TABLE data.exginfo (market VARCHAR, symbol VARCHAR, base_asset VARCHAR, "
            "contract_type VARCHAR)"
        )

    def close(self):
        self.con.close()

    def ping(self):
        return {"ok": True}

    def execute(self, sql):
        self.con.execute(sql)
        return {}

    def execute_transaction(self, statements):
        self.con.execute("BEGIN")
        try:
            for statement in statements:
                self.con.execute(statement)
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        self.con.execute("COMMIT")
        return {}

    def append(self, schema, table, data):
        self.con.register("_incoming", data)
        self.con.execute(f"INSERT INTO {schema}.{table} SELECT * FROM _incoming")
        self.con.unregister("_incoming")
        return {}

    def query(self, sql):
        return self.con.execute(sql).to_arrow_table().to_pylist()

    # test helpers
    def set_perps(self, perps, tradfi=None):
        self.con.execute("DELETE FROM data.exginfo")
        for symbol, base in perps.items():
            self.con.execute("INSERT INTO data.exginfo VALUES ('usdt_perp', ?, ?, 'PERPETUAL')", [symbol, base])
        for symbol, base in (tradfi or {}).items():
            self.con.execute("INSERT INTO data.exginfo VALUES ('usdt_perp', ?, ?, 'TRADIFI_PERPETUAL')", [symbol, base])

    def rows(self):
        return self.query(
            "SELECT known_at, event_time, market, symbol, venue_symbol, state, source "
            "FROM data.binance_listing ORDER BY known_at, symbol"
        )

    def duck_time(self):
        return self.query("SELECT duck_time FROM data.watermark WHERE table_name = 'binance_listing'")[0]["duck_time"]


@pytest.fixture
def db():
    client = LocalDuckport()
    client.init_schema()
    yield client
    client.close()
