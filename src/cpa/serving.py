"""Serving en Cassandra/AstraDB con el driver de Python (decisión abierta A5 → D-12).

Conexión (variables de entorno, nunca en el repo):
  - AstraDB: ASTRA_DB_SECURE_BUNDLE_PATH + ASTRA_DB_TOKEN (y ASTRA_DB_KEYSPACE, por defecto `cpa`)
  - Cassandra local (infra/docker-compose.yml): CASSANDRA_HOST (default 127.0.0.1) y CASSANDRA_PORT (9042)
Las escrituras son INSERT por clave primaria completa → upsert idempotente: re-ejecutar no duplica filas.
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame

from .common import ROOT

USAGE_COLS = ["org_id", "usage_date", "service", "daily_cost_usd", "requests", "cpu_hours", "storage_gb_hours",
              "genai_tokens", "carbon_kg", "events", "events_value_unparseable", "org_name", "plan_tier"]


class Serving:
    def __init__(self, keyspace: str = "cpa"):
        from cassandra.cluster import Cluster

        bundle, token = os.environ.get("ASTRA_DB_SECURE_BUNDLE_PATH"), os.environ.get("ASTRA_DB_TOKEN")
        if bundle and token:
            from cassandra.auth import PlainTextAuthProvider

            self.target = "astra"
            self.keyspace = os.environ.get("ASTRA_DB_KEYSPACE", keyspace)
            self.cluster = Cluster(cloud={"secure_connect_bundle": bundle},
                                   auth_provider=PlainTextAuthProvider("token", token))
        else:
            self.target = "local"
            self.keyspace = keyspace
            self.cluster = Cluster([os.environ.get("CASSANDRA_HOST", "127.0.0.1")],
                                   port=int(os.environ.get("CASSANDRA_PORT", "9042")))
        self.session = self.cluster.connect()

    # ------------------------------------------------------------ DDL
    def apply_schema(self, path: str = os.path.join(ROOT, "infra", "cql", "01_schema.cql")) -> None:
        with open(path) as fh:
            text = re.sub(r"--[^\n]*", "", fh.read())
        for stmt in filter(None, (s.strip() for s in text.split(";"))):
            if stmt.upper().startswith("CREATE KEYSPACE") and self.target == "astra":
                continue  # en AstraDB el keyspace se crea desde la consola
            self.session.execute(stmt.replace("cpa.", f"{self.keyspace}."))
        self.session.set_keyspace(self.keyspace)

    # ------------------------------------------------------------ cargas
    def _concurrent(self, cql: str, rows: list[tuple]) -> int:
        from cassandra.concurrent import execute_concurrent_with_args

        stmt = self.session.prepare(cql)
        results = execute_concurrent_with_args(self.session, stmt, rows, concurrency=64, raise_on_first_error=True)
        return sum(1 for ok, _ in results if ok)

    def upsert_usage(self, gold: DataFrame, run_id: str) -> int:
        """Gold org_daily_usage_by_service → usage_by_org_day_service (Q1). Volumen chico: se itera en el driver."""
        now = datetime.now(timezone.utc)
        rows = [tuple(r[c] for c in USAGE_COLS) + (run_id, now) for r in gold.select(*USAGE_COLS).toLocalIterator()]
        cols = ", ".join(USAGE_COLS + ["run_id", "updated_at"])
        marks = ", ".join("?" * (len(USAGE_COLS) + 2))
        return self._concurrent(f"INSERT INTO usage_by_org_day_service ({cols}) VALUES ({marks})", rows)

    def replace_top14(self, top: DataFrame, run_id: str) -> int:
        """Q2: borra y reescribe cada partición (org_id, as_of_date) → sin filas viejas con otro costo en la clave."""
        rows = [(r.org_id, r.as_of_date, float(r.cost_14d_usd), r.service, float(r.requests_14d or 0),
                 r.window_start, run_id) for r in top.toLocalIterator()]
        parts = sorted({(r[0], r[1]) for r in rows})
        self._concurrent("DELETE FROM cost_14d_by_org_service WHERE org_id = ? AND as_of_date = ?", parts)
        return self._concurrent(
            "INSERT INTO cost_14d_by_org_service (org_id, as_of_date, cost_14d_usd, service, requests_14d, "
            "window_start, run_id) VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

    # ------------------------------------------------------------ consultas
    def q1_usage_range(self, org_id: str, start: date, end: date) -> list:
        return list(self.session.execute(
            "SELECT usage_date, service, daily_cost_usd, requests, cpu_hours, storage_gb_hours, genai_tokens, events "
            "FROM usage_by_org_day_service WHERE org_id = %s AND usage_date >= %s AND usage_date <= %s",
            (org_id, start, end)))

    def q2_top_services(self, org_id: str, as_of: date, n: int = 5) -> list:
        return list(self.session.execute(
            "SELECT service, cost_14d_usd, requests_14d, window_start, as_of_date FROM cost_14d_by_org_service "
            "WHERE org_id = %s AND as_of_date = %s LIMIT %s", (org_id, as_of, n)))

    def count(self, table: str) -> int:
        return self.session.execute(f"SELECT COUNT(*) FROM {table}").one()[0]

    def close(self) -> None:
        self.cluster.shutdown()
