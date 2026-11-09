"""Configuración, sesión Spark, run_id y run_log (observabilidad y linaje a nivel corrida, Sección 7.6 del diseño)."""
from __future__ import annotations

import os
import re
import subprocess
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _resolve(value, variables: dict):
    if isinstance(value, str):
        return re.sub(r"\$\{(\w+)\}", lambda m: str(variables[m.group(1)]), value)
    return value


def load_config(path: str | None = None, overrides: dict | None = None) -> dict:
    """Lee config/pipeline.yaml (o el .example si no existe). Las rutas relativas se resuelven contra la raíz del repo.

    `overrides` permite cambiar claves desde la CLI, p. ej. {"paths.lake_root": "/tmp/lake"}.
    Ningún secreto vive en el YAML: las credenciales de Cassandra/AstraDB se leen del entorno (.env).
    """
    if path is None:
        path = os.path.join(ROOT, "config", "pipeline.yaml")
        if not os.path.exists(path):
            path = os.path.join(ROOT, "config", "pipeline.example.yaml")
    with open(path) as fh:
        cfg = yaml.safe_load(fh)
    for dotted, val in (overrides or {}).items():
        node = cfg
        *parents, leaf = dotted.split(".")
        for p in parents:
            node = node.setdefault(p, {})
        node[leaf] = val
    paths = cfg["paths"]
    root = paths["lake_root"]
    paths["lake_root"] = root if os.path.isabs(root) else os.path.join(ROOT, root)
    for k, v in paths.items():
        if k != "lake_root":
            paths[k] = _resolve(v, {"lake_root": paths["lake_root"]})
    cfg["_config_file"] = os.path.relpath(path, ROOT)
    return cfg


def load_dotenv(path: str = os.path.join(ROOT, ".env")) -> None:
    """Carga .env sin dependencias extra (no pisa variables ya definidas)."""
    if not os.path.exists(path):
        return
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def spark_session(cfg: dict, app: str):
    from pyspark.sql import SparkSession

    s = cfg["spark"]
    spark = (SparkSession.builder.appName(app).master(s.get("master", "local[*]"))
             .config("spark.sql.shuffle.partitions", str(s.get("shuffle_partitions", 8)))
             .config("spark.sql.session.timeZone", s.get("session_timezone", "UTC"))  # D-07
             .config("spark.sql.sources.partitionOverwriteMode", s.get("partition_overwrite_mode", "dynamic"))
             .config("spark.driver.memory", s.get("driver_memory", "2g"))
             .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel(s.get("log_level", "ERROR"))
    return spark


def new_run_id() -> str:
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


def git_sha() -> str:
    try:
        sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True,
                             text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True).stdout
        return sha + ("-dirty" if dirty.strip() else "")
    except Exception:  # sin git (p. ej. Colab con zip): se registra igual
        return "unknown"


class RunLog:
    """Una fila por corrida × etapa × tabla en `_metadata/run_log` (Parquet, append).

    Permite responder "¿corrió?, ¿cuántos entraron/salieron/quedaron en quarantine?" (usuario Ingeniería de datos)
    y recorrer el linaje Gold → Silver → Bronze → source_file a través del run_id.
    """

    COLUMNS = ["run_id", "stage", "table", "rows_in", "rows_out", "rows_quarantine", "status",
               "started_at", "duration_s", "detail", "git_sha", "config_file"]

    def __init__(self, cfg: dict, run_id: str):
        self.cfg, self.run_id, self.rows = cfg, run_id, []
        self.sha = git_sha()

    def add(self, stage: str, table: str, rows_in=None, rows_out=None, rows_quarantine=None,
            status="OK", started=None, detail: str = ""):
        started = started or time.time()
        row = dict(run_id=self.run_id, stage=stage, table=table,
                   rows_in=None if rows_in is None else int(rows_in),
                   rows_out=None if rows_out is None else int(rows_out),
                   rows_quarantine=None if rows_quarantine is None else int(rows_quarantine),
                   status=status, started_at=datetime.fromtimestamp(started, timezone.utc).isoformat(),
                   duration_s=round(time.time() - started, 2), detail=detail,
                   git_sha=self.sha, config_file=self.cfg.get("_config_file", ""))
        self.rows.append(row)
        print(f"[run_log] {stage:<18} {table:<28} in={row['rows_in']} out={row['rows_out']} "
              f"q={row['rows_quarantine']} {status} {row['duration_s']}s {detail}")
        return row

    @contextmanager
    def stage(self, stage: str, table: str):
        """Registra FAILED con el mensaje de error si la etapa falla, y relanza (manejo explícito de errores)."""
        started = time.time()
        box: dict = {}
        try:
            yield box
        except Exception as exc:
            self.add(stage, table, status="FAILED", started=started, detail=f"{type(exc).__name__}: {exc}"[:500])
            self.flush()
            raise
        else:
            self.add(stage, table, box.get("rows_in"), box.get("rows_out"), box.get("rows_quarantine"),
                     started=started, detail=box.get("detail", ""))

    def flush(self):
        if not self.rows:
            return
        from pyspark.sql import SparkSession, types as T

        spark = SparkSession.getActiveSession()
        schema = T.StructType([
            T.StructField("run_id", T.StringType()), T.StructField("stage", T.StringType()),
            T.StructField("table", T.StringType()), T.StructField("rows_in", T.LongType()),
            T.StructField("rows_out", T.LongType()), T.StructField("rows_quarantine", T.LongType()),
            T.StructField("status", T.StringType()), T.StructField("started_at", T.StringType()),
            T.StructField("duration_s", T.DoubleType()), T.StructField("detail", T.StringType()),
            T.StructField("git_sha", T.StringType()), T.StructField("config_file", T.StringType())])
        df = spark.createDataFrame([[r[c] for c in self.COLUMNS] for r in self.rows], schema)
        df.coalesce(1).write.mode("append").parquet(os.path.join(self.cfg["paths"]["metadata"], "run_log"))
        self.rows = []
