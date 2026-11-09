"""Integración del streaming (Bronze → reconcile → Silver/Gold) sobre un Landing mínimo armado en el test.

Casos: duplicado dentro y entre archivos, JSON corrupto, event_id nulo, costo inválido y un evento tardío
respecto del watermark. Verifica el balance Landing = Bronze + duplicados + tardíos + sin clave, la quarantine y
que re-ejecutar no agrega filas (checkpoint).
"""
import json
import os

import pytest

from cpa import pipeline
from cpa.common import RunLog, load_config


def ev(**kw):
    base = {"event_id": "e1", "timestamp": "2025-07-20T10:00:00Z", "org_id": "org_a", "resource_id": "res_a",
            "service": "genai", "region": "us-east", "metric": "requests", "value": "10", "unit": "count",
            "cost_usd_increment": 1.5, "schema_version": 2, "carbon_kg": 0.1, "genai_tokens": 100}
    base.update(kw)
    return json.dumps(base)


@pytest.fixture
def lake(tmp_path, spark):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    stream = tmp_path / "landing" / "usage_events_stream"
    stream.mkdir(parents=True)
    files = {
        "p0.jsonl": [ev(), ev(event_id="e2", value="abc"), ev(), "{json roto", ev(event_id=None)],
        "p1.jsonl": [ev(), ev(event_id="e3", cost_usd_increment=-5.0)],
        # tardío: 60 días antes del máximo visto en los archivos anteriores
        "p2.jsonl": [ev(event_id="e_late", timestamp="2025-05-21T10:00:00Z")],
    }
    for i, (name, lines) in enumerate(files.items()):
        f = stream / name
        f.write_text("\n".join(lines) + "\n")
        os.utime(f, (1_700_000_000 + i * 10, 1_700_000_000 + i * 10))  # orden de llegada determinístico
    cfg = load_config(overrides={"paths.lake_root": str(tmp_path), "streaming.max_files_per_trigger": 1,
                                 "streaming.watermark_delay": "1 day"})
    # dimensiones mínimas en Silver
    spark.createDataFrame([("org_a", "Org A", "tech", "pro", "us-east", True)],
                          "org_id string, org_name string, industry string, plan_tier string, hq_region string, "
                          "is_current boolean").write.parquet(cfg["paths"]["silver"] + "/dim_org")
    spark.createDataFrame([("res_a", "running", False)], "resource_id string, state string, resource_has_pii boolean") \
        .write.parquet(cfg["paths"]["silver"] + "/dim_resource")
    return cfg


def test_stream_balance_quarantine_e_idempotencia(spark, lake):
    log = RunLog(lake, "test-run")
    pipeline.bronze_stream(spark, lake, log)
    bronze = spark.read.parquet(lake["paths"]["bronze"] + "/usage_events")
    assert sorted(r.event_id for r in bronze.collect()) == ["e1", "e2", "e3"]
    assert {"ingest_ts", "source_file", "run_id", "event_date"} <= set(bronze.columns)   # O5

    balance = pipeline.reconcile(spark, lake, log)
    assert balance == dict(landing_rows=8, bronze_rows=3, duplicate_copies=2, late_dropped_by_watermark=1,
                           without_key_or_time=2, watermark="1 day", balanced=True)
    late = spark.read.parquet(lake["paths"]["quarantine"] + "/late_events")
    assert sorted((r.rule_id, r.event_id) for r in late.collect()) == [("L01", "e_late"), ("Q01", None), ("Q01", None)]

    dates = [r.event_date for r in bronze.select("event_date").distinct().collect()]
    counts = pipeline.process_event_dates(spark, lake, log, dates, 0)
    assert counts == dict(bronze=3, silver=2, quarantine=1, gold=1)
    gold = spark.read.parquet(lake["paths"]["gold"] + "/org_daily_usage_by_service").first()
    assert gold.events == 2 and gold.requests == 10 and gold.events_value_unparseable == 1

    # Re-ejecución: el checkpoint evita releer archivos; Silver/Gold se reescriben con el mismo resultado.
    pipeline.bronze_stream(spark, lake, log)
    assert spark.read.parquet(lake["paths"]["bronze"] + "/usage_events").count() == 3
    assert pipeline.process_event_dates(spark, lake, log, dates, 1) == counts
