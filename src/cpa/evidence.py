"""Genera evidence/e2/: rutas, tamaños, conteos, unicidad, quarantine, run_log, streaming, anomalías y serving.

    PYTHONPATH=src python -m cpa.evidence                       # todo → evidence/e2
    PYTHONPATH=src python -m cpa.evidence --snapshot out.json   # sólo conteos (lo usa la demo de idempotencia)
Todo lo que se escribe acá se regenera desde el lake: no hay pasos manuales.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import shutil
import subprocess

from pyspark.sql import functions as F

from .common import ROOT, load_config, spark_session

TABLES = [  # (zona, tabla, clave única esperada o None)
    ("bronze", "customers_orgs", ["org_id"]), ("bronze", "users", ["user_id"]),
    ("bronze", "resources", ["resource_id"]), ("bronze", "billing_monthly", ["invoice_id"]),
    ("bronze", "support_tickets", ["ticket_id"]), ("bronze", "marketing_touches", ["touch_id"]),
    ("bronze", "nps_surveys", ["org_id", "survey_date"]), ("bronze", "usage_events", ["event_id"]),
    ("silver", "dim_org", ["org_id", "valid_from"]), ("silver", "dim_user", ["user_id"]),
    ("silver", "dim_resource", ["resource_id"]), ("silver", "fact_billing", ["invoice_id"]),
    ("silver", "fact_usage_events", ["event_id"]),
    ("gold", "org_daily_usage_by_service", ["org_id", "usage_date", "service"]),
    ("gold", "cost_anomaly_mart", ["org_id", "date", "service"]),
    ("quarantine", "silver_usage_events", None), ("quarantine", "late_events", None),
]


def rel(path: str) -> str:
    return os.path.relpath(path, ROOT)


def dir_stats(path: str) -> dict:
    files = [f for f in glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)]
    parts = {os.path.dirname(os.path.relpath(f, path)) for f in files} - {""}
    sizes = [os.path.getsize(f) for f in files]
    return dict(files=len(files), partitions=len(parts), bytes=sum(sizes),
                avg_file_kb=round(sum(sizes) / len(sizes) / 1024, 1) if sizes else 0)


def snapshot(spark, cfg) -> dict:
    """Conteos y unicidad por tabla + filas en Cassandra (si está disponible)."""
    out = {}
    for zone, table, key in TABLES:
        path = os.path.join(cfg["paths"][zone], table)
        if not os.path.isdir(path):
            continue
        df = spark.read.parquet(path)
        rows = df.count()
        info = dict(zone=zone, table=table, path=rel(path), rows=rows, **dir_stats(path))
        if key:
            info["key"] = "+".join(key)
            info["duplicate_keys"] = rows - df.select(*key).distinct().count()
        out[f"{zone}.{table}"] = info
    try:
        from .serving import Serving

        s = Serving(cfg["cassandra"]["keyspace"])
        s.session.set_keyspace(s.keyspace)
        out["cassandra"] = {t: s.count(t) for t in ["usage_by_org_day_service", "cost_14d_by_org_service"]}
        s.close()
    except Exception as exc:  # sin Cassandra la evidencia del lake se genera igual
        out["cassandra"] = {"error": str(exc)[:200]}
    return out


def write_csv(df, path: str, limit: int | None = None) -> None:
    rows = (df.limit(limit) if limit else df).collect()
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(df.columns)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])


def compare_with_mapreduce(spark, cfg) -> dict:
    """Gold vs la salida MapReduce de la E1 (evidence/mapreduce): misma lógica → mismas claves y métricas."""
    mr_path = os.path.join(ROOT, "evidence", "mapreduce", "org_daily_usage_by_service_mr.csv")
    if not os.path.exists(mr_path):
        return {"error": "falta la salida MapReduce de la E1"}
    mr = spark.read.option("header", True).option("inferSchema", True).csv(mr_path)
    gold = spark.read.parquet(os.path.join(cfg["paths"]["gold"], "org_daily_usage_by_service"))
    keys = ["org_id", "usage_date", "service"]
    mr = mr.withColumn("usage_date", F.to_date("usage_date"))
    j = gold.alias("g").join(mr.alias("m"), keys, "full_outer")
    metrics = {"daily_cost_usd": "cost_usd", "requests": "requests", "cpu_hours": "cpu_hours",
               "storage_gb_hours": "storage_gb_hours", "genai_tokens": "genai_tokens", "carbon_kg": "carbon_kg",
               "events": "events"}
    agg = j.agg(F.sum(F.col("g.events").isNull().cast("int")).alias("solo_mr"),
                F.sum(F.col("m.events").isNull().cast("int")).alias("solo_gold"),
                *[F.max(F.abs(F.col(f"g.{g}") - F.col(f"m.{m}"))).alias(g) for g, m in metrics.items()]).first()
    return dict(gold_keys=gold.count(), mapreduce_keys=mr.count(), only_in_mapreduce=agg.solo_mr,
                only_in_gold=agg.solo_gold, max_abs_diff={g: agg[g] for g in metrics})


def quarantine_evidence(spark, cfg, out: str) -> dict:
    os.makedirs(out, exist_ok=True)
    summary = {}
    for path in sorted(glob.glob(os.path.join(cfg["paths"]["quarantine"], "*"))):
        name = os.path.basename(path)
        df = spark.read.parquet(path)
        by_rule = {f"{r.rule_id} · {r.reason}": r["count"] for r in df.groupBy("rule_id", "reason").count().collect()}
        summary[name] = dict(rows=df.count(), by_rule=by_rule, path=rel(path))
        if df.count():
            cols = [c for c in ["rule_id", "reason", "event_id", "event_ts", "org_id", "resource_id", "service",
                                "metric", "value_raw", "cost_usd_increment", "source_file", "invoice_id", "user_id"]
                    if c in df.columns]
            sample = df.select(*cols)
            if "source_file" in cols:
                sample = sample.withColumn("source_file", F.regexp_extract("source_file", r"([^/]+)$", 1))
            write_csv(sample.orderBy(F.col(cols[2]) if len(cols) > 2 else F.col("rule_id")),
                      os.path.join(out, f"{name}_muestra.csv"), 10)
    return summary


def streaming_summary(cfg) -> list[dict]:
    path = os.path.join(cfg["paths"]["metadata"], "streaming_progress", "bronze_usage_events.json")
    if not os.path.exists(path):
        return []
    rows = []
    for b in json.load(open(path)):
        ops = b.get("stateOperators") or [{}]
        rows.append(dict(batch_id=b["batchId"], timestamp=b["timestamp"], input_rows=b["numInputRows"],
                         watermark=b.get("eventTime", {}).get("watermark"),
                         max_event_time=b.get("eventTime", {}).get("max"),
                         state_rows=ops[0].get("numRowsTotal"), dropped_by_watermark=ops[0].get("numRowsDroppedByWatermark"),
                         duration_ms=b.get("durationMs", {}).get("triggerExecution")))
    return rows


def main(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(ROOT, "evidence", "e2"))
    ap.add_argument("--snapshot", default=None, help="sólo escribe el snapshot de conteos en este archivo")
    ap.add_argument("--lake-root", default=None)
    args = ap.parse_args(argv)
    cfg = load_config(overrides={"paths.lake_root": os.path.abspath(args.lake_root)} if args.lake_root else None)
    spark = spark_session(cfg, "cpa-evidence")

    snap = snapshot(spark, cfg)
    if args.snapshot:
        os.makedirs(os.path.dirname(os.path.abspath(args.snapshot)), exist_ok=True)
        json.dump(snap, open(args.snapshot, "w"), indent=1, default=str)
        spark.stop()
        return

    out = args.out
    os.makedirs(out, exist_ok=True)
    json.dump(snap, open(os.path.join(out, "lake_inventory.json"), "w"), indent=1, default=str)
    with open(os.path.join(out, "lake_inventory.md"), "w") as fh:
        fh.write("| Tabla | Ruta | Filas | Particiones | Archivos | Tamaño (KB) | KB/archivo | Claves duplicadas |\n"
                 "|---|---|---|---|---|---|---|---|\n")
        for k, v in snap.items():
            if k != "cassandra":
                fh.write(f"| {k} | `{v['path']}` | {v['rows']:,} | {v['partitions']} | {v['files']} | "
                         f"{v['bytes'] / 1024:,.1f} | {v['avg_file_kb']} | {v.get('duplicate_keys', '—')} |\n")
        fh.write(f"\nCassandra: {snap.get('cassandra')}\n")

    # Esquemas reales (printSchema) de cada tabla del lake
    os.makedirs(os.path.join(out, "esquemas"), exist_ok=True)
    for k, v in snap.items():
        if k != "cassandra":
            schema = spark.read.parquet(os.path.join(ROOT, v["path"]))._jdf.schema().treeString()
            open(os.path.join(out, "esquemas", f"{k}.txt"), "w").write(schema)

    json.dump(compare_with_mapreduce(spark, cfg), open(os.path.join(out, "gold_vs_mapreduce.json"), "w"),
              indent=1, default=str)
    json.dump(quarantine_evidence(spark, cfg, os.path.join(out, "quarantine")),
              open(os.path.join(out, "quarantine", "resumen.json"), "w"), indent=1, ensure_ascii=False)

    # Muestras de Silver/Gold: value_raw vs value (Q04), unit derivada (Q03), v1 vs v2
    os.makedirs(os.path.join(out, "muestras"), exist_ok=True)
    silver = spark.read.parquet(os.path.join(cfg["paths"]["silver"], "fact_usage_events"))
    write_csv(silver.filter("flag_value_unparseable or flag_unit_derived or value is null")
              .select("event_id", "schema_version", "metric", "value_raw", "value", "unit", "flag_unit_derived",
                      "flag_value_unparseable", "carbon_kg", "genai_tokens", "org_name", "plan_tier"),
              os.path.join(out, "muestras", "silver_correcciones_Q03_Q04.csv"), 15)
    with open(os.path.join(out, "muestras", "silver_flags.json"), "w") as fh:
        json.dump({r.schema_version: r.asDict() for r in silver.groupBy("schema_version").agg(
            F.count("*").alias("eventos"), F.sum(F.col("flag_unit_derived").cast("int")).alias("Q03_unit_derivada"),
            F.sum(F.col("flag_value_unparseable").cast("int")).alias("Q04_value_no_casteable"),
            F.sum(F.col("value").isNull().cast("int")).alias("value_nulo"),
            F.sum(F.col("carbon_kg").isNotNull().cast("int")).alias("con_carbon_kg"),
            F.sum(F.col("genai_tokens").isNotNull().cast("int")).alias("con_genai_tokens")).collect()},
                  fh, indent=1, default=str)
    gold = spark.read.parquet(os.path.join(cfg["paths"]["gold"], "org_daily_usage_by_service"))
    write_csv(gold.orderBy(F.desc("daily_cost_usd")), os.path.join(out, "muestras", "gold_top20_costo.csv"), 20)
    dim_org = spark.read.parquet(os.path.join(cfg["paths"]["silver"], "dim_org"))
    write_csv(dim_org.select("org_id", "org_name", "plan_tier", "lifecycle_stage", "record_hash", "valid_from",
                             "valid_to", "is_current"), os.path.join(out, "muestras", "silver_dim_org_scd2.csv"), 10)

    # Anomalías
    os.makedirs(os.path.join(out, "analitica"), exist_ok=True)
    mart = spark.read.parquet(os.path.join(cfg["paths"]["gold"], "cost_anomaly_mart"))
    write_csv(mart.filter("is_anomaly").orderBy(F.desc("max_abs_robust_z")),
              os.path.join(out, "analitica", "top_anomalias.csv"), 15)
    s = mart.agg(F.count("*").alias("filas"), F.sum(F.col("is_anomaly").cast("int")).alias("filas_anomalas"),
                 F.sum("anomalous_events").alias("eventos_anomalos"), F.sum("events").alias("eventos"),
                 F.sum("anomalous_cost_usd").alias("costo_anomalo_usd"), F.sum("daily_cost_usd").alias("costo_total_usd"),
                 F.countDistinct(F.when(F.col("is_anomaly"), F.col("org_id"))).alias("orgs_afectadas")).first().asDict()
    s["pct_eventos_anomalos"] = round(100 * s["eventos_anomalos"] / s["eventos"], 3)
    s["pct_costo_anomalo"] = round(100 * s["costo_anomalo_usd"] / s["costo_total_usd"], 2)
    by_service = {r.service: r["n"] for r in mart.filter("is_anomaly").groupBy("service").agg(F.count("*").alias("n")).collect()}
    json.dump(dict(resumen=s, filas_anomalas_por_servicio=by_service, metodo=cfg["anomalies"]),
              open(os.path.join(out, "analitica", "metricas.json"), "w"), indent=1, default=str)

    # Streaming, reconciliación y run_log
    json.dump(streaming_summary(cfg), open(os.path.join(out, "streaming_bronze_progress.json"), "w"), indent=1)
    rec = os.path.join(cfg["paths"]["metadata"], "reconciliation.json")
    if os.path.exists(rec):
        shutil.copy(rec, os.path.join(out, "reconciliation.json"))
    run_log = spark.read.parquet(os.path.join(cfg["paths"]["metadata"], "run_log"))
    write_csv(run_log.orderBy("started_at"), os.path.join(out, "run_log.csv"))

    # Serving: captura de cqlsh (si el contenedor local está corriendo)
    os.makedirs(os.path.join(out, "serving"), exist_ok=True)
    try:
        cql = open(os.path.join(ROOT, "infra", "cql", "02_consultas.cql")).read()
        res = subprocess.run(["docker", "exec", "-i", "cpa-cassandra", "cqlsh"], input=cql, text=True,
                             capture_output=True, timeout=120)
        open(os.path.join(out, "serving", "cqlsh_consultas.txt"), "w").write(
            "-- $ docker exec -i cpa-cassandra cqlsh < infra/cql/02_consultas.cql\n\n" + cql + "\n-- Salida:\n" + res.stdout + res.stderr)
        res = subprocess.run(["docker", "exec", "cpa-cassandra", "cqlsh", "-e", "DESCRIBE KEYSPACE cpa"],
                             text=True, capture_output=True, timeout=120)
        open(os.path.join(out, "serving", "describe_keyspace.txt"), "w").write(res.stdout)
    except Exception as exc:
        print("sin captura de cqlsh:", exc)
    spark.stop()
    print(f"OK -> {rel(out)}")


if __name__ == "__main__":
    main()
