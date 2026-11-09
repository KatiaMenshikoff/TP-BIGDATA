"""Orquestador de la Entrega 2: etapas idempotentes, ejecutables juntas o por separado.

    PYTHONPATH=src python -m cpa.pipeline                        # todas las etapas
    PYTHONPATH=src python -m cpa.pipeline --stages bronze_batch,silver_masters
    PYTHONPATH=src python -m cpa.pipeline --no-serving           # sin Cassandra (sólo el lake)

Etapas (en orden):
  bronze_batch     CSV de Landing → Bronze Parquet (7 fuentes) + quarantine de filas corruptas/duplicadas
  silver_masters   dim_org (SCD2), dim_user (email hasheado), dim_resource (tags), fact_billing (USD)
  bronze_stream    Structured Streaming JSONL → Bronze (watermark + dedup event_id + checkpoint)
  silver_gold      Structured Streaming sobre Bronze → foreachBatch: Silver + quarantine + Gold (+ upsert Cassandra)
  reconcile        Balance Landing vs Bronze: tardíos descartados por el watermark → quarantine/late_events
  anomalies        cost_anomaly_mart (z robusto MAD por servicio+métrica)
  serving          Cassandra: esquema, carga completa de Q1, tabla Q2 (top-N 14 días) y consultas
Cada etapa escribe filas en _metadata/run_log; si falla queda FAILED con el error y la corrida se corta.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import timedelta
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T_

from . import transforms as T
from .common import ROOT, RunLog, load_config, load_dotenv, new_run_id, spark_session
from .schemas import BATCH_SOURCES, EVENT_SCHEMA

STAGES = ["bronze_batch", "silver_masters", "bronze_stream", "silver_gold", "reconcile", "anomalies", "serving"]


def p(cfg, zone, *parts) -> str:
    return os.path.join(cfg["paths"][zone], *parts)


def exists(path: str) -> bool:
    return os.path.isdir(path) and any(not n.startswith((".", "_")) for n in os.listdir(path))


def write_partitioned(df: DataFrame, path: str, partition: str | None, mode: str = "overwrite") -> None:
    """Un archivo por partición (repartition por la columna de partición) → evita small files (D-03, R13)."""
    if partition:
        df.repartition(partition).write.mode(mode).partitionBy(partition).parquet(path)
    else:
        df.coalesce(1).write.mode(mode).parquet(path)


# ====================================================================== batch
def bronze_batch(spark: SparkSession, cfg: dict, log: RunLog) -> None:
    for name, src in BATCH_SOURCES.items():
        with log.stage("bronze_batch", name) as m:
            raw = (spark.read.schema(src["schema"]).option("header", True).option("escape", "\"").option("mode", "PERMISSIVE")
                   .option("columnNameOfCorruptRecord", "_corrupt_record")
                   .csv(p(cfg, "landing", src["file"])))
            raw = T.add_technical_columns(raw, log.run_id).withColumn("ingest_date", F.current_date()).cache()
            business = [c for c in src["schema"].fieldNames() if c != "_corrupt_record"]
            # Dedup por clave natural: se conserva la primera aparición; las copias van a quarantine (no se pierden).
            w = Window.partitionBy(*src["key"]).orderBy("source_file")
            ranked = raw.withColumn("_rn", F.row_number().over(w))
            good, bad = T.split_by_rules(ranked, [
                ("B00", "fila no parseable con el esquema explícito", F.col("_corrupt_record").isNotNull()),
                ("B01", f"clave natural nula ({'+'.join(src['key'])})",
                 reduce(lambda a, b: a | b, [F.col(k).isNull() for k in src["key"]])),
                ("B02", "clave natural duplicada", F.col("_rn") > 1),
            ])
            good = good.drop("_rn", "_corrupt_record").withColumn("record_hash", T.record_hash(business))
            write_partitioned(good, p(cfg, "bronze", name), src["partition"])
            n_bad = bad.count()
            if n_bad:
                write_partitioned(bad.drop("_rn").withColumn("quarantine_ts", F.current_timestamp()),
                                  p(cfg, "quarantine", f"bronze_{name}"), "ingest_date")
            m.update(rows_in=raw.count(), rows_out=spark.read.parquet(p(cfg, "bronze", name)).count(),
                     rows_quarantine=n_bad, detail=f"partition={src['partition']}")
            raw.unpersist()


def latest_snapshot(spark, cfg, name) -> tuple[DataFrame, object]:
    df = spark.read.parquet(p(cfg, "bronze", name))
    as_of = df.agg(F.max("ingest_date")).first()[0]
    return df.filter(F.col("ingest_date") == as_of), as_of


def silver_masters(spark: SparkSession, cfg: dict, log: RunLog) -> None:
    keep_tech = ["ingest_ts", "source_file", "run_id"]

    with log.stage("silver_masters", "dim_org (SCD2)") as m:
        snap, as_of = latest_snapshot(spark, cfg, "customers_orgs")
        snap = T.conform_orgs(snap).select("org_id", *T.ORG_ATTRS, "flag_nps_out_of_range",
                                           "flag_plan_incoherent", "record_hash", *keep_tech)
        path = p(cfg, "silver", "dim_org")
        current = spark.read.parquet(path) if exists(path) else None
        dim = T.scd2_merge(current, snap, "org_id", as_of).localCheckpoint(eager=True)  # corta el linaje: lee y pisa la misma ruta
        write_partitioned(dim, path, None)
        m.update(rows_in=snap.count(), rows_out=dim.count(),
                 detail=f"as_of={as_of} vigentes={dim.filter('is_current').count()}")

    with log.stage("silver_masters", "dim_user") as m:
        snap, _ = latest_snapshot(spark, cfg, "users")
        dim = T.conform_users(snap).drop("ingest_date")
        write_partitioned(dim, p(cfg, "silver", "dim_user"), None)
        m.update(rows_in=snap.count(), rows_out=dim.count(),
                 detail=f"Q07 login_inconsistente={dim.filter('flag_login_inconsistent').count()} · email→sha256")

    with log.stage("silver_masters", "dim_resource") as m:
        snap, _ = latest_snapshot(spark, cfg, "resources")
        dim = T.conform_resources(snap).drop("ingest_date")
        write_partitioned(dim, p(cfg, "silver", "dim_resource"), None)
        m.update(rows_in=snap.count(), rows_out=dim.count(),
                 detail=f"recursos con pii={dim.filter('resource_has_pii').count()}")

    with log.stage("silver_masters", "fact_billing") as m:
        bronze = spark.read.parquet(p(cfg, "bronze", "billing_monthly"))
        fact = T.conform_billing(bronze, tuple(cfg["quality"]["usd_fx_tolerance"]))
        write_partitioned(fact, p(cfg, "silver", "fact_billing"), "month")
        m.update(rows_in=bronze.count(), rows_out=fact.count(),
                 detail=f"Q08 fx_sospechoso={fact.filter('flag_fx_suspicious').count()} "
                        f"Q09 nota_credito={fact.filter('flag_credit_note').count()} "
                        f"Q10 credits_imputados={fact.filter('flag_credits_imputed').count()}")


# ====================================================================== streaming
def _save_progress(cfg, name: str, query, run_id: str) -> list[dict]:
    """Guarda el query progress de cada micro-batch (observabilidad, O1). Un archivo por corrida: una re-ejecución
    sin datos nuevos no pisa la evidencia de la corrida que sí procesó; `<name>.json` apunta a la última con datos."""
    # Rutas relativas a la raíz del repo: la evidencia no expone el directorio local de quien la generó.
    progress = [json.loads(p_.json.replace(ROOT + os.sep, "").replace(ROOT, "."))
                for p_ in query.recentProgress]
    out = p(cfg, "metadata", "streaming_progress")
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, f"{name}_{run_id}.json"), "w") as fh:
        json.dump(progress, fh, indent=1, default=str)
    if sum(x["numInputRows"] for x in progress):
        with open(os.path.join(out, f"{name}.json"), "w") as fh:
            json.dump(progress, fh, indent=1, default=str)
    return progress


def bronze_stream(spark: SparkSession, cfg: dict, log: RunLog) -> None:
    st = cfg["streaming"]
    bronze_path = p(cfg, "bronze", "usage_events")
    with log.stage("bronze_stream", "usage_events") as m:
        before = spark.read.parquet(bronze_path).count() if exists(bronze_path) else 0
        stream = (spark.readStream.schema(EVENT_SCHEMA).option("mode", "PERMISSIVE")
                  .option("maxFilesPerTrigger", st["max_files_per_trigger"])
                  .json(p(cfg, "landing", "usage_events_stream")))
        events = T.parse_event_time(T.add_technical_columns(stream, log.run_id))
        # Sin event_id o sin event_ts no hay clave ni tiempo de evento: no se pueden deduplicar ni "watermarkear".
        # No se pierden en silencio: la etapa reconcile los encuentra (Landing vs Bronze) y los manda a quarantine.
        events = (events.filter(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull())
                  .withWatermark("event_ts", st["watermark_delay"])                       # D-05
                  .dropDuplicatesWithinWatermark(["event_id"]))
        trigger = {"availableNow": True} if st["trigger"] == "available_now" else {"processingTime": st["trigger"]}
        query = (events.coalesce(1).writeStream.format("parquet")
                 .option("path", bronze_path)
                 .option("checkpointLocation", p(cfg, "checkpoints", "bronze_usage_events"))
                 .partitionBy("event_date").outputMode("append").trigger(**trigger)
                 .queryName("bronze_usage_events").start())
        query.awaitTermination()
        if query.exception():
            raise RuntimeError(str(query.exception()))
        progress = _save_progress(cfg, "bronze_usage_events", query, log.run_id)
        rows_in = sum(x["numInputRows"] for x in progress)
        dropped = sum(op.get("numRowsDroppedByWatermark", 0) for x in progress for op in x.get("stateOperators", []))
        after = spark.read.parquet(bronze_path).count()
        m.update(rows_in=rows_in, rows_out=after - before,
                 detail=f"micro-batches={len(progress)} watermark={st['watermark_delay']} "
                        f"descartados_por_watermark={dropped} total_bronze={after}")


def process_event_dates(spark, cfg, log: RunLog, dates: list, batch_id, serving=None) -> dict:
    """Reconstruye Silver y Gold para las fechas afectadas (D-05, punto 3-4 del diseño).

    Se leen TODOS los eventos de Bronze de esas fechas (no sólo el micro-batch) y se reescriben las particiones
    completas con overwrite dinámico: re-ejecutar o recibir un tardío produce el mismo resultado (idempotencia).
    """
    started = time.time()
    date_filter = F.col("event_date").isin(dates)
    bronze = spark.read.parquet(p(cfg, "bronze", "usage_events")).filter(date_filter)
    dim_org = spark.read.parquet(p(cfg, "silver", "dim_org"))
    dim_res = spark.read.parquet(p(cfg, "silver", "dim_resource"))

    valid, quarantine = T.conform_events(bronze, dim_org, dim_res)
    silver = (T.enrich_events(T.event_features(valid), dim_org, dim_res)
              .withColumn("silver_run_id", F.lit(log.run_id)).withColumn("silver_ts", F.current_timestamp()))
    silver_path = p(cfg, "silver", "fact_usage_events")
    write_partitioned(silver, silver_path, "event_date")
    n_q = quarantine.count()
    if n_q:
        write_partitioned(quarantine.withColumn("quarantine_ts", F.current_timestamp())
                          .withColumn("quarantine_run_id", F.lit(log.run_id)),
                          p(cfg, "quarantine", "silver_usage_events"), "event_date")
    n_in = bronze.count()
    n_silver = spark.read.parquet(silver_path).filter(date_filter).count()
    log.add("silver_events", f"fact_usage_events[batch {batch_id}]", n_in, n_silver, n_q, started=started,
            detail=f"fechas={len(dates)}")

    started = time.time()
    silver_dates = spark.read.parquet(silver_path).filter(date_filter)
    gold = T.build_daily_usage(silver_dates).withColumn("gold_run_id", F.lit(log.run_id))
    gold_path = p(cfg, "gold", "org_daily_usage_by_service")
    write_partitioned(gold, gold_path, "usage_date")
    gold_dates = spark.read.parquet(gold_path).filter(F.col("usage_date").isin(dates))
    n_gold = gold_dates.count()
    detail = f"fechas={len(dates)}"
    if serving is not None:
        detail += f" upsert_cassandra={serving.upsert_usage(gold_dates, log.run_id)}"
    log.add("gold_usage", f"org_daily_usage_by_service[batch {batch_id}]", n_silver, n_gold, started=started,
            detail=detail)
    return dict(bronze=n_in, silver=n_silver, quarantine=n_q, gold=n_gold)


def silver_gold_stream(spark: SparkSession, cfg: dict, log: RunLog, serving=None) -> None:
    st = cfg["streaming"]
    bronze_path = p(cfg, "bronze", "usage_events")
    with log.stage("silver_gold", "foreachBatch") as m:
        schema = spark.read.parquet(bronze_path).schema
        stream = (spark.readStream.schema(schema).option("maxFilesPerTrigger", st["silver_max_files_per_trigger"])
                  .parquet(bronze_path))
        batches = []

        def handle(batch_df: DataFrame, batch_id: int) -> None:
            dates = sorted(r.event_date for r in batch_df.select("event_date").distinct().collect())
            if dates:
                batches.append(process_event_dates(spark, cfg, log, dates, batch_id, serving) | {"batch_id": batch_id})

        query = (stream.writeStream.foreachBatch(handle)
                 .option("checkpointLocation", p(cfg, "checkpoints", "silver_gold_usage"))
                 .trigger(availableNow=True).queryName("silver_gold_usage").start())
        query.awaitTermination()
        if query.exception():
            raise RuntimeError(str(query.exception()))
        _save_progress(cfg, "silver_gold_usage", query, log.run_id)
        m.update(rows_in=sum(x["numInputRows"] for x in query.recentProgress) if query.recentProgress else 0,
                 detail=f"micro-batches procesados={len(batches)}")


def rebuild_all_dates(spark, cfg, log: RunLog, serving=None) -> None:
    """Backfill batch (misma función que foreachBatch): reconstruye Silver/Gold para todas las fechas de Bronze."""
    dates = sorted(r.event_date for r in spark.read.parquet(p(cfg, "bronze", "usage_events"))
                   .select("event_date").distinct().collect())
    process_event_dates(spark, cfg, log, dates, "backfill", serving)


# ====================================================================== late data
def reconcile(spark: SparkSession, cfg: dict, log: RunLog) -> dict:
    """Balance Landing vs Bronze por event_id (O4: ningún evento se pierde en silencio).

    landing_rows = bronze_rows + copias_duplicadas + tardíos_descartados + sin_clave_o_tiempo
    Los tardíos (event_id válido que no llegó a Bronze porque el watermark lo descartó) van a quarantine/late_events
    con su archivo de origen, para diagnóstico y reinyección.
    """
    with log.stage("reconcile", "late_events") as m:
        schema = T_.StructType(EVENT_SCHEMA.fields + [T_.StructField("_corrupt_record", T_.StringType())])
        landing = T.parse_event_time(
            spark.read.schema(schema).option("mode", "PERMISSIVE").json(p(cfg, "landing", "usage_events_stream"))
            .withColumn("source_file", F.col("_metadata.file_path"))).cache()
        bronze_ids = spark.read.parquet(p(cfg, "bronze", "usage_events")).select("event_id").distinct()
        keyed = landing.filter(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull())
        no_key = landing.filter(F.col("event_id").isNull() | F.col("event_ts").isNull())
        late = keyed.join(bronze_ids, "event_id", "left_anti")
        w = Window.partitionBy("event_id").orderBy("source_file")
        late = late.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        n_landing, n_keyed, n_nokey, n_late = landing.count(), keyed.count(), no_key.count(), late.count()
        n_bronze = spark.read.parquet(p(cfg, "bronze", "usage_events")).count()
        n_distinct = keyed.select("event_id").distinct().count()
        quarantine = (late.withColumn("rule_id", F.lit("L01"))
                      .withColumn("reason", F.lit(f"tardío: descartado por watermark de {cfg['streaming']['watermark_delay']}"))
                      .unionByName(no_key.withColumn("rule_id", F.lit("Q01"))
                                   .withColumn("reason", F.lit("sin event_id o sin timestamp parseable")),
                                   allowMissingColumns=True)
                      .withColumn("quarantine_ts", F.current_timestamp()).withColumn("run_id", F.lit(log.run_id)))
        # Se reescribe completa: es una foto del balance actual (idempotente).
        quarantine.coalesce(1).write.mode("overwrite").parquet(p(cfg, "quarantine", "late_events"))
        balance = dict(landing_rows=n_landing, bronze_rows=n_bronze, duplicate_copies=n_keyed - n_distinct,
                       late_dropped_by_watermark=n_late, without_key_or_time=n_nokey,
                       watermark=cfg["streaming"]["watermark_delay"])
        balance["balanced"] = n_landing == n_bronze + balance["duplicate_copies"] + n_late + n_nokey
        os.makedirs(cfg["paths"]["metadata"], exist_ok=True)
        with open(p(cfg, "metadata", "reconciliation.json"), "w") as fh:
            json.dump(balance, fh, indent=1)
        landing.unpersist()
        m.update(rows_in=n_landing, rows_out=n_bronze, rows_quarantine=n_late + n_nokey,
                 detail=" ".join(f"{k}={v}" for k, v in balance.items()))
        if not balance["balanced"]:
            raise AssertionError(f"Balance Landing vs Bronze no cierra: {balance}")
        return balance


# ====================================================================== analítica
def anomalies(spark: SparkSession, cfg: dict, log: RunLog) -> None:
    a = cfg["anomalies"]
    with log.stage("anomalies", "cost_anomaly_mart") as m:
        silver = spark.read.parquet(p(cfg, "silver", "fact_usage_events"))
        mart = T.build_cost_anomaly_mart(silver, a["group_by"], a["threshold"])
        write_partitioned(mart, p(cfg, "gold", "cost_anomaly_mart"), "date")
        stats = mart.agg(F.count("*").alias("filas"), F.sum(F.col("is_anomaly").cast("int")).alias("dias_anomalos"),
                         F.sum("anomalous_events").alias("eventos_anomalos")).first()
        m.update(rows_in=silver.count(), rows_out=stats.filas,
                 detail=f"eventos_anomalos={stats.eventos_anomalos} org-dia-servicio_anomalos={stats.dias_anomalos} "
                        f"metodo={a['method']} umbral={a['threshold']}")


# ====================================================================== serving
def serve(spark: SparkSession, cfg: dict, log: RunLog, serving) -> None:
    gold = spark.read.parquet(p(cfg, "gold", "org_daily_usage_by_service"))
    with log.stage("serving", "usage_by_org_day_service") as m:
        n = serving.upsert_usage(gold, log.run_id)
        m.update(rows_in=gold.count(), rows_out=serving.count("usage_by_org_day_service"),
                 detail=f"upserts={n} destino={serving.target}")
    with log.stage("serving", "cost_14d_by_org_service") as m:
        as_of = gold.agg(F.max("usage_date")).first()[0]
        top = (T.top_services_window(gold, as_of, cfg["serving"]["top_window_days"])
               .withColumn("window_start", F.lit(as_of - timedelta(days=cfg["serving"]["top_window_days"] - 1))))
        n = serving.replace_top14(top, log.run_id)
        m.update(rows_in=top.count(), rows_out=serving.count("cost_14d_by_org_service"),
                 detail=f"as_of={as_of} inserts={n}")


# ====================================================================== CLI
def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stages", default=",".join(STAGES), help="lista separada por comas")
    ap.add_argument("--config", default=None)
    ap.add_argument("--lake-root", default=None, help="pisa paths.lake_root (p. ej. un lake temporal de demo)")
    ap.add_argument("--watermark", default=None, help="pisa streaming.watermark_delay (demo de late data)")
    ap.add_argument("--no-serving", action="store_true", help="no conecta a Cassandra")
    args = ap.parse_args(argv)

    overrides = {}
    if args.lake_root:
        overrides["paths.lake_root"] = os.path.abspath(args.lake_root)
    if args.watermark:
        overrides["streaming.watermark_delay"] = args.watermark
    load_dotenv()
    cfg = load_config(args.config, overrides)
    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    unknown = set(stages) - set(STAGES) - {"backfill"}
    if unknown:
        ap.error(f"etapas desconocidas: {sorted(unknown)}")

    spark = spark_session(cfg, "cpa-pipeline")
    log = RunLog(cfg, new_run_id())
    print(f"run_id={log.run_id} lake={os.path.relpath(cfg['paths']['lake_root'], ROOT)} config={cfg['_config_file']}")
    serving = None
    if not args.no_serving and cfg["serving"]["enabled"] and {"silver_gold", "serving", "backfill"} & set(stages):
        from .serving import Serving

        with log.stage("serving", "connect") as m:
            serving = Serving(cfg["cassandra"]["keyspace"])
            serving.apply_schema()
            m.update(detail=f"destino={serving.target} keyspace={serving.keyspace}")
    started = time.time()
    try:
        for stage in stages:
            if stage == "bronze_batch":
                bronze_batch(spark, cfg, log)
            elif stage == "silver_masters":
                silver_masters(spark, cfg, log)
            elif stage == "bronze_stream":
                bronze_stream(spark, cfg, log)
            elif stage == "silver_gold":
                silver_gold_stream(spark, cfg, log, serving)
            elif stage == "backfill":
                rebuild_all_dates(spark, cfg, log, serving)
            elif stage == "reconcile":
                reconcile(spark, cfg, log)
            elif stage == "anomalies":
                anomalies(spark, cfg, log)
            elif stage == "serving" and serving is not None:
                serve(spark, cfg, log, serving)
        log.add("pipeline", "total", started=started, detail=f"etapas={','.join(stages)}")
    finally:
        log.flush()
        if serving is not None:
            serving.close()
        spark.stop()
    print(f"OK run_id={log.run_id}")


if __name__ == "__main__":
    main()
