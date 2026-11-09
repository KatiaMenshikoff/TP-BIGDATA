"""Transformaciones puras (DataFrame → DataFrame) compartidas por batch y streaming (D-01).

Ninguna función lee ni escribe disco: así se prueban con DataFrames chicos (tests/) y se reutilizan dentro de
`foreachBatch` y en el backfill batch sin duplicar lógica. Las reglas Q01–Q11 son las de la Sección 7.4 del diseño.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, Window, functions as F

from .schemas import EXPECTED_UNIT, METRICS

QUARANTINE_COLS = ["rule_id", "reason"]


# ---------------------------------------------------------------- utilidades
def record_hash(cols: list[str]):
    """Hash estable de las columnas de negocio: detecta cambios para SCD2 y sirve de huella de la fila."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]), 256)


def split_by_rules(df: DataFrame, rules: list[tuple[str, str, object]]) -> tuple[DataFrame, DataFrame]:
    """Aplica reglas bloqueantes en orden. Cada fila inválida va a quarantine con la PRIMERA regla que falla.

    rules: [(rule_id, reason, condición_de_falla)]. Devuelve (válidas, quarantine con rule_id y reason).
    """
    rule_id = reduce(lambda acc, r: acc.when(r[2], F.lit(r[0])), rules[1:],
                     F.when(rules[0][2], F.lit(rules[0][0])))
    reason = reduce(lambda acc, r: acc.when(r[2], F.lit(r[1])), rules[1:],
                    F.when(rules[0][2], F.lit(rules[0][1])))
    tagged = df.withColumn("rule_id", rule_id).withColumn("reason", reason)
    return tagged.filter(F.col("rule_id").isNull()).drop(*QUARANTINE_COLS), tagged.filter(F.col("rule_id").isNotNull())


# ---------------------------------------------------------------- Bronze
def add_technical_columns(df: DataFrame, run_id: str, source_file_col="_metadata.file_path") -> DataFrame:
    """Columnas técnicas obligatorias en Bronze (O5): ingest_ts, source_file, run_id."""
    return (df.withColumn("ingest_ts", F.current_timestamp())
              .withColumn("source_file", F.col(source_file_col))
              .withColumn("run_id", F.lit(run_id)))


def parse_event_time(df: DataFrame) -> DataFrame:
    """`timestamp` ISO-8601 con Z → event_ts (UTC por D-07) y event_date (partición).

    try_to_timestamp: con ANSI activo (default en Spark 4) un timestamp mal formado haría fallar todo el job;
    así queda nulo y la fila termina en quarantine (Q00) en lugar de cortar el stream (detectado por los tests).
    """
    return (df.withColumn("event_ts", F.try_to_timestamp(F.col("timestamp")))
              .withColumn("event_date", F.to_date("event_ts")))


# ---------------------------------------------------------------- Silver · eventos
def conform_events(bronze: DataFrame, dim_org: DataFrame, dim_resource: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze usage_events → (fact_usage_events, quarantine).

    Correcciones (no descartan): Q03 unit derivada de metric · Q04 value con try_cast y value_raw conservado ·
    normalización de texto · unificación v1/v2.
    Bloqueantes (→ quarantine): Q01 event_id nulo o duplicado · Q02 costo < -0.01 · Q05 org/resource inexistente.
    """
    unit_map = F.create_map(*[F.lit(x) for kv in EXPECTED_UNIT.items() for x in kv])
    norm = (bronze
            .withColumn("service", F.lower(F.trim("service")))
            .withColumn("region", F.lower(F.trim("region")))
            .withColumn("metric", F.lower(F.trim("metric")))
            .withColumn("value_raw", F.col("value"))
            .withColumn("value", F.col("value").try_cast("double"))                          # Q04
            .withColumn("flag_value_unparseable", F.col("value_raw").isNotNull() & F.col("value").isNull())
            .withColumn("flag_unit_derived", F.col("unit").isNull() & F.col("metric").isNotNull())
            .withColumn("unit", F.coalesce(F.col("unit"), unit_map[F.col("metric")]))      # Q03
            .withColumn("schema_version", F.coalesce("schema_version", F.lit(1)))
            .withColumn("flag_cost_negative", F.col("cost_usd_increment") < 0))

    # Q01 (duplicados): se conserva la primera ingesta de cada event_id; las demás copias van a quarantine.
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").asc(), F.col("source_file").asc())
    norm = norm.withColumn("_dup_rank", F.when(F.col("event_id").isNotNull(), F.row_number().over(w)))

    orgs = dim_org.select("org_id", F.lit(True).alias("_org_ok")).distinct()
    res = dim_resource.select("resource_id", F.lit(True).alias("_res_ok"))
    joined = (norm.join(F.broadcast(orgs), "org_id", "left")
                  .join(F.broadcast(res), "resource_id", "left"))

    valid, quarantine = split_by_rules(joined, [
        ("Q01", "event_id nulo", F.col("event_id").isNull()),
        ("Q01", "event_id duplicado", F.col("_dup_rank") > 1),
        ("Q00", "event_ts nulo o no parseable", F.col("event_ts").isNull()),
        ("Q02", "cost_usd_increment < -0.01 (anomalía de costo)", F.col("cost_usd_increment") < -0.01),
        ("Q02", "cost_usd_increment nulo", F.col("cost_usd_increment").isNull()),
        ("Q05", "org_id inexistente en dim_org", F.col("_org_ok").isNull()),
        ("Q05", "resource_id inexistente en dim_resource", F.col("_res_ok").isNull()),
    ])
    quarantine = quarantine.withColumn("flag_cost_anomaly", F.col("rule_id") == "Q02")
    helper = ["_dup_rank", "_org_ok", "_res_ok"]
    return valid.drop(*helper), quarantine.drop(*helper)


def event_features(fact: DataFrame) -> DataFrame:
    """Features por evento (consigna 4.4): daily_cost_usd (suma de cost_usd), requests, cpu_hours,
    storage_gb_hours (pivot de metric/value), genai_tokens y carbon_kg (sólo v2)."""
    out = fact.withColumn("cost_usd", F.col("cost_usd_increment"))
    for m in METRICS:
        out = out.withColumn(m, F.when(F.col("metric") == m, F.coalesce(F.col("value"), F.lit(0.0))).otherwise(0.0))
    return out


def enrich_events(fact: DataFrame, dim_org: DataFrame, dim_resource: DataFrame) -> DataFrame:
    """Joins de enriquecimiento con dimensiones chicas (broadcast): atributos de la org vigente y del recurso."""
    org = dim_org.filter("is_current").select("org_id", "org_name", "industry", "plan_tier", "hq_region")
    res = dim_resource.select("resource_id", F.col("state").alias("resource_state"), "resource_has_pii")
    return fact.join(F.broadcast(org), "org_id", "left").join(F.broadcast(res), "resource_id", "left")


# ---------------------------------------------------------------- Gold
def build_daily_usage(silver_events: DataFrame) -> DataFrame:
    """Mart FinOps org_daily_usage_by_service · grano org_id × usage_date × service (consigna 7.3)."""
    ev = event_features(silver_events) if "requests" not in silver_events.columns else silver_events
    return (ev.groupBy("org_id", F.col("event_date").alias("usage_date"), "service")
              .agg(F.round(F.sum("cost_usd"), 6).alias("daily_cost_usd"),
                   F.sum("requests").alias("requests"),
                   F.round(F.sum("cpu_hours"), 6).alias("cpu_hours"),
                   F.round(F.sum("storage_gb_hours"), 6).alias("storage_gb_hours"),
                   F.sum(F.coalesce("genai_tokens", F.lit(0.0))).cast("long").alias("genai_tokens"),
                   F.round(F.sum(F.coalesce("carbon_kg", F.lit(0.0))), 6).alias("carbon_kg"),
                   F.count("*").alias("events"),
                   F.sum(F.col("flag_value_unparseable").cast("int")).alias("events_value_unparseable"),
                   F.max("schema_version").alias("max_schema_version"),
                   F.first("org_name", ignorenulls=True).alias("org_name"),
                   F.first("plan_tier", ignorenulls=True).alias("plan_tier")))


def robust_z(events: DataFrame, group_by: list[str], value_col: str = "cost_usd_increment") -> DataFrame:
    """z robusto = 0,6745·(x − mediana)/MAD por grupo (D-08). Si MAD = 0 el z queda en 0 (grupo constante)."""
    med = events.groupBy(*group_by).agg(F.percentile(value_col, 0.5).alias("_median"))
    dev = events.join(med, group_by).withColumn("_absdev", F.abs(F.col(value_col) - F.col("_median")))
    mad = dev.groupBy(*group_by).agg(F.percentile("_absdev", 0.5).alias("_mad"))
    return (dev.join(mad, group_by)
               .withColumn("robust_z", F.when(F.col("_mad") > 0,
                                              0.6745 * (F.col(value_col) - F.col("_median")) / F.col("_mad"))
                                        .otherwise(F.lit(0.0)))
               .withColumnRenamed("_median", "group_median").withColumnRenamed("_mad", "group_mad")
               .drop("_absdev"))


def build_cost_anomaly_mart(silver_events: DataFrame, group_by: list[str], threshold: float) -> DataFrame:
    """cost_anomaly_mart · grano org_id × date × service. Flag = algún evento del día con |z robusto| > umbral."""
    scored = robust_z(silver_events, group_by).withColumn("is_outlier", F.abs("robust_z") > threshold)
    return (scored.groupBy("org_id", F.col("event_date").alias("date"), "service")
                  .agg(F.round(F.sum("cost_usd_increment"), 6).alias("daily_cost_usd"),
                       F.count("*").alias("events"),
                       F.sum(F.col("is_outlier").cast("int")).alias("anomalous_events"),
                       F.round(F.sum(F.when(F.col("is_outlier"), F.col("cost_usd_increment")).otherwise(0.0)), 6)
                        .alias("anomalous_cost_usd"),
                       F.round(F.max(F.abs("robust_z")), 4).alias("max_abs_robust_z"))
                  .withColumn("is_anomaly", F.col("anomalous_events") > 0)
                  .withColumn("method", F.lit(f"robust_z_mad|{'+'.join(group_by)}|{threshold}")))


def top_services_window(daily: DataFrame, as_of, days: int = 14) -> DataFrame:
    """Q2: costo acumulado por org × servicio en los `days` días que terminan en `as_of` (inclusive)."""
    start = F.date_sub(F.lit(as_of).cast("date"), days - 1)
    return (daily.filter((F.col("usage_date") >= start) & (F.col("usage_date") <= F.lit(as_of).cast("date")))
                 .groupBy("org_id", "service")
                 .agg(F.round(F.sum("daily_cost_usd"), 4).alias("cost_14d_usd"),
                      F.sum("requests").alias("requests_14d"))
                 .withColumn("as_of_date", F.lit(as_of).cast("date")))


# ---------------------------------------------------------------- Silver · maestros
ORG_ATTRS = ["org_name", "industry", "hq_region", "plan_tier", "is_enterprise", "signup_date", "sales_rep",
             "lifecycle_stage", "marketing_source", "nps_score"]


def conform_orgs(snapshot: DataFrame) -> DataFrame:
    """Normaliza el snapshot de orgs y marca incoherencias (no corrige la fuente)."""
    return (snapshot
            .withColumn("hq_region", F.lower(F.trim("hq_region")))
            .withColumn("plan_tier", F.lower(F.trim("plan_tier")))
            .withColumn("flag_nps_out_of_range", (F.col("nps_score") < -100) | (F.col("nps_score") > 100))
            .withColumn("nps_score", F.when(F.col("nps_score").between(-100, 100), F.col("nps_score")))
            .withColumn("flag_plan_incoherent", (F.col("plan_tier") == "enterprise") != F.col("is_enterprise"))
            .withColumn("record_hash", record_hash(ORG_ATTRS)))


def scd2_merge(current: DataFrame | None, snapshot: DataFrame, key: str, as_of) -> DataFrame:
    """SCD tipo 2 por hash de atributos. Idempotente: re-aplicar el mismo snapshot no cambia la tabla.

    - clave nueva → versión abierta (valid_from = as_of, valid_to = null, is_current = true)
    - hash distinto → se cierra la versión vigente (valid_to = as_of) y se abre una nueva
    - hash igual → sin cambios. Las claves que desaparecen del snapshot se conservan (no hay bajas en la fuente).
    """
    as_of = F.lit(as_of).cast("date")
    snap = snapshot.withColumn("valid_from", as_of).withColumn("valid_to", F.lit(None).cast("date")) \
                   .withColumn("is_current", F.lit(True))
    if current is None:
        return snap
    cols = snap.columns
    cur = current.filter("is_current").select(key, F.col("record_hash").alias("_cur_hash"))
    history = current.filter(~F.col("is_current")).select(*cols)
    open_rows = current.filter("is_current").select(*cols)
    changed = (snap.join(cur, key, "left")
                   .filter(F.col("_cur_hash").isNull() | (F.col("_cur_hash") != F.col("record_hash")))
                   .select(*cols))
    changed_keys = changed.select(key).distinct().withColumn("_changed", F.lit(True))
    reopened = (open_rows.join(changed_keys, key, "left")
                .withColumn("valid_to", F.when(F.col("_changed"), as_of).otherwise(F.col("valid_to")))
                .withColumn("is_current", F.when(F.col("_changed"), F.lit(False)).otherwise(F.lit(True)))
                .select(*cols))
    return history.unionByName(reopened).unionByName(changed)


def conform_users(snapshot: DataFrame) -> DataFrame:
    """PII: el email se reemplaza por su SHA-256 (D-10). Q07: last_login < created_at → flag (advertencia)."""
    return (snapshot
            .withColumn("email_hash", F.sha2(F.lower(F.trim("email")), 256))
            .drop("email")
            .withColumn("flag_login_inconsistent", F.col("last_login") < F.col("created_at")))


def conform_resources(snapshot: DataFrame) -> DataFrame:
    """tags_json (JSON embebido) → array<string>; flag de PII a partir del tag `pii:true`."""
    tags = F.from_json(F.col("tags_json"), "array<string>")
    return (snapshot
            .withColumn("service", F.lower(F.trim("service")))
            .withColumn("region", F.lower(F.trim("region")))
            .withColumn("tags", F.coalesce(tags, F.array().cast("array<string>")))
            .withColumn("resource_has_pii", F.array_contains("tags", "pii:true"))
            .drop("tags_json"))


def conform_billing(bronze: DataFrame, fx_tolerance: tuple[float, float]) -> DataFrame:
    """fact_billing: Q10 credits nulo → 0 · Q09 nota de crédito · Q08 FX de USD fuera de tolerancia ·
    total en USD = (subtotal − credits + taxes) × exchange_rate_to_usd (D-06, supuesto S3)."""
    lo, hi = fx_tolerance
    b = (bronze
         .withColumn("flag_credits_imputed", F.col("credits").isNull())
         .withColumn("credits", F.coalesce("credits", F.lit(0).cast("decimal(12,2)")))
         .withColumn("currency", F.upper(F.trim("currency")))
         .withColumn("flag_credit_note", F.col("subtotal") < 0)
         .withColumn("flag_fx_suspicious", (F.col("currency") == "USD") &
                     ~F.col("exchange_rate_to_usd").between(lo, hi)))
    total_local = F.col("subtotal") - F.col("credits") + F.col("taxes")
    fx = F.col("exchange_rate_to_usd")
    return (b.withColumn("total_local", total_local)
             .withColumn("subtotal_usd", F.round(F.col("subtotal") * fx, 2))
             .withColumn("credits_usd", F.round(F.col("credits") * fx, 2))
             .withColumn("taxes_usd", F.round(F.col("taxes") * fx, 2))
             .withColumn("total_usd", F.round(total_local * fx, 2)))
