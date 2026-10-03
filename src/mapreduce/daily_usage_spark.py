"""Equivalente PySpark (DataFrame API) del flujo MapReduce de referencia.

Misma lógica que daily_usage_mapreduce.py pero en un único DAG de Spark:
  read JSON (esquema explícito) -> filtros de calidad -> dropDuplicates(event_id)
  -> groupBy(org_id, usage_date, service).agg(...)   <- Exchange = shuffle; partial_sum = combiner

Uso (local o Colab):
    python src/mapreduce/daily_usage_spark.py [--landing datalake/landing] [--out evidence/mapreduce]
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F, types as T

# Esquema explícito unificado v1/v2 (value como string para castear con fallback controlado).
EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("org_id", T.StringType()),
    T.StructField("resource_id", T.StringType()),
    T.StructField("service", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("metric", T.StringType()),
    T.StructField("value", T.StringType()),
    T.StructField("unit", T.StringType()),
    T.StructField("cost_usd_increment", T.DoubleType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("carbon_kg", T.DoubleType()),
    T.StructField("genai_tokens", T.DoubleType()),
])


def build(spark: SparkSession, landing: str):
    raw = (spark.read.schema(EVENT_SCHEMA).json(os.path.join(landing, "usage_events_stream"))
           .withColumn("source_file", F.input_file_name()))
    valid = (raw
             .filter(F.col("event_id").isNotNull() & F.col("org_id").isNotNull() & F.col("timestamp").isNotNull())
             .filter(F.col("cost_usd_increment") >= -0.01)
             .dropDuplicates(["event_id"])
             .withColumn("usage_date", F.to_date(F.to_timestamp("timestamp")))
             .withColumn("service", F.lower(F.trim("service")))
             .withColumn("value_num", F.col("value").try_cast("double")))
    metric_sum = lambda m: F.sum(F.when(F.col("metric") == m, F.col("value_num")).otherwise(0.0)).alias(m)
    return (valid.groupBy("org_id", "usage_date", "service")
            .agg(F.sum("cost_usd_increment").alias("cost_usd"),
                 metric_sum("requests"), metric_sum("cpu_hours"), metric_sum("storage_gb_hours"),
                 F.sum(F.coalesce("genai_tokens", F.lit(0.0))).alias("genai_tokens"),
                 F.sum(F.coalesce("carbon_kg", F.lit(0.0))).alias("carbon_kg"),
                 F.count("*").alias("events")))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", default="datalake/landing")
    ap.add_argument("--out", default="evidence/mapreduce")
    args = ap.parse_args()
    spark = (SparkSession.builder.appName("cpa-mapreduce-equivalente").master("local[*]")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC")  # eventos en UTC: evita corrimiento de usage_date
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    daily = build(spark, args.landing)
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "spark_physical_plan.txt"), "w") as fh:
        fh.write(daily._jdf.queryExecution().explainString(
            spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")))
    print("filas:", daily.count())
    daily.orderBy("org_id", "usage_date", "service").show(5, truncate=False)
    (daily.orderBy("org_id", "usage_date", "service").coalesce(1)
     .write.mode("overwrite").option("header", True).csv(os.path.join(args.out, "org_daily_usage_by_service_spark")))
    spark.stop()


if __name__ == "__main__":
    main()
