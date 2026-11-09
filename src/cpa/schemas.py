"""Esquemas explícitos de Landing (D-04): nunca se usa inferSchema.

Los tipos destino salen de docs/02_inventario_fuentes.md. `_corrupt_record` captura las filas que no parsean
(modo PERMISSIVE) para mandarlas a quarantine en lugar de perderlas.
"""
from pyspark.sql import types as T

S, D, B, DBL = T.StringType(), T.DateType(), T.BooleanType(), T.DoubleType()
MONEY = T.DecimalType(12, 2)
CORRUPT = T.StructField("_corrupt_record", S)


def _schema(*fields) -> T.StructType:
    return T.StructType([T.StructField(n, t) for n, t in fields] + [CORRUPT])


# Fuentes batch: archivo, clave natural (dedup), esquema, columna de partición en Bronze (D-03).
BATCH_SOURCES = {
    "customers_orgs": dict(file="customers_orgs.csv", key=["org_id"], partition="ingest_date", schema=_schema(
        ("org_id", S), ("org_name", S), ("industry", S), ("hq_region", S), ("plan_tier", S),
        ("is_enterprise", B), ("signup_date", D), ("sales_rep", S), ("lifecycle_stage", S),
        ("marketing_source", S), ("nps_score", DBL))),
    "users": dict(file="users.csv", key=["user_id"], partition="ingest_date", schema=_schema(
        ("user_id", S), ("org_id", S), ("email", S), ("role", S), ("active", B), ("created_at", D),
        ("last_login", D))),
    "resources": dict(file="resources.csv", key=["resource_id"], partition="ingest_date", schema=_schema(
        ("resource_id", S), ("org_id", S), ("service", S), ("region", S), ("created_at", D), ("state", S),
        ("tags_json", S))),
    "billing_monthly": dict(file="billing_monthly.csv", key=["invoice_id"], partition="month", schema=_schema(
        ("invoice_id", S), ("org_id", S), ("month", D), ("subtotal", MONEY), ("credits", MONEY),
        ("taxes", MONEY), ("currency", S), ("exchange_rate_to_usd", DBL))),
    "support_tickets": dict(file="support_tickets.csv", key=["ticket_id"], partition="ingest_date", schema=_schema(
        ("ticket_id", S), ("org_id", S), ("category", S), ("severity", S), ("created_at", D),
        ("resolved_at", D), ("csat", DBL), ("sla_breached", B))),
    "marketing_touches": dict(file="marketing_touches.csv", key=["touch_id"], partition="ingest_date",
                              schema=_schema(("touch_id", S), ("org_id", S), ("campaign", S), ("channel", S),
                                             ("timestamp", D), ("clicked", B), ("converted", B))),
    "nps_surveys": dict(file="nps_surveys.csv", key=["org_id", "survey_date"], partition="ingest_date",
                        schema=_schema(("org_id", S), ("survey_date", D), ("nps_score", DBL), ("comment", S))),
}

# Eventos: esquema unificado v1/v2. `value` se lee como string (1.309 llegan como texto) y se castea en Silver
# con try_cast; `carbon_kg` y `genai_tokens` sólo existen en v2 y quedan nulos en v1.
EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", S), T.StructField("timestamp", S), T.StructField("org_id", S),
    T.StructField("resource_id", S), T.StructField("service", S), T.StructField("region", S),
    T.StructField("metric", S), T.StructField("value", S), T.StructField("unit", S),
    T.StructField("cost_usd_increment", DBL), T.StructField("schema_version", T.IntegerType()),
    T.StructField("carbon_kg", DBL), T.StructField("genai_tokens", DBL),
])

EXPECTED_UNIT = {"requests": "count", "cpu_hours": "hours", "storage_gb_hours": "gb_hours"}
METRICS = list(EXPECTED_UNIT)
