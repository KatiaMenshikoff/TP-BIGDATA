"""Pruebas de las transformaciones de Silver/Gold (src/cpa/transforms.py) con DataFrames chicos."""
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from cpa import transforms as T
from cpa.schemas import EVENT_SCHEMA


def events(spark, rows):
    """Arma un Bronze mínimo: campos del evento + columnas técnicas + event_ts/event_date."""
    base = dict(event_id="e1", timestamp="2025-07-20T10:00:00Z", org_id="org_a", resource_id="res_a",
                service="genai", region="us-east", metric="requests", value="10", unit="count",
                cost_usd_increment=1.5, schema_version=2, carbon_kg=0.1, genai_tokens=100.0)
    data = [[{**base, **r}[f] for f in EVENT_SCHEMA.fieldNames()] for r in rows]
    df = spark.createDataFrame(data, EVENT_SCHEMA)
    df = df.withColumn("ingest_ts", F.current_timestamp()).withColumn("source_file", F.lit("p0.jsonl"))
    return T.parse_event_time(df)


@pytest.fixture
def dims(spark):
    org = spark.createDataFrame([("org_a", "Org A", "tech", "pro", "us-east", True)],
                                "org_id string, org_name string, industry string, plan_tier string, hq_region string, is_current boolean")
    res = spark.createDataFrame([("res_a", "running", False)], "resource_id string, state string, resource_has_pii boolean")
    return org, res


def test_q04_value_string_se_castea_y_conserva_value_raw(spark, dims):
    valid, q = T.conform_events(events(spark, [dict(value="12.5"), dict(event_id="e2", value="abc")]), *dims)
    rows = {r.event_id: r for r in valid.collect()}
    assert rows["e1"].value == 12.5 and not rows["e1"].flag_value_unparseable
    assert rows["e2"].value is None and rows["e2"].value_raw == "abc" and rows["e2"].flag_value_unparseable
    assert q.count() == 0


def test_q03_unit_nula_se_deriva_de_metric(spark, dims):
    valid, _ = T.conform_events(events(spark, [dict(unit=None, metric="cpu_hours")]), *dims)
    r = valid.first()
    assert r.unit == "hours" and r.flag_unit_derived


@pytest.mark.parametrize("override,rule", [
    (dict(event_id=None), "Q01"),
    (dict(cost_usd_increment=-5.0), "Q02"),
    (dict(org_id="org_x"), "Q05"),
    (dict(resource_id="res_x"), "Q05"),
    (dict(timestamp="no-es-fecha"), "Q00"),
])
def test_reglas_bloqueantes_van_a_quarantine_con_rule_id(spark, dims, override, rule):
    valid, q = T.conform_events(events(spark, [override]), *dims)
    assert valid.count() == 0
    assert [r.rule_id for r in q.collect()] == [rule]


def test_q02_tolera_redondeo_y_marca_anomalia(spark, dims):
    valid, q = T.conform_events(events(spark, [dict(cost_usd_increment=-0.01), dict(event_id="e2", cost_usd_increment=-0.02)]), *dims)
    assert [r.event_id for r in valid.collect()] == ["e1"] and valid.first().flag_cost_negative
    assert q.first().flag_cost_anomaly


def test_q01_duplicados_conservan_la_primera_ingesta(spark, dims):
    valid, q = T.conform_events(events(spark, [dict(), dict(), dict(event_id="e2")]), *dims)
    assert sorted(r.event_id for r in valid.collect()) == ["e1", "e2"]
    assert [(r.event_id, r.reason) for r in q.collect()] == [("e1", "event_id duplicado")]


def test_v1_sin_campos_v2_queda_nulo_y_suma_cero_en_gold(spark, dims):
    df = events(spark, [dict(schema_version=1, carbon_kg=None, genai_tokens=None), dict(event_id="e2")])
    valid, _ = T.conform_events(df, *dims)
    gold = T.build_daily_usage(T.enrich_events(T.event_features(valid), *dims)).first()
    assert gold.genai_tokens == 100 and gold.carbon_kg == pytest.approx(0.1) and gold.max_schema_version == 2


def test_gold_agrega_por_org_dia_servicio(spark, dims):
    df = events(spark, [dict(), dict(event_id="e2", metric="cpu_hours", value="2.5", unit="hours"),
                        dict(event_id="e3", timestamp="2025-07-21T00:00:00Z"),
                        dict(event_id="e4", service="compute")])
    valid, _ = T.conform_events(df, *dims)
    gold = {(r.usage_date, r.service): r for r in T.build_daily_usage(T.event_features(valid).withColumn(
        "org_name", F.lit("x")).withColumn("plan_tier", F.lit("pro"))).collect()}
    r = gold[(date(2025, 7, 20), "genai")]
    assert len(gold) == 3 and r.events == 2 and r.requests == 10 and r.cpu_hours == 2.5 and r.daily_cost_usd == 3.0


def test_robust_z_marca_spike_y_tolera_mad_cero(spark):
    rows = [("a", "m", float(x)) for x in [1, 1.1, 0.9, 1.0, 1.05, 50]] + [("b", "m", 2.0)] * 3
    z = T.robust_z(spark.createDataFrame(rows, "service string, metric string, cost_usd_increment double"),
                   ["service", "metric"])
    out = {(r.service, r.cost_usd_increment): r.robust_z for r in z.collect()}
    assert out[("a", 50.0)] > 3.5 and abs(out[("a", 1.0)]) < 3.5
    assert out[("b", 2.0)] == 0.0                      # MAD = 0 → z = 0, sin división por cero


def test_scd2_cambios_abren_version_y_reaplicar_es_idempotente(spark):
    cols = "org_id string, plan_tier string, record_hash string"
    v1 = spark.createDataFrame([("o1", "free", "h1"), ("o2", "pro", "h2")], cols)
    dim1 = T.scd2_merge(None, v1, "org_id", "2025-07-01")
    v2 = spark.createDataFrame([("o1", "pro", "h1b"), ("o2", "pro", "h2")], cols)
    dim2 = T.scd2_merge(dim1, v2, "org_id", "2025-08-01")
    rows = sorted((r.org_id, r.plan_tier, str(r.valid_to), r.is_current) for r in dim2.collect())
    assert rows == [("o1", "free", "2025-08-01", False), ("o1", "pro", "None", True), ("o2", "pro", "None", True)]
    dim3 = T.scd2_merge(dim2, v2, "org_id", "2025-08-01")
    assert dim3.count() == 3 and dim3.filter("is_current").count() == 2


def test_billing_usd_credits_nulos_y_flags(spark):
    df = spark.sql("""SELECT * FROM VALUES
        ('i1', CAST(100 AS DECIMAL(12,2)), CAST(NULL AS DECIMAL(12,2)), CAST(21 AS DECIMAL(12,2)), 'usd', 1.05D),
        ('i2', CAST(-10 AS DECIMAL(12,2)), CAST(0 AS DECIMAL(12,2)), CAST(0 AS DECIMAL(12,2)), 'ARS', 0.0015D)
        AS t(invoice_id, subtotal, credits, taxes, currency, exchange_rate_to_usd)""")
    out = {r.invoice_id: r for r in T.conform_billing(df, (0.99, 1.01)).collect()}
    assert float(out["i1"].total_usd) == pytest.approx(127.05) and out["i1"].flag_credits_imputed
    assert out["i1"].flag_fx_suspicious and out["i1"].currency == "USD"
    assert out["i2"].flag_credit_note and not out["i2"].flag_fx_suspicious


def test_users_email_hasheado_y_q07(spark):
    df = spark.createDataFrame([("u1", "A@x.com ", date(2025, 7, 10), date(2025, 7, 1))],
                               "user_id string, email string, created_at date, last_login date")
    r = T.conform_users(df).first()
    assert "email" not in r.asDict() and len(r.email_hash) == 64 and r.flag_login_inconsistent


def test_resources_tags_json_y_pii(spark):
    df = spark.createDataFrame([("r1", "Compute ", "US-East", '["env:prod","pii:true"]'), ("r2", "storage", "x", None)],
                               "resource_id string, service string, region string, tags_json string")
    out = {r.resource_id: r for r in T.conform_resources(df).collect()}
    assert out["r1"].resource_has_pii and out["r1"].service == "compute" and out["r1"].region == "us-east"
    assert out["r2"].tags == [] and not out["r2"].resource_has_pii


def test_top_14_dias_respeta_la_ventana(spark):
    daily = spark.createDataFrame([("o", date(2025, 8, 31), "genai", 5.0, 1.0), ("o", date(2025, 8, 18), "genai", 1.0, 1.0),
                                   ("o", date(2025, 8, 17), "genai", 100.0, 1.0)],
                                  "org_id string, usage_date date, service string, daily_cost_usd double, requests double")
    r = T.top_services_window(daily, "2025-08-31", 14).first()
    assert r.cost_14d_usd == 6.0                      # 17/08 queda fuera: ventana 18/08–31/08
