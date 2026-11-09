-- Keyspace y tablas query-first de la Entrega 2 (D-09, D-12). Idempotente: IF NOT EXISTS.
-- Local (Docker): SimpleStrategy RF=1. En AstraDB el keyspace se crea desde la consola y esta sentencia se omite.
CREATE KEYSPACE IF NOT EXISTS cpa
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Q1 · Costos y requests diarios por organización y servicio en un rango de fechas.
-- Partición = org_id: una consulta = una partición (O6). Clustering por fecha permite el rango con WHERE usage_date >= ... AND <= ...
-- Tamaño: 80 orgs × 60 días × 6 servicios → ~140 filas por partición (muy por debajo de los 100 MB recomendados).
CREATE TABLE IF NOT EXISTS cpa.usage_by_org_day_service (
  org_id                    text,
  usage_date                date,
  service                   text,
  daily_cost_usd            double,
  requests                  double,
  cpu_hours                 double,
  storage_gb_hours          double,
  genai_tokens              bigint,
  carbon_kg                 double,
  events                    int,
  events_value_unparseable  int,
  org_name                  text,
  plan_tier                 text,
  run_id                    text,
  updated_at                timestamp,
  PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- Q2 · Top-N servicios por costo acumulado en los últimos 14 días para una organización.
-- El costo es clustering (DESC) para que el top-N sea un LIMIT sin ordenar en el cliente. Como las columnas de la clave
-- no se actualizan, la partición (org_id, as_of_date) se borra y se reescribe en cada corrida (versión 1.1 del diseño).
CREATE TABLE IF NOT EXISTS cpa.cost_14d_by_org_service (
  org_id        text,
  as_of_date    date,
  cost_14d_usd  double,
  service       text,
  requests_14d  double,
  window_start  date,
  run_id        text,
  PRIMARY KEY ((org_id, as_of_date), cost_14d_usd, service)
) WITH CLUSTERING ORDER BY (cost_14d_usd DESC, service ASC);
