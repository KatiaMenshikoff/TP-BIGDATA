# Perfil inicial de Landing

Generado: 2026-10-03T00:33:38+00:00 · Landing: `datalake/landing`

## Inventario

| fuente | filas | columnas | KB | clave | dup_clave |
|---|---|---|---|---|---|
| customers_orgs.csv | 80 | 11 | 7.6 | org_id | 0 |
| users.csv | 800 | 7 | 69.0 | user_id | 0 |
| resources.csv | 400 | 7 | 35.8 | resource_id | 0 |
| support_tickets.csv | 1000 | 8 | 69.3 | ticket_id | 0 |
| marketing_touches.csv | 1500 | 7 | 99.6 | touch_id | 0 |
| nps_surveys.csv | 92 | 4 | 3.9 | org_id+survey_date | 0 |
| billing_monthly.csv | 240 | 8 | 15.5 | invoice_id | 0 |
| usage_events_stream/*.jsonl | 43200 | 11-13 | 12624.5 | event_id | 0 |


## customers_orgs.csv

Filas: **80** · Columnas: 11 · Clave `org_id` duplicada: 0

| column | inferred_type | nulls | null_pct | distinct | min | max | examples |
|---|---|---|---|---|---|---|---|
| org_id | string | 0 | 0.0 | 80 |  |  | ['org_xaji0y6d', 'org_pbhsahxt', 'org_hv3a3zmf', 'org_8mdd4v30'] |
| org_name | string | 0 | 0.0 | 80 |  |  | ['Nimbus Labs 0', 'Nova Tech 1', 'Gamma Data 2', 'Apex Data 3'] |
| industry | string | 0 | 0.0 | 10 |  |  | ['Education', 'Media', 'Manufacturing', 'E-commerce'] |
| hq_region | string | 0 | 0.0 | 7 |  |  | ['sa-east', 'us-east', 'eu-central', 'us-west'] |
| plan_tier | string | 0 | 0.0 | 4 |  |  | ['standard', 'pro', 'free', 'enterprise'] |
| is_enterprise | boolean | 0 | 0.0 | 2 |  |  | ['True', 'False'] |
| signup_date | date | 0 | 0.0 | 40 | 2025-05-04 | 2025-07-02 | ['2025-05-26', '2025-06-16', '2025-05-12', '2025-05-30'] |
| sales_rep | string | 0 | 0.0 | 5 |  |  | ['rep_c', 'rep_d', 'rep_b', 'rep_e'] |
| lifecycle_stage | string | 0 | 0.0 | 5 |  |  | ['churned', 'lead', 'active', 'at_risk'] |
| marketing_source | string | 0 | 0.0 | 5 |  |  | ['partner', 'event', 'organic', 'ads'] |
| nps_score | numeric | 11 | 13.75 | 45 | -38.0 | 101.0 | ['-3.0', '4.0', '12.0', '17.0'] |

**Hallazgos:**
- nps_score nulo en 11 orgs; fuera de [-100,100]: 1
- plan_tier vs is_enterprise inconsistentes: 25 de 80


## users.csv

Filas: **800** · Columnas: 7 · Clave `user_id` duplicada: 0

| column | inferred_type | nulls | null_pct | distinct | min | max | examples |
|---|---|---|---|---|---|---|---|
| user_id | string | 0 | 0.0 | 800 |  |  | ['user_9tze5r5u', 'user_qpgb7r3o', 'user_cwbfuk9e', 'user_1v2isqp4'] |
| org_id | string | 0 | 0.0 | 80 |  |  | ['org_3t60rjiw', 'org_afeyuhz1', 'org_ykl1cq99', 'org_g8sbi4q2'] |
| email | string | 0 | 0.0 | 800 |  |  | ['user_9tze5r5u@example.com', 'user_qpgb7r3o@example.com', 'user_cwbfuk9e@example.com', 'user_1v2isqp4@example.com'] |
| role | string | 0 | 0.0 | 6 |  |  | ['devops', 'developer', 'ml_engineer', 'data_engineer'] |
| active | boolean | 0 | 0.0 | 2 |  |  | ['True', 'False'] |
| created_at | date | 0 | 0.0 | 100 | 2025-05-04 | 2025-08-11 | ['2025-07-10', '2025-07-15', '2025-06-12', '2025-05-12'] |
| last_login | date | 139 | 17.38 | 110 | 2025-05-14 | 2025-08-31 | ['2025-08-10', '2025-07-26', '2025-08-11', '2025-06-20'] |

**Hallazgos:**
- last_login nulo: 139; last_login < created_at: 232
- email es PII -> enmascarar/hashear en Silver


## resources.csv

Filas: **400** · Columnas: 7 · Clave `resource_id` duplicada: 0

| column | inferred_type | nulls | null_pct | distinct | min | max | examples |
|---|---|---|---|---|---|---|---|
| resource_id | string | 0 | 0.0 | 400 |  |  | ['res_eubfn9kr', 'res_fvb66h3r', 'res_cbrlqmn4', 'res_ew1yf0dw'] |
| org_id | string | 0 | 0.0 | 80 |  |  | ['org_pnsm43d8', 'org_i7p5tb94', 'org_d14ve92m', 'org_pja1wj0t'] |
| service | string | 0 | 0.0 | 6 |  |  | ['compute', 'database', 'storage', 'networking'] |
| region | string | 0 | 0.0 | 7 |  |  | ['sa-east', 'ap-south', 'eu-central', 'us-west'] |
| created_at | date | 0 | 0.0 | 106 | 2025-05-04 | 2025-08-21 | ['2025-08-14', '2025-06-05', '2025-08-10', '2025-06-02'] |
| state | string | 0 | 0.0 | 3 |  |  | ['running', 'stopped', 'terminated'] |
| tags_json | string | 83 | 20.75 | 159 |  |  | ['["env:prod"]', '["env:prod", "pii:true"]', '["pii:true"]', '["env:prod", "costcenter:beta"]'] |

**Hallazgos:**
- tags_json nulo: 83; tags 'pii:true': 85


## support_tickets.csv

Filas: **1000** · Columnas: 8 · Clave `ticket_id` duplicada: 0

| column | inferred_type | nulls | null_pct | distinct | min | max | examples |
|---|---|---|---|---|---|---|---|
| ticket_id | string | 0 | 0.0 | 1000 |  |  | ['tkt_zjrumxqw', 'tkt_3b4nthzm', 'tkt_32hzs32d', 'tkt_1jqbexgf'] |
| org_id | string | 0 | 0.0 | 80 |  |  | ['org_x7eedtjv', 'org_gv0e38da', 'org_9mx2x18h', 'org_1swjckjl'] |
| category | string | 0 | 0.0 | 6 |  |  | ['performance', 'security', 'availability', 'usability'] |
| severity | string | 0 | 0.0 | 4 |  |  | ['low', 'critical', 'medium', 'high'] |
| created_at | date | 0 | 0.0 | 115 | 2025-05-09 | 2025-08-31 | ['2025-07-02', '2025-07-21', '2025-07-03', '2025-07-18'] |
| resolved_at | date | 240 | 24.0 | 125 | 2025-05-10 | 2025-09-19 | ['2025-07-08', '2025-07-22', '2025-07-05', '2025-07-31'] |
| csat | numeric | 254 | 25.4 | 8 | 0.0 | 7.0 | ['4.0', '3.0', '5.0', '2.0'] |
| sla_breached | boolean | 0 | 0.0 | 2 |  |  | ['False', 'True'] |

**Hallazgos:**
- csat nulo: 254; fuera de [1,5]: 40
- tickets abiertos (resolved_at nulo): 240; resolved < created: 0
- sla_breached=True: 95; critical: 56


## marketing_touches.csv

Filas: **1500** · Columnas: 7 · Clave `touch_id` duplicada: 0

| column | inferred_type | nulls | null_pct | distinct | min | max | examples |
|---|---|---|---|---|---|---|---|
| touch_id | string | 0 | 0.0 | 1500 |  |  | ['mkt_7322hr5s', 'mkt_osc0whs1', 'mkt_4eci9rtj', 'mkt_6u5a2x54'] |
| org_id | string | 0 | 0.0 | 80 |  |  | ['org_zbikcidk', 'org_5iqvnb4g', 'org_okep7y6w', 'org_ohp6vz41'] |
| campaign | string | 0 | 0.0 | 6 |  |  | ['security_week', 'webinar_finops', 'upgrade_enterprise', 'genai_launch'] |
| channel | string | 0 | 0.0 | 4 |  |  | ['ads', 'email', 'in_app', 'event'] |
| timestamp | date | 0 | 0.0 | 120 | 2025-05-04 | 2025-08-31 | ['2025-08-26', '2025-07-23', '2025-06-21', '2025-08-09'] |
| clicked | boolean | 0 | 0.0 | 2 |  |  | ['False', 'True'] |
| converted | boolean | 0 | 0.0 | 2 |  |  | ['False', 'True'] |

**Hallazgos:**
- converted=True sin clicked=True: 96


## nps_surveys.csv

Filas: **92** · Columnas: 4 · Clave `org_id+survey_date` duplicada: 0

| column | inferred_type | nulls | null_pct | distinct | min | max | examples |
|---|---|---|---|---|---|---|---|
| org_id | string | 0 | 0.0 | 60 |  |  | ['org_xaji0y6d', 'org_hv3a3zmf', 'org_8mdd4v30', 'org_t9nt3w5u'] |
| survey_date | date | 0 | 0.0 | 57 | 2025-05-24 | 2025-08-31 | ['2025-08-22', '2025-06-13', '2025-06-18', '2025-05-24'] |
| nps_score | numeric | 19 | 20.65 | 41 | -16.0 | 68.0 | ['11.0', '6.0', '15.0', '19.0'] |
| comment | string | 10 | 10.87 | 6 |  |  | ['Complex billing', 'Love genAI features', 'Missing features', 'Stable but slow'] |

**Hallazgos:**
- nps_score nulo: 19; comment nulo: 10


## billing_monthly.csv

Filas: **240** · Columnas: 8 · Clave `invoice_id` duplicada: 0

| column | inferred_type | nulls | null_pct | distinct | min | max | examples |
|---|---|---|---|---|---|---|---|
| invoice_id | string | 0 | 0.0 | 240 |  |  | ['inv_nym31sk0', 'inv_540sejok', 'inv_ilp4vn2v', 'inv_27nls4sz'] |
| org_id | string | 0 | 0.0 | 80 |  |  | ['org_xaji0y6d', 'org_pbhsahxt', 'org_hv3a3zmf', 'org_8mdd4v30'] |
| month | date | 0 | 0.0 | 3 | 2025-06-01 | 2025-08-01 | ['2025-06-01', '2025-07-01', '2025-08-01'] |
| subtotal | numeric | 0 | 0.0 | 240 | -1671.83 | 2237.39 | ['1133.34', '619.17', '540.79', '520.91'] |
| credits | numeric | 137 | 57.08 | 100 | 0.0 | 79.06 | ['25.93', '4.78', '11.43', '14.58'] |
| taxes | numeric | 0 | 0.0 | 240 | 13.18 | 469.85 | ['238.0', '130.03', '113.57', '109.39'] |
| currency | string | 0 | 0.0 | 3 |  |  | ['USD', 'ARS', 'EUR'] |
| exchange_rate_to_usd | numeric | 0 | 0.0 | 208 | 0.00133 | 1.19808 | ['0.97684', '1.00331', '0.0016', '0.00147'] |

**Hallazgos:**
- credits nulo: 137 (se asume 0)
- subtotal negativo: 13; taxes negativo: 0
- currency=ARS: 51 facturas, exchange_rate_to_usd en [0.00133, 0.00162]
- currency=EUR: 29 facturas, exchange_rate_to_usd en [0.9981, 1.19808]
- currency=USD: 160 facturas, exchange_rate_to_usd en [0.85463, 1.11791]
- orgs con más de una moneda: 50


## usage_events_stream/*.jsonl

- 43200 eventos en 120 archivos (~360.0 por archivo); 0 event_id duplicados en este corte
- 120 de 120 archivos contienen eventos de todo el rango temporal -> llegada desordenada (late data)
- value: 1309 como string, 877 nulos
- unit nulo en 2075 eventos (derivable desde metric); inconsistencias metric/unit: 0
- schema_version=1 hasta 2025-07-17 23:56:00+00:00, schema_version=2 desde 2025-07-18 00:01:00+00:00
- genai_tokens sólo en v2 y servicios ['genai'] (3132 eventos); carbon_kg en 32400 eventos v2
- cost_usd_increment negativo: 216 (< -0.01: 211); outliers |z robusto por servicio+métrica|>3.5: 312


```json
{
  "cost_stats": {
    "count": 43200.0,
    "mean": 3.4128,
    "std": 7.9236,
    "min": -154.4608,
    "50%": 1.0044,
    "90%": 9.9971,
    "99%": 16.7175,
    "99.9%": 132.2659,
    "max": 317.4308
  },
  "services": {
    "compute": 12498,
    "storage": 7614,
    "database": 7419,
    "networking": 6921,
    "analytics": 4590,
    "genai": 4158
  },
  "metrics": {
    "requests": 19512,
    "storage_gb_hours": 12996,
    "cpu_hours": 10692
  },
  "regions": {
    "ap-northeast": 7773,
    "us-east": 7566,
    "us-west": 6384,
    "sa-east": 6270,
    "ap-south": 5943,
    "eu-west": 4854,
    "eu-central": 4410
  },
  "key_sets": {
    "carbon_kg,cost_usd_increment,event_id,metric,org_id,region,resource_id,schema_version,service,timestamp,unit,value": 29268,
    "cost_usd_increment,event_id,metric,org_id,region,resource_id,schema_version,service,timestamp,unit,value": 10800,
    "carbon_kg,cost_usd_increment,event_id,genai_tokens,metric,org_id,region,resource_id,schema_version,service,timestamp,unit,value": 3132
  },
  "raw_value_types": {
    "NoneType": 877,
    "float": 41014,
    "str": 1309
  }
}
```
