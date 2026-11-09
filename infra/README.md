# Infraestructura

| Archivo | Uso |
|---|---|
| `docker-compose.yml` | Cassandra 5.0 local (`cpa-cassandra`, puerto 9042, heap 1 GB, volumen persistente). Destino de serving por defecto (D-11) |
| `cql/01_schema.cql` | Keyspace `cpa` y tablas *query-first* `usage_by_org_day_service` (Q1) y `cost_14d_by_org_service` (Q2). Lo aplica el pipeline al conectarse (idempotente) |
| `cql/02_consultas.cql` | Consultas Q1 y Q2 + conteo de control |

```bash
docker compose -f infra/docker-compose.yml up -d          # o docker-compose ...
docker exec -i cpa-cassandra cqlsh < infra/cql/02_consultas.cql
docker compose -f infra/docker-compose.yml down -v        # apagar y borrar los datos
```

**AstraDB:** con `ASTRA_DB_SECURE_BUNDLE_PATH` y `ASTRA_DB_TOKEN` en `.env` el pipeline escribe en AstraDB con el mismo código; el keyspace se crea desde la consola de Astra (el `CREATE KEYSPACE` del script se omite).
