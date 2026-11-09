#!/usr/bin/env bash
# Limpieza / reinicio controlado: borra las zonas derivadas, checkpoints y metadatos (Landing NO se toca)
# y vacía las tablas de Cassandra. Bronze y _checkpoints se borran juntos: borrar sólo el checkpoint
# haría que el stream vuelva a agregar los 120 archivos a Bronze (append).
# Uso: scripts/reset.sh [--keep-cassandra] [lake_root]
set -euo pipefail
cd "$(dirname "$0")/.."
KEEP_CASSANDRA=0
if [ "${1:-}" = "--keep-cassandra" ]; then KEEP_CASSANDRA=1; shift; fi
LAKE="${1:-datalake}"
for z in bronze silver gold quarantine _checkpoints _metadata; do rm -rf "${LAKE:?}/$z"; done
echo "Lake reiniciado: $LAKE (landing intacta: $(find "$LAKE/landing" -type f 2>/dev/null | wc -l | tr -d ' ') archivos)"
if [ "$KEEP_CASSANDRA" = 0 ] && docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^cpa-cassandra$'; then
  docker exec cpa-cassandra cqlsh -e "TRUNCATE cpa.usage_by_org_day_service; TRUNCATE cpa.cost_14d_by_org_service;" 2>/dev/null \
    && echo "Cassandra: tablas de cpa vaciadas" || echo "Cassandra: keyspace cpa todavía no existe"
fi
