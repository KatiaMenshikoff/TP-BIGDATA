#!/usr/bin/env bash
# Descomprime el dataset provisto en datalake/landing y verifica su integridad.
# Uso: scripts/setup_landing.sh [ruta/al/cloud_provider_challenge_dataset_v1.zip]
set -euo pipefail
ZIP="${1:-../cloud_provider_challenge_dataset_v1.zip}"
cd "$(dirname "$0")/.."
[ -f "$ZIP" ] || { echo "No se encontró $ZIP"; exit 1; }
[ -d datalake/landing ] && chmod -R u+w datalake/landing
unzip -q -o "$ZIP" 'datalake/landing/*' -d .
chmod -R a-w datalake/landing          # Landing es inmutable
if [ -f evidence/profile/landing_manifest.sha256 ]; then
  (cd datalake/landing && shasum -a 256 -c --quiet ../../evidence/profile/landing_manifest.sha256) \
    && echo "Landing OK: hashes coinciden con el manifest"
fi
echo "Landing lista: $(find datalake/landing -type f | wc -l | tr -d ' ') archivos"
