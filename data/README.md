# Datos

Los datos no se versionan en git. Para obtenerlos:

1. Descargar `cloud_provider_challenge_dataset_v1.zip` del campus (13 MB, 128 archivos).
2. Ejecutar `scripts/setup_landing.sh ruta/al/zip`. El script lo descomprime en `datalake/landing/`, lo deja en sólo lectura y verifica los hashes contra `evidence/profile/landing_manifest.sha256`.

Contenido: 7 CSV (maestros, tickets, marketing, NPS, facturación) y 120 archivos JSONL de eventos de uso (43.200 eventos, 03/07–31/08/2025). Diccionario en `docs/02_inventario_fuentes.md`.
