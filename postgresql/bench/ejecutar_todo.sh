#!/usr/bin/env bash
# Ejecuta todo el laboratorio en orden (DENTRO del contenedor):
#   docker exec -it postgres-epm bash /bench/ejecutar_todo.sh
# Requiere haber corrido antes en el host:  python generar_datos.py
set -euo pipefail
export PGUSER=epm PGDATABASE=epm_iot
run() { echo; echo "############ $1"; psql -v ON_ERROR_STOP=1 -f "/scripts/$1"; }
run 01_esquema.sql
run 02_datos_prueba.sql
run 03_consultas.sql
run 04_carga_masiva.sql
run 05_rendimiento.sql
run 06_bench_ingesta.sql
echo; echo "############ 07_concurrencia.sh"; bash /bench/07_concurrencia.sh
run 08_almacenamiento_analitica.sql
run 09_expiracion_ttl.sql
run 10_resumen.sql
