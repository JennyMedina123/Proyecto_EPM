#!/usr/bin/env bash
# =====================================================================
# 07_concurrencia.sh | EXPERIMENTOS 2 y 3 (se ejecuta DENTRO del contenedor)
#   docker exec -it postgres-epm bash /bench/07_concurrencia.sh
#
# E2  Ingesta realista: 1 lectura por transacción desde varias conexiones
#     (así llegan los eventos del broker), con y sin commit síncrono.
# E3  Interferencia OLTP vs OLAP: latencia de la consulta de alerta
#     temprana (requisito < 2 ms) sola, con ingesta concurrente y con
#     ingesta + consultas analíticas históricas en la MISMA base.
# Requiere haber ejecutado antes 01..06.
# =====================================================================
set -euo pipefail

DB="${PGDATABASE:-epm_iot}"
USR="${PGUSER:-epm}"
DUR="${DURACION:-20}"          # segundos por escenario
CLI_ING="${CLIENTES_INGESTA:-8}"
CLI_LEC="${CLIENTES_LECTURA:-4}"
CLI_ANA="${CLIENTES_ANALITICA:-2}"
DIR=/bench
TMP=$(mktemp -d)
cd "$TMP"

psql_q() { psql -U "$USR" -d "$DB" -qtAX -c "$1"; }

guardar() {  # experimento metrica valor unidad
  psql_q "INSERT INTO bench.resultados VALUES ('$1','$2',$3,'$4')
          ON CONFLICT (experimento, metrica) DO UPDATE SET valor = EXCLUDED.valor;"
}

# Percentiles de latencia a partir de los logs de pgbench (-l).
# Columna 3 del log = latencia de la transacción en microsegundos.
percentiles() {  # prefijo_log -> "n p50 p95 p99 max pct_mayor_2ms" (ms)
  cat "$1".[0-9]* | awk '{print $3}' | sort -n | awk '
    { v[NR] = $1; if ($1 > 2000) lento++ }
    END {
      n = NR
      printf "%d %.3f %.3f %.3f %.3f %.1f\n", n,
        v[int(n*0.50)]/1000, v[int(n*0.95)]/1000, v[int(n*0.99)]/1000,
        v[n]/1000, 100*lento/n
    }'
}

tps() { grep -E "^tps" "$1" | head -1 | awk '{printf "%.0f", $3}'; }

linea() { printf '%-48s %8s %9s %9s %9s %9s %8s\n' "$@"; }

echo
echo "=== E2. INGESTA REALISTA: 1 fila por transacción, $CLI_ING conexiones, ${DUR}s ==="
pgbench -U "$USR" -d "$DB" -n -f $DIR/ingesta_fila.sql -c "$CLI_ING" -j 2 -T "$DUR" \
        -l --log-prefix=ing_sync > ing_sync.txt 2>&1
TPS_SYNC=$(tps ing_sync.txt); read -r N P50 P95 P99 MAX LENTO <<< "$(percentiles ing_sync)"
echo "  synchronous_commit=on : $TPS_SYNC filas/s | p50 ${P50} ms | p99 ${P99} ms | >2 ms: ${LENTO}%"
guardar 'E2 Ingesta fila a fila' 'filas/s commit síncrono' "$TPS_SYNC" 'filas/s'
guardar 'E2 Ingesta fila a fila' 'p99 commit síncrono' "$P99" 'ms'

PGOPTIONS='-c synchronous_commit=off' \
pgbench -U "$USR" -d "$DB" -n -f $DIR/ingesta_fila.sql -c "$CLI_ING" -j 2 -T "$DUR" \
        -l --log-prefix=ing_async > ing_async.txt 2>&1
TPS_ASYNC=$(tps ing_async.txt); read -r N P50 P95 P99 MAX LENTO <<< "$(percentiles ing_async)"
echo "  synchronous_commit=off: $TPS_ASYNC filas/s | p50 ${P50} ms | p99 ${P99} ms | >2 ms: ${LENTO}%"
echo "  Requerido por EPM     : 85000 filas/s"
guardar 'E2 Ingesta fila a fila' 'filas/s commit asíncrono' "$TPS_ASYNC" 'filas/s'
guardar 'E2 Ingesta fila a fila' 'pct de lo requerido (síncrono)' "$(awk "BEGIN{printf \"%.1f\", 100*$TPS_SYNC/85000}")" '%'

echo
echo "=== E3. INTERFERENCIA: consulta de alerta (última lectura), $CLI_LEC conexiones, ${DUR}s ==="
linea "Escenario" "consultas" "p50 ms" "p95 ms" "p99 ms" "max ms" ">2ms %"

# E3a: sola
pgbench -U "$USR" -d "$DB" -n -f $DIR/lectura_puntual.sql -c "$CLI_LEC" -j 2 -T "$DUR" \
        -l --log-prefix=lec_sola > lec_sola.txt 2>&1
read -r N P50 P95 P99 MAX LENTO <<< "$(percentiles lec_sola)"
linea "a) Lectura sola" "$N" "$P50" "$P95" "$P99" "$MAX" "$LENTO"
guardar 'E3 Interferencia' 'p99 lectura sola' "$P99" 'ms'
guardar 'E3 Interferencia' '% >2ms lectura sola' "$LENTO" '%'

# E3b: + ingesta concurrente
pgbench -U "$USR" -d "$DB" -n -f $DIR/ingesta_fila.sql -c "$CLI_ING" -j 2 -T $((DUR + 4)) > /dev/null 2>&1 &
PID_ING=$!
sleep 2
pgbench -U "$USR" -d "$DB" -n -f $DIR/lectura_puntual.sql -c "$CLI_LEC" -j 2 -T "$DUR" \
        -l --log-prefix=lec_ing > lec_ing.txt 2>&1
wait $PID_ING
read -r N P50 P95 P99 MAX LENTO <<< "$(percentiles lec_ing)"
linea "b) Lectura + ingesta" "$N" "$P50" "$P95" "$P99" "$MAX" "$LENTO"
guardar 'E3 Interferencia' 'p99 lectura + ingesta' "$P99" 'ms'
guardar 'E3 Interferencia' '% >2ms lectura + ingesta' "$LENTO" '%'

# E3c: + ingesta + analítica histórica
pgbench -U "$USR" -d "$DB" -n -f $DIR/ingesta_fila.sql -c "$CLI_ING" -j 2 -T $((DUR + 4)) > /dev/null 2>&1 &
PID_ING=$!
pgbench -U "$USR" -d "$DB" -n -f $DIR/analitica.sql -c "$CLI_ANA" -j 1 -T $((DUR + 4)) \
        -l --log-prefix=ana > ana.txt 2>&1 &
PID_ANA=$!
sleep 2
pgbench -U "$USR" -d "$DB" -n -f $DIR/lectura_puntual.sql -c "$CLI_LEC" -j 2 -T "$DUR" \
        -l --log-prefix=lec_mix > lec_mix.txt 2>&1
wait $PID_ING $PID_ANA
read -r N P50 P95 P99 MAX LENTO <<< "$(percentiles lec_mix)"
linea "c) Lectura + ingesta + analítica" "$N" "$P50" "$P95" "$P99" "$MAX" "$LENTO"
guardar 'E3 Interferencia' 'p99 lectura + ingesta + analítica' "$P99" 'ms'
guardar 'E3 Interferencia' '% >2ms lectura + ingesta + analítica' "$LENTO" '%'

read -r N P50 P95 P99 MAX LENTO <<< "$(percentiles ana)"
echo
echo "  Consulta analítica histórica: $N ejecuciones, mediana ${P50} ms"
guardar 'E3 Interferencia' 'mediana consulta analítica (fila)' "$P50" 'ms'

echo
echo "Requisito EPM: alerta temprana < 2 ms. Resultados guardados en bench.resultados"
rm -rf "$TMP"
