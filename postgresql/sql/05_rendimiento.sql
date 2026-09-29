-- =====================================================================
-- 05_rendimiento.sql | Reto video: tiempos, consulta sin clave, particiones
-- (equivalente a TRACING ON de cqlsh -> EXPLAIN ANALYZE)
-- =====================================================================
SET search_path TO epm_iot;
\timing on

-- ---------------------------------------------------------------------
-- 1. Medir el tiempo de respuesta de la consulta "última lectura"
--    Observar en el plan:
--      * Sólo aparece UNA partición (lecturas_sensor_20260903) -> pruning
--      * "Index Scan Backward" sobre la PK -> orden DESC sin ordenar
--      * "Execution Time" en milisegundos
-- ---------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT ts, temperatura, presion, vibracion
FROM lecturas_sensor
WHERE sensor_id = 'b0000000-0000-0000-0000-000000000001'
  AND ts >= '2026-09-03' AND ts < '2026-09-04'
ORDER BY ts DESC
LIMIT 1;

-- ---------------------------------------------------------------------
-- 2. Consulta que NO respeta la clave (rango de vibración sin sensor)
--    Cassandra la rechaza y exige ALLOW FILTERING.
--    PostgreSQL SÍ la ejecuta, pero debe leer TODAS las particiones
--    (Seq Scan en cada una). Comparar "Execution Time" y "Buffers".
-- ---------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT sensor_id, ts, vibracion
FROM lecturas_sensor
WHERE vibracion BETWEEN 7 AND 10;

SELECT COUNT(*) AS lecturas_con_vibracion_alta
FROM lecturas_sensor
WHERE vibracion BETWEEN 7 AND 10;

-- 2b. Ventaja relacional: se puede crear un índice secundario sobre
--     cualquier columna y el mismo filtro deja de recorrer toda la tabla.
CREATE INDEX idx_lecturas_vibracion ON lecturas_sensor (vibracion);
ANALYZE lecturas_sensor;

EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT sensor_id, ts, vibracion
FROM lecturas_sensor
WHERE vibracion BETWEEN 7 AND 10;

-- (Costo: cada índice extra encarece las 85.000 escrituras/segundo.)
DROP INDEX idx_lecturas_vibracion;

-- ---------------------------------------------------------------------
-- 3. ¿Cuántas particiones se generan?
-- ---------------------------------------------------------------------
-- 3a. Particiones físicas (tablas hijas) existentes
SELECT COUNT(*) AS particiones_fisicas
FROM pg_inherits
WHERE inhparent = 'lecturas_sensor'::regclass;

-- 3b. "Particiones lógicas" estilo Cassandra (sensor, día) en los datos cargados
SELECT COUNT(*) AS grupos_sensor_dia
FROM (SELECT DISTINCT sensor_id, (ts AT TIME ZONE 'America/Bogota')::date
      FROM lecturas_sensor) x;
-- Esperado: 101 (20 sensores x 5 días + 1 del sensor de prueba)

-- 3c. Proyección a un año completo con 85.000 sensores
SELECT 365                         AS particiones_fisicas_postgres_anio,
       85000 * 365                 AS grupos_sensor_dia_anio,   -- = particiones Cassandra
       85000::bigint * 86400       AS filas_por_particion_diaria;

-- 3d. Tamaño real por fila medido en una partición llena (tabla + índice PK)
SELECT pg_size_pretty(pg_total_relation_size('lecturas_sensor_20260903')) AS tamano_particion,
       (SELECT COUNT(*) FROM lecturas_sensor_20260903)                   AS filas,
       ROUND(pg_total_relation_size('lecturas_sensor_20260903')::numeric
             / (SELECT COUNT(*) FROM lecturas_sensor_20260903), 1)       AS bytes_por_fila;
