-- =====================================================================
-- 08_almacenamiento_analitica.sql | EXPERIMENTO 4: almacenamiento por
-- filas vs. columnar (retención de 7 años CREG + analítica / IA)
--
-- PostgreSQL guarda FILAS completas en páginas de 8 kB, sin compresión
-- para filas pequeñas. Un motor columnar (ClickHouse, Parquet) guarda
-- cada columna por separado y comprimida (Delta + LZ4/ZSTD).
-- Requiere haber ejecutado 06 (2.550.000 filas por partición de prueba).
-- =====================================================================
SET search_path TO epm_iot, public;
\timing on

CREATE OR REPLACE FUNCTION bench.explain_json(q TEXT) RETURNS JSON
LANGUAGE plpgsql AS $$
DECLARE r JSON;
BEGIN
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' || q INTO r;
    RETURN r;
END $$;

ANALYZE lecturas_sensor;

-- ---------------------------------------------------------------------
-- 4.1 Costo real de almacenamiento por lectura (partición de 2,55 M filas)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bench.tamano;
CREATE TABLE bench.tamano AS
SELECT (SELECT COUNT(*) FROM lecturas_sensor_20260910)                   AS filas,
       pg_relation_size('lecturas_sensor_20260910')                      AS bytes_tabla,
       pg_indexes_size('lecturas_sensor_20260910')                       AS bytes_indice,
       pg_total_relation_size('lecturas_sensor_20260910')                AS bytes_total;

SELECT filas,
       pg_size_pretty(bytes_tabla)                    AS tabla,
       pg_size_pretty(bytes_indice)                   AS indice_pk,
       ROUND(bytes_total::numeric / filas, 1)         AS bytes_por_fila,
       36                                             AS bytes_utiles_por_fila,
       ROUND(bytes_total::numeric / filas / 36, 1)    AS factor_sobrecosto
FROM bench.tamano;
-- bytes útiles = uuid 16 + timestamptz 8 + 3 x real 4.
-- El resto es cabecera de tupla (23 B + alineación), puntero (4 B),
-- espacio libre de página y el índice B-tree.

-- ---------------------------------------------------------------------
-- 4.2 Amplificación de lectura en analítica: promedio de UNA columna
--     En almacenamiento por filas hay que leer todas las columnas.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bench.lectura_analitica;
CREATE TABLE bench.lectura_analitica AS
WITH e AS (
    SELECT bench.explain_json('SELECT AVG(vibracion) FROM epm_iot.lecturas_sensor') AS j
)
SELECT (SELECT COUNT(*) FROM lecturas_sensor)                                AS filas,
       ((j->0->'Plan'->>'Shared Hit Blocks')::bigint
        + (j->0->'Plan'->>'Shared Read Blocks')::bigint)                     AS paginas_leidas,
       (j->0->>'Execution Time')::numeric                                    AS ms
FROM e;

SELECT filas,
       paginas_leidas,
       pg_size_pretty(paginas_leidas * 8192)                         AS bytes_leidos,
       pg_size_pretty(filas * 4)                                     AS bytes_de_vibracion,
       ROUND(paginas_leidas * 8192.0 / (filas * 4), 1)               AS amplificacion_lectura,
       pg_size_pretty(filas * 4 / 10)                                AS columnar_estimado_10a1,
       ROUND(ms, 1)                                                  AS ms_postgres
FROM bench.lectura_analitica;
-- amplificacion_lectura: cuántos bytes lee PostgreSQL por cada byte que
-- realmente necesita. Un motor columnar lee sólo la columna vibracion,
-- y comprimida (~10:1 en series de tiempo, según el informe).

-- ---------------------------------------------------------------------
-- 4.3 Proyección a la escala de EPM con el tamaño MEDIDO
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bench.proyeccion;
CREATE TABLE bench.proyeccion AS
WITH p AS (
    SELECT bytes_total::numeric / filas  AS bpf_pg,
           bytes_indice::numeric / filas AS bpf_idx
    FROM bench.tamano
), v AS (
    SELECT 85000::numeric * 86400 AS filas_dia
)
SELECT capa, dias, bpf_pg,
       ROUND(filas_dia * dias * bpf_pg / 1e12, 2)            AS tb_postgres,
       ROUND(filas_dia * dias * 50 / 1e12, 2)                AS tb_cassandra_informe,
       ROUND(filas_dia * dias * 50 / 10 / 1e12, 2)           AS tb_columnar_10a1,
       ROUND(filas_dia * bpf_idx / 1e9, 1)                   AS gb_indice_pk_por_dia
FROM p, v,
     (VALUES ('HOT 24 h', 1::numeric), ('WARM 30 días', 30), ('COLD 7 años', 2556.75)) c(capa, dias);

SELECT capa, tb_postgres, tb_cassandra_informe, tb_columnar_10a1,
       ROUND(bpf_pg / 5, 1) AS veces_mas_que_columnar   -- 50 B / 10 = 5 B por lectura
FROM bench.proyeccion;

SELECT gb_indice_pk_por_dia AS gb_indice_pk_de_un_dia,
       'Debe caber en RAM para que las lecturas puntuales sean < 2 ms' AS nota
FROM bench.proyeccion WHERE capa = 'HOT 24 h';

-- Guardar resultados
INSERT INTO bench.resultados
SELECT 'E4 Almacenamiento', 'bytes por fila (tabla + PK)', ROUND(bytes_total::numeric / filas, 1), 'bytes' FROM bench.tamano
UNION ALL
SELECT 'E4 Almacenamiento', 'amplificación de lectura AVG(vibracion)',
       ROUND(paginas_leidas * 8192.0 / (filas * 4), 1), 'x' FROM bench.lectura_analitica
UNION ALL
SELECT 'E4 Almacenamiento', 'ms AVG(vibracion) todo el histórico', ROUND(ms, 1), 'ms' FROM bench.lectura_analitica
UNION ALL
SELECT 'E4 Almacenamiento', 'TB 7 años PostgreSQL', tb_postgres, 'TB' FROM bench.proyeccion WHERE capa = 'COLD 7 años'
UNION ALL
SELECT 'E4 Almacenamiento', 'TB 7 años columnar 10:1', tb_columnar_10a1, 'TB' FROM bench.proyeccion WHERE capa = 'COLD 7 años'
ON CONFLICT (experimento, metrica) DO UPDATE SET valor = EXCLUDED.valor;
