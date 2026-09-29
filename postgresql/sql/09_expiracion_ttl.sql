-- =====================================================================
-- 09_expiracion_ttl.sql | EXPERIMENTO 5: expiración de la capa HOT (24 h)
--
-- Cassandra: TTL por registro, el dato vence solo y se descarta en la
-- compactación sin generar tombstones de DELETE.
-- PostgreSQL no tiene TTL. Se comparan, con 2.550.000 filas cada una:
--   A) DELETE de un día  -> tuplas muertas (análogo a tombstones) + VACUUM
--   B) DROP de la partición del día -> operación de metadatos
-- Requiere haber ejecutado 06. Ejecutar AL FINAL: borra datos.
-- =====================================================================
SET search_path TO epm_iot, public;
\timing on

DROP TABLE IF EXISTS bench.expiracion;
CREATE TABLE bench.expiracion (paso TEXT, ms NUMERIC, wal_bytes BIGINT, filas_vivas BIGINT,
                               tuplas_muertas BIGINT, tamano TEXT);

CREATE OR REPLACE PROCEDURE bench.medir(p_paso TEXT, p_sql TEXT, p_particion TEXT)
LANGUAGE plpgsql AS $$
DECLARE
    t0 TIMESTAMPTZ; l0 PG_LSN; st RECORD;
BEGIN
    t0 := clock_timestamp(); l0 := pg_current_wal_insert_lsn();
    IF p_sql IS NOT NULL THEN EXECUTE p_sql; END IF;
    INSERT INTO bench.expiracion (paso, ms, wal_bytes)
    VALUES (p_paso,
            round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1),
            pg_current_wal_insert_lsn() - l0);
    IF to_regclass(p_particion) IS NOT NULL THEN
        SELECT * INTO st FROM pgstattuple(p_particion);
        UPDATE bench.expiracion
           SET filas_vivas = st.tuple_count, tuplas_muertas = st.dead_tuple_count,
               tamano = pg_size_pretty(pg_total_relation_size(p_particion))
         WHERE paso = p_paso;
    ELSE
        UPDATE bench.expiracion SET filas_vivas = 0, tuplas_muertas = 0,
               tamano = 'eliminada' WHERE paso = p_paso;
    END IF;
END $$;

-- ---------------------------------------------------------------------
-- A) Enfoque tradicional: DELETE del día 2026-09-10
-- ---------------------------------------------------------------------
-- Se desactiva autovacuum en esta partición para observar el efecto aislado
ALTER TABLE lecturas_sensor_20260910 SET (autovacuum_enabled = false);
CALL bench.medir('A0 estado inicial', NULL, 'lecturas_sensor_20260910');
CALL bench.medir('A1 DELETE del día',
     $q$DELETE FROM epm_iot.lecturas_sensor
        WHERE ts >= '2026-09-10 00:00-05' AND ts < '2026-09-11 00:00-05'$q$,
     'lecturas_sensor_20260910');
-- Leer la partición "vacía" todavía recorre todas las tuplas muertas
CALL bench.medir('A2 SELECT sobre tuplas muertas',
     'SELECT COUNT(*) FROM epm_iot.lecturas_sensor_20260910', 'lecturas_sensor_20260910');
\echo 'Ejecutando VACUUM (no puede ir dentro de un procedimiento)...'
SELECT clock_timestamp() AS t0, pg_current_wal_insert_lsn() AS l0 \gset
VACUUM lecturas_sensor_20260910;
INSERT INTO bench.expiracion
SELECT 'A3 VACUUM',
       round(extract(epoch FROM clock_timestamp() - :'t0'::timestamptz)::numeric * 1000, 1),
       pg_current_wal_insert_lsn() - :'l0'::pg_lsn,
       s.tuple_count, s.dead_tuple_count,
       pg_size_pretty(pg_total_relation_size('lecturas_sensor_20260910'))
FROM pgstattuple('lecturas_sensor_20260910') s;
CALL bench.medir('A4 SELECT después de VACUUM',
     'SELECT COUNT(*) FROM epm_iot.lecturas_sensor_20260910', 'lecturas_sensor_20260910');

-- ---------------------------------------------------------------------
-- B) Enfoque recomendado: eliminar la partición vencida (2026-09-11)
-- ---------------------------------------------------------------------
CALL bench.medir('B0 estado inicial', NULL, 'lecturas_sensor_20260911');
CALL bench.medir('B1 DETACH + DROP de la partición',
     'ALTER TABLE epm_iot.lecturas_sensor DETACH PARTITION epm_iot.lecturas_sensor_20260911;
      DROP TABLE epm_iot.lecturas_sensor_20260911;',
     'lecturas_sensor_20260911');

SELECT paso, ms, pg_size_pretty(wal_bytes) AS wal, filas_vivas, tuplas_muertas, tamano
FROM bench.expiracion ORDER BY paso;

INSERT INTO bench.resultados
SELECT 'E5 Expiración', 'ms ' || paso, ms, 'ms' FROM bench.expiracion
WHERE paso IN ('A1 DELETE del día', 'A2 SELECT sobre tuplas muertas', 'A3 VACUUM',
               'B1 DETACH + DROP de la partición')
UNION ALL
SELECT 'E5 Expiración', 'WAL ' || paso, wal_bytes, 'bytes' FROM bench.expiracion
WHERE paso IN ('A1 DELETE del día', 'B1 DETACH + DROP de la partición')
ON CONFLICT (experimento, metrica) DO UPDATE SET valor = EXCLUDED.valor;

-- ---------------------------------------------------------------------
-- Automatización: función purgar_particiones (script 01). Simula que
-- "ahora" es 2026-09-04 00:00 con retención de 24 h -> elimina 01 y 02.
-- ---------------------------------------------------------------------
SELECT * FROM purgar_particiones(INTERVAL '24 hours', TIMESTAMPTZ '2026-09-04 00:00:00-05')
         AS particion_eliminada;

-- Programación diaria (cron del host, o pg_cron / pg_partman):
--   docker exec postgres-epm psql -U epm -d epm_iot -c
--     "SELECT epm_iot.crear_particiones_diarias(current_date, current_date + 7);
--      SELECT epm_iot.purgar_particiones(INTERVAL '24 hours');"
