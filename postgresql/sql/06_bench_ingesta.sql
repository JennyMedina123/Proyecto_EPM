-- =====================================================================
-- 06_bench_ingesta.sql | EXPERIMENTO 1: ¿Aguanta 85.000 eventos/segundo?
--
-- Restricción EPM: 85.000 sensores x 1 lectura/s = 85.000 filas/s
-- sostenidas, 24x7. Si insertar 1 "segundo" de telemetría tarda más de
-- 1.000 ms, la base se atrasa y la cola crece sin límite.
--
-- Escenario A: tabla con PK + FK (modelo relacional normal)
-- Escenario B: A + 3 índices secundarios (los que pediría la analítica)
-- Ambos son el MEJOR caso para PostgreSQL: un solo INSERT masivo por
-- segundo desde una sola sesión. El caso realista (una fila por
-- transacción desde muchas conexiones) se mide con bench/07_*.sh
-- =====================================================================
SET search_path TO epm_iot, public;
\timing on

-- ---------------------------------------------------------------------
-- Esquema de resultados (lo usan todos los experimentos)
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bench;
CREATE TABLE IF NOT EXISTS bench.resultados (
    experimento  TEXT,
    metrica      TEXT,
    valor        NUMERIC,
    unidad       TEXT,
    PRIMARY KEY (experimento, metrica)
);
DROP TABLE IF EXISTS bench.ingesta;
CREATE TABLE bench.ingesta (
    escenario  TEXT,
    segundo    INT,
    filas      INT,
    ms         NUMERIC,
    wal_bytes  BIGINT
);

-- ---------------------------------------------------------------------
-- 85.000 sensores reales (repartidos en las 6 turbinas)
-- ---------------------------------------------------------------------
INSERT INTO sensor (sensor_id, turbina_id, codigo)
SELECT ('c0000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid,
       ('a0000000-0000-0000-0000-00000000000' || (1 + n % 6))::uuid,
       'IOT-' || lpad(n::text, 5, '0')
FROM generate_series(1, 85000) n
ON CONFLICT DO NOTHING;

SELECT COUNT(*) AS sensores_iot FROM sensor WHERE codigo LIKE 'IOT-%';

-- Particiones para los días de la prueba
SELECT crear_particiones_diarias('2026-09-10', '2026-09-11');

-- ---------------------------------------------------------------------
-- Procedimiento: simula N segundos de telemetría. Cada segundo inserta
-- una lectura por sensor (85.000 filas), hace COMMIT y registra tiempo y
-- WAL generado. Los COMMIT dentro del procedimiento son válidos en PG 11+.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bench.simular_ingesta(
    p_escenario TEXT, p_segundos INT, p_inicio TIMESTAMPTZ)
LANGUAGE plpgsql AS $$
DECLARE
    t0   TIMESTAMPTZ;
    lsn0 PG_LSN;
    n    INT;
BEGIN
    FOR s IN 0 .. p_segundos - 1 LOOP
        t0   := clock_timestamp();
        lsn0 := pg_current_wal_insert_lsn();

        INSERT INTO epm_iot.lecturas_sensor (sensor_id, ts, temperatura, presion, vibracion)
        SELECT sensor_id,
               p_inicio + s * INTERVAL '1 second',
               55 + random() * 10,
               11 + random() * 2,
               random() * 6
        FROM epm_iot.sensor
        WHERE codigo LIKE 'IOT-%';
        GET DIAGNOSTICS n = ROW_COUNT;
        COMMIT;

        INSERT INTO bench.ingesta
        VALUES (p_escenario, s + 1, n,
                round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 1),
                pg_current_wal_insert_lsn() - lsn0);
        COMMIT;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------
-- Escenario A: PK + FK  (30 segundos simulados = 2.550.000 filas)
-- ---------------------------------------------------------------------
CHECKPOINT;
CALL bench.simular_ingesta('A: PK + FK', 30, '2026-09-10 00:00:00-05');

-- ---------------------------------------------------------------------
-- Escenario B: + 3 índices secundarios para consultas analíticas
-- ---------------------------------------------------------------------
CREATE INDEX idx_lect_vibracion   ON lecturas_sensor (vibracion);
CREATE INDEX idx_lect_temperatura ON lecturas_sensor (temperatura);
CREATE INDEX idx_lect_ts          ON lecturas_sensor (ts);
CHECKPOINT;
CALL bench.simular_ingesta('B: PK + FK + 3 índices', 30, '2026-09-11 00:00:00-05');
DROP INDEX idx_lect_vibracion, idx_lect_temperatura, idx_lect_ts;
ANALYZE lecturas_sensor;

-- ---------------------------------------------------------------------
-- Resultados
-- ---------------------------------------------------------------------
-- Evolución segundo a segundo (¿se mantiene por debajo de 1.000 ms?)
SELECT escenario, segundo, ms,
       CASE WHEN ms > 1000 THEN 'ATRASADO' ELSE 'ok' END AS estado
FROM bench.ingesta ORDER BY escenario, segundo;

-- Resumen por escenario
SELECT escenario,
       SUM(filas)                                        AS filas,
       ROUND(AVG(ms), 1)                                 AS ms_promedio_por_segundo,
       MAX(ms)                                           AS ms_peor_segundo,
       COUNT(*) FILTER (WHERE ms > 1000)                 AS segundos_atrasados,
       ROUND(GREATEST(SUM(ms) - COUNT(*) * 1000, 0) / 1000.0, 1) AS atraso_acumulado_s,
       ROUND(SUM(filas) / (SUM(ms) / 1000.0))            AS filas_por_segundo,
       ROUND(SUM(filas) / (SUM(ms) / 1000.0) / 85000 * 100, 1) AS pct_capacidad_requerida,
       ROUND(SUM(wal_bytes)::numeric / SUM(filas), 1)    AS wal_bytes_por_fila,
       ROUND(SUM(wal_bytes)::numeric / SUM(filas) / 36, 1) AS amplificacion_escritura
FROM bench.ingesta
GROUP BY escenario ORDER BY escenario;
-- amplificacion_escritura = bytes WAL / 36 bytes útiles por lectura
-- (uuid 16 + ts 8 + 3 x real 4). El WAL se escribe ANTES que la tabla,
-- así que el disco recibe además los datos y el índice: esto son IOPS.

-- Guardar en la tabla de resultados
INSERT INTO bench.resultados
SELECT 'E1 Ingesta masiva', 'filas/s ' || escenario,
       ROUND(SUM(filas) / (SUM(ms) / 1000.0)), 'filas/s'
FROM bench.ingesta GROUP BY escenario
UNION ALL
SELECT 'E1 Ingesta masiva', 'WAL bytes/fila ' || escenario,
       ROUND(SUM(wal_bytes)::numeric / SUM(filas), 1), 'bytes'
FROM bench.ingesta GROUP BY escenario
ON CONFLICT (experimento, metrica) DO UPDATE SET valor = EXCLUDED.valor;
