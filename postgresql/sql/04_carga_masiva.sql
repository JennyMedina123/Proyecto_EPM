-- =====================================================================
-- 04_carga_masiva.sql | Carga de las 100.000 lecturas simuladas
-- Flujo: Python -> CSV -> Docker (/data) -> PostgreSQL (COPY)
-- =====================================================================
SET search_path TO epm_iot;
\timing on

COPY sensor (sensor_id, turbina_id, codigo)
FROM '/data/sensores.csv' WITH (FORMAT csv, HEADER true);

COPY lecturas_sensor (sensor_id, ts, temperatura, presion, vibracion)
FROM '/data/lecturas.csv' WITH (FORMAT csv, HEADER true);

-- Actualizar estadísticas para el optimizador
ANALYZE lecturas_sensor;

-- 1) Total cargado (100.000 simuladas + 4 de prueba)
SELECT COUNT(*) AS total_lecturas FROM lecturas_sensor;

-- 2) Filas por partición física (1 partición = 1 día = 20 sensores x 1.000)
SELECT tableoid::regclass AS particion, COUNT(*) AS filas
FROM lecturas_sensor
GROUP BY 1 ORDER BY 1;

-- 3) Verificar una "partición lógica" estilo Cassandra: 1 sensor + 1 día
SELECT COUNT(*) AS lecturas_sensor_dia
FROM lecturas_sensor
WHERE sensor_id = 'b0000000-0000-0000-0000-000000000001'
  AND ts >= '2026-09-03' AND ts < '2026-09-04';
-- Esperado: 1000

-- 4) Última lectura de ese sensor ese día
SELECT ts, temperatura, presion, vibracion
FROM lecturas_sensor
WHERE sensor_id = 'b0000000-0000-0000-0000-000000000001'
  AND ts >= '2026-09-03' AND ts < '2026-09-04'
ORDER BY ts DESC
LIMIT 1;
