-- =====================================================================
-- 03_consultas.sql | Las 3 consultas que debe resolver el modelo
-- =====================================================================
SET search_path TO epm_iot;
\timing on

-- ---------------------------------------------------------------------
-- 2.1 Última lectura registrada de un sensor
--   Cassandra: WHERE sensor_id = ? AND date = ? LIMIT 1
--   PostgreSQL: el filtro sobre ts permite "partition pruning" (sólo se
--   lee la partición del día) y el índice PK (sensor_id, ts) se recorre
--   hacia atrás, por lo que LIMIT 1 devuelve la más reciente.
-- ---------------------------------------------------------------------
SELECT sensor_id, ts, temperatura, presion, vibracion
FROM lecturas_sensor
WHERE sensor_id = 'b0000000-0000-0000-0000-000000000099'
  AND ts >= '2026-09-28' AND ts < '2026-09-29'
ORDER BY ts DESC
LIMIT 1;
-- Esperado: 14:10:00, vibración 3.8

-- ---------------------------------------------------------------------
-- 2.2 Lecturas de un sensor durante la última hora
--   (se fija la hora de referencia 14:10 para que el ejercicio sea
--    reproducible; en producción se usa now())
-- ---------------------------------------------------------------------
SELECT ts, temperatura, presion, vibracion
FROM lecturas_sensor
WHERE sensor_id = 'b0000000-0000-0000-0000-000000000099'
  AND ts >= TIMESTAMPTZ '2026-09-28 14:10:00-05' - INTERVAL '1 hour'
  AND ts <= TIMESTAMPTZ '2026-09-28 14:10:00-05'
ORDER BY ts DESC;
-- Esperado: 3 filas (14:10, 14:05, 14:00)

-- Versión producción:
-- WHERE sensor_id = $1 AND ts >= now() - INTERVAL '1 hour'

-- ---------------------------------------------------------------------
-- 2.3 Promedio de vibración POR TURBINA durante la última hora
--   En Cassandra sólo se podía promediar un sensor (partition key).
--   En el modelo relacional el JOIN con sensor/turbina/central permite
--   agregar TODOS los sensores de la turbina en una sola consulta.
-- ---------------------------------------------------------------------
SELECT c.nombre              AS central,
       t.codigo              AS turbina,
       COUNT(*)              AS lecturas,
       ROUND(AVG(l.vibracion)::numeric, 2) AS vibracion_promedio
FROM lecturas_sensor l
JOIN sensor  s ON s.sensor_id  = l.sensor_id
JOIN turbina t ON t.turbina_id = s.turbina_id
JOIN central c ON c.central_id = t.central_id
WHERE t.turbina_id = 'a0000000-0000-0000-0000-000000000001'
  AND l.ts >= TIMESTAMPTZ '2026-09-28 14:10:00-05' - INTERVAL '1 hour'
  AND l.ts <= TIMESTAMPTZ '2026-09-28 14:10:00-05'
GROUP BY c.nombre, t.codigo;
-- Esperado: 3 lecturas, promedio 3.50
