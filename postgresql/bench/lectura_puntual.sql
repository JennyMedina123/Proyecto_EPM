-- Alerta temprana: última lectura de un sensor (consulta operacional)
\set s random(1, 85000)
SELECT ts, vibracion
FROM epm_iot.lecturas_sensor
WHERE sensor_id = ('c0000000-0000-0000-0000-' || lpad(:s::text, 12, '0'))::uuid
  AND ts >= '2026-09-10 00:00:00-05' AND ts < '2026-09-11 00:00:00-05'
ORDER BY ts DESC
LIMIT 1;
