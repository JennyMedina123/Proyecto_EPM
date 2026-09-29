-- Consulta analítica/histórica: promedio de vibración por turbina en todo el histórico
SELECT t.codigo, AVG(l.vibracion), MAX(l.vibracion), COUNT(*)
FROM epm_iot.lecturas_sensor l
JOIN epm_iot.sensor  s ON s.sensor_id  = l.sensor_id
JOIN epm_iot.turbina t ON t.turbina_id = s.turbina_id
GROUP BY t.codigo;
