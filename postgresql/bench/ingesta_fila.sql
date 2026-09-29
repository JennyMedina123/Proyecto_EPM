-- Una lectura por transacción (así llegan los eventos desde el broker)
\set s random(1, 85000)
INSERT INTO epm_iot.lecturas_sensor (sensor_id, ts, temperatura, presion, vibracion)
VALUES (('c0000000-0000-0000-0000-' || lpad(:s::text, 12, '0'))::uuid,
        clock_timestamp(), 55 + random() * 10, 11 + random() * 2, random() * 6)
ON CONFLICT DO NOTHING;
