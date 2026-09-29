-- =====================================================================
-- 02_datos_prueba.sql | Catálogo (centrales, turbinas) + datos de prueba
-- =====================================================================
SET search_path TO epm_iot;

INSERT INTO central (central_id, nombre) VALUES
  (1, 'Hidroituango'),
  (2, 'Guatapé'),
  (3, 'Porce III');

INSERT INTO turbina (turbina_id, central_id, codigo) VALUES
  ('a0000000-0000-0000-0000-000000000001', 1, 'HIT-T1'),
  ('a0000000-0000-0000-0000-000000000002', 1, 'HIT-T2'),
  ('a0000000-0000-0000-0000-000000000003', 2, 'GUA-T1'),
  ('a0000000-0000-0000-0000-000000000004', 2, 'GUA-T2'),
  ('a0000000-0000-0000-0000-000000000005', 3, 'POR-T1'),
  ('a0000000-0000-0000-0000-000000000006', 3, 'POR-T2');

-- Sensor de prueba (equivalente a los INSERT manuales hechos en cqlsh)
INSERT INTO sensor (sensor_id, turbina_id, codigo) VALUES
  ('b0000000-0000-0000-0000-000000000099', 'a0000000-0000-0000-0000-000000000001', 'SEN-PRUEBA');

INSERT INTO lecturas_sensor (sensor_id, ts, temperatura, presion, vibracion) VALUES
  ('b0000000-0000-0000-0000-000000000099', '2026-09-28 14:00:00-05', 61.2, 12.1, 3.2),
  ('b0000000-0000-0000-0000-000000000099', '2026-09-28 14:05:00-05', 61.8, 12.3, 3.5),
  ('b0000000-0000-0000-0000-000000000099', '2026-09-28 14:10:00-05', 62.4, 12.2, 3.8),
  ('b0000000-0000-0000-0000-000000000099', '2026-09-28 12:30:00-05', 60.9, 12.0, 3.1); -- fuera de la hora

-- ¿En qué partición física quedó cada fila?
SELECT tableoid::regclass AS particion, ts, vibracion
FROM lecturas_sensor
ORDER BY ts DESC;
