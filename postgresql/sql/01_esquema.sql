-- =====================================================================
-- 01_esquema.sql  |  EPM IoT - Modelo relacional en PostgreSQL 16
-- Equivalente relacional del keyspace EPM_IOT de Cassandra
-- Ejecutar:  docker exec -it postgres-epm psql -U epm -d epm_iot -f /scripts/01_esquema.sql
-- =====================================================================

-- Extensión para inspeccionar tuplas muertas (demo de expiración, script 06)
CREATE EXTENSION IF NOT EXISTS pgstattuple;

DROP SCHEMA IF EXISTS bench CASCADE;   -- resultados de experimentos previos
DROP SCHEMA IF EXISTS epm_iot CASCADE;
CREATE SCHEMA epm_iot;
SET search_path TO epm_iot;
ALTER DATABASE epm_iot SET search_path TO epm_iot, public;

-- ---------------------------------------------------------------------
-- Tablas maestras (normalización: en Cassandra turbina_id se repetía en
-- cada lectura; aquí se guarda una sola vez y se obtiene con JOIN)
-- ---------------------------------------------------------------------
CREATE TABLE central (
    central_id  SMALLINT     PRIMARY KEY,
    nombre      VARCHAR(40)  NOT NULL UNIQUE
);

CREATE TABLE turbina (
    turbina_id  UUID         PRIMARY KEY,
    central_id  SMALLINT     NOT NULL REFERENCES central(central_id),
    codigo      VARCHAR(20)  NOT NULL UNIQUE
);

CREATE TABLE sensor (
    sensor_id   UUID         PRIMARY KEY,
    turbina_id  UUID         NOT NULL REFERENCES turbina(turbina_id),
    codigo      VARCHAR(20)  NOT NULL UNIQUE
);
CREATE INDEX idx_sensor_turbina ON sensor(turbina_id);

-- ---------------------------------------------------------------------
-- Tabla de hechos: lecturas_sensor
--   Cassandra : PRIMARY KEY ((sensor_id, date), timestamp)
--               CLUSTERING ORDER BY (timestamp DESC)
--   PostgreSQL: PARTITION BY RANGE (ts)  -> una partición física por día
--               PRIMARY KEY (sensor_id, ts) -> B-tree que agrupa por sensor
--               y ordena por tiempo (se recorre hacia atrás = DESC)
-- ---------------------------------------------------------------------
CREATE TABLE lecturas_sensor (
    sensor_id    UUID         NOT NULL REFERENCES sensor(sensor_id),
    ts           TIMESTAMPTZ  NOT NULL,          -- "timestamp" en Cassandra
    temperatura  REAL,
    presion      REAL,
    vibracion    REAL,
    PRIMARY KEY (sensor_id, ts)
) PARTITION BY RANGE (ts);

-- Partición por defecto: recibe lecturas de días sin partición creada
CREATE TABLE lecturas_sensor_default PARTITION OF lecturas_sensor DEFAULT;

-- ---------------------------------------------------------------------
-- Función: crea particiones diarias en un rango de fechas
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION crear_particiones_diarias(p_desde DATE, p_hasta DATE)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    d DATE := p_desde;
    n INTEGER := 0;
    nombre TEXT;
BEGIN
    WHILE d <= p_hasta LOOP
        nombre := format('lecturas_sensor_%s', to_char(d, 'YYYYMMDD'));
        IF to_regclass(nombre) IS NULL THEN
            EXECUTE format(
              'CREATE TABLE %I PARTITION OF lecturas_sensor
                 FOR VALUES FROM (%L) TO (%L)',
              nombre, d::timestamptz, (d + 1)::timestamptz);
            n := n + 1;
        END IF;
        d := d + 1;
    END LOOP;
    RETURN n;
END $$;

-- ---------------------------------------------------------------------
-- Función: "TTL" relacional -> elimina particiones completas vencidas.
-- DROP de una partición no genera tuplas muertas (equivalente a evitar
-- tombstones en Cassandra). p_referencia permite simular el "ahora".
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION purgar_particiones(
    p_retencion  INTERVAL,
    p_referencia TIMESTAMPTZ DEFAULT now())
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE
    r RECORD;
    limite_sup TIMESTAMPTZ;
BEGIN
    FOR r IN
        SELECT c.relname,
               pg_get_expr(c.relpartbound, c.oid) AS limites
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'epm_iot.lecturas_sensor'::regclass
          AND c.relname <> 'lecturas_sensor_default'
    LOOP
        -- limites = FOR VALUES FROM ('...') TO ('...')
        limite_sup := substring(r.limites FROM $r$TO \('([^']+)'\)$r$)::timestamptz;
        IF limite_sup <= p_referencia - p_retencion THEN
            EXECUTE format('ALTER TABLE lecturas_sensor DETACH PARTITION %I', r.relname);
            EXECUTE format('DROP TABLE %I', r.relname);
            RETURN NEXT r.relname;
        END IF;
    END LOOP;
END $$;

-- Particiones para los datos del reto:
--   01-05 sep 2026 -> 100.000 lecturas simuladas (generar_datos.py)
--   28 sep 2026    -> lecturas de prueba (02_datos_prueba.sql)
--   hoy + 7 días   -> lecturas reales que lleguen con now()
SELECT crear_particiones_diarias('2026-09-01', '2026-09-05')
     + crear_particiones_diarias('2026-09-28', '2026-09-28')
     + crear_particiones_diarias(current_date, current_date + 7) AS particiones_creadas;

-- Verificación
\d+ lecturas_sensor
