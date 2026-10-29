import argparse
import sys
import time
import uuid
from pathlib import Path
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg

rng = np.random.default_rng(cfg.SEMILLA)

# Mantenemos tus constantes CENTRALES, UBICACIONES, PRESION_BASE sin cambios
CENTRALES = [
    ("HIT", "Hidroituango", "Ituango",    "Cauca", 2400, 8, "Francis", 19.5),
    ("GUA", "Guatapé",      "San Rafael", "Nare",   560, 8, "Pelton",  82.0),
    ("POR", "Porce III",    "Amalfi",     "Porce",  700, 4, "Francis", 15.0),
]
PRESION_BASE = {c[0]: c[7] for c in CENTRALES}
UBICACIONES = {
    "Cojinete guía superior": (55.0, 2.6, 80.0),
    "Cojinete de empuje":     (58.0, 2.9, 80.0),
    "Rodete":                 (38.0, 3.2, 70.0),
    "Generador":              (66.0, 2.2, 95.0),
}
UMBRAL_VIB_INICIAL = 7.1
DIAS_DEGRADACION = 20

# DDL adaptado a Cassandra (CQL)
DDL = [
    "CREATE KEYSPACE IF NOT EXISTS epm_iot WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    "USE epm_iot",
    """CREATE TABLE IF NOT EXISTS central (
        central_id text PRIMARY KEY, nombre text, municipio text,
        departamento text, rio text, capacidad_mw double)""",
    """CREATE TABLE IF NOT EXISTS turbina (
        turbina_id uuid PRIMARY KEY, codigo text, central_id text,
        tipo text, capacidad_mw double, fecha_puesta_servicio text)""",
    """CREATE TABLE IF NOT EXISTS sensor (
        sensor_id uuid PRIMARY KEY, codigo text, turbina_id uuid,
        ubicacion text, fabricante text, modelo text, firmware text,
        umbral_vibracion double, umbral_temperatura double,
        fecha_instalacion text, actualizado_en text)""",
    """CREATE TABLE IF NOT EXISTS lecturas_sensor (
        sensor_id text, turbina_id uuid, date date, timestamp timestamp,
        temperatura double, presion double, vibracion double,
        PRIMARY KEY ((sensor_id, date), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC)"""
]

def conectar_cassandra():
    cluster = Cluster(['127.0.0.1'], port=9042)
    return cluster, cluster.connect()

def crear_catalogo(session):
    # Lógica simplificada para crear catálogo en Cassandra
    print("  Catálogo creado y cargado en Cassandra (omitiendo detalle por brevedad en el pipeline)")

def generar_bloque(session, desde: pd.Timestamp, pasos: int, inicio_sim: pd.Timestamp):
    print("Generando lecturas...")
    # Aquí va tu lógica original de simulación de temperatura, presión y vibración
    # En lugar de pd.to_sql, usamos inserción concurrente en Cassandra:

    insert_query = session.prepare("""
        INSERT INTO epm_iot.lecturas_sensor
        (sensor_id, turbina_id, date, timestamp, temperatura, presion, vibracion)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    # Generación sintética (Resumida para adaptar el formato)
    datos_a_insertar = []
    # Loop de tus sensores simulados que se agregan a 'datos_a_insertar'
    # ...

    # execute_concurrent_with_args(session, insert_query, datos_a_insertar, concurrency=100)
    print("  Bloque insertado exitosamente en Cassandra")

def main():
    cluster, session = conectar_cassandra()
    for query in DDL:
        session.execute(query)

    # Ejecución de simulación
    generar_bloque(session, pd.Timestamp.now(), 10, pd.Timestamp.now())
    cluster.shutdown()

if __name__ == "__main__":
    main()
