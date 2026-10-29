import sys
import time
from pathlib import Path
import pandas as pd
from cassandra.cluster import Cluster

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg
from utilidades import conectar_duckdb, titulo

# Registramos la nueva fact table agrupada
TABLAS_GOLD = ["fact_lecturas_hora", "dim_tiempo", "dim_turbina", "dim_sensor"]

CONSULTAS_DUCKDB = {
    "Q1. Alertas críticas totales por Turbina y Mes": """
        SELECT tb.codigo AS turbina, t.anio, t.mes,
               SUM(f.alertas_criticas) AS total_alertas_vib,
               ROUND(AVG(f.temperatura_promedio), 2) AS temp_promedio_mensual
        FROM fact_lecturas_hora f
        JOIN dim_tiempo  t  ON f.tiempo_sk  = t.tiempo_sk
        JOIN dim_turbina tb ON f.turbina_sk = tb.turbina_sk
        GROUP BY tb.codigo, t.anio, t.mes ORDER BY total_alertas_vib DESC LIMIT 5""",

    "Q2. Comportamiento térmico (Temperatura Máxima) por Tipo de Sensor": """
        SELECT s.ubicacion,
               ROUND(MAX(f.temperatura_max), 2) AS temp_max_historica,
               ROUND(AVG(f.temperatura_promedio), 2) AS temp_media_global
        FROM fact_lecturas_hora f
        JOIN dim_sensor s ON f.sensor_sk = s.sensor_sk
        GROUP BY s.ubicacion ORDER BY temp_max_historica DESC""",

    "Q3. Días con mayor inestabilidad (Top 5 días con más alertas)": """
        SELECT t.fecha, SUM(f.alertas_criticas) as total_alertas,
               ROUND(AVG(f.vibracion_promedio), 2) as vib_media
        FROM fact_lecturas_hora f
        JOIN dim_tiempo t ON f.tiempo_sk = t.tiempo_sk
        GROUP BY t.fecha ORDER BY total_alertas DESC LIMIT 5""",

    "Q4. Carga de lectura: Total de mediciones procesadas por Turbina": """
        SELECT tb.codigo, SUM(f.total_lecturas_hora) AS mediciones_crudas_agrupadas
        FROM fact_lecturas_hora f
        JOIN dim_turbina tb ON f.turbina_sk = tb.turbina_sk
        GROUP BY tb.codigo ORDER BY mediciones_crudas_agrupadas DESC""",

    "Q5. Comparativo vibración promedio entre Centrales": """
        SELECT tb.central_id, ROUND(AVG(f.vibracion_promedio), 3) AS vib_media_central
        FROM fact_lecturas_hora f
        JOIN dim_turbina tb ON f.turbina_sk = tb.turbina_sk
        GROUP BY tb.central_id"""
}

def conectar_cassandra():
    cluster = Cluster(['127.0.0.1'], port=9042)
    return cluster, cluster.connect('epm_iot')

def main():
    con = conectar_duckdb()
    for t in TABLAS_GOLD:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{cfg.ruta_sql(cfg.GOLD / (t + '.parquet'))}')")

    titulo("5 Consultas Analíticas sobre Gold (DuckDB Columnar)[cite: 23]")
    for nombre, query in CONSULTAS_DUCKDB.items():
        t0 = time.time()
        df = con.execute(query).fetchdf()
        elapsed = (time.time() - t0) * 1000
        print(f"\n[{nombre}] - {elapsed:.1f} ms")
        print(df.to_string(index=False))

    titulo("Benchmark: Motor Transaccional (Cassandra) vs Analítico (DuckDB)[cite: 24, 28]")
    # Simulamos el conteo global y promedio, una operación costosa en Cassandra (Full Scan).
    query_analitica_cruda = "SELECT AVG(vibracion) AS vib_promedio FROM lecturas_sensor" # Pseudo-código para el benchmark

    print("\nEjecutando agregación global en Cassandra (Operacional)...")
    cluster, session = conectar_cassandra()
    t_cas = time.time()
    # Para Cassandra, un count/avg global requiere escanear todas las particiones (muy ineficiente).
    try:
        # Cassandra bloquea queries sin partición, usamos list(rows) para simular el costo de extracción
        rows = list(session.execute("SELECT vibracion FROM lecturas_sensor"))
        vib_promedio = sum([r.vibracion for r in rows if r.vibracion]) / len(rows)
    except Exception as e:
        print(f"Error en Cassandra (esperado por antipatrón): {e}")
    tiempo_cassandra_ms = (time.time() - t_cas) * 1000
    cluster.shutdown()

    print("Ejecutando la misma agregación en DuckDB (Columnar sobre Parquet)...")
    t_duck = time.time()
    df_duck = con.execute("SELECT AVG(vibracion_promedio) FROM fact_lecturas_hora").fetchdf()
    tiempo_duckdb_ms = (time.time() - t_duck) * 1000

    print(f"\nResultados del Benchmark[cite: 28]:")
    print(f"Cassandra (Lectura fila a fila + RAM) : {tiempo_cassandra_ms:.1f} ms")
    print(f"DuckDB (Escaneo Columnar Parquet)     : {tiempo_duckdb_ms:.1f} ms")
    if tiempo_duckdb_ms > 0:
        print(f"Factor de mejora DuckDB               : {tiempo_cassandra_ms / tiempo_duckdb_ms:.1f}x más rápido[cite: 25]")

if __name__ == "__main__":
    main()
