import duckdb
import time
import statistics
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg

# Asegurar que el directorio de documentación exista para guardar los planes
cfg.DOCS.mkdir(parents=True, exist_ok=True)

# Definimos 2 de las consultas analíticas del M3 enfocadas en rangos de tiempo
QUERIES = {
    "Q1_Alertas_Mes": """
        SELECT tb.codigo AS turbina, t.anio, t.mes, SUM(f.alertas_criticas) AS total_alertas
        FROM fact_lecturas_hora f
        JOIN dim_tiempo t ON f.tiempo_sk = t.tiempo_sk
        JOIN dim_turbina tb ON f.turbina_sk = tb.turbina_sk
        WHERE t.mes = 9 AND t.anio = 2026
        GROUP BY tb.codigo, t.anio, t.mes
    """,
    "Q3_Dias_Inestabilidad": """
        SELECT t.fecha, SUM(f.alertas_criticas) as total_alertas
        FROM fact_lecturas_hora f
        JOIN dim_tiempo t ON f.tiempo_sk = t.tiempo_sk
        WHERE t.fecha >= '2026-09-01' AND t.fecha <= '2026-09-15'
        GROUP BY t.fecha
    """
}

def obtener_plan_ejecucion(con, nombre, query):
    """Extrae el EXPLAIN ANALYZE y lo guarda en un archivo de texto[cite: 40]."""
    plan_df = con.execute(f"EXPLAIN ANALYZE {query}").fetchdf()
    plan_texto = plan_df.iloc[0, 1] if len(plan_df.columns) > 1 else plan_df.to_string()

    ruta_archivo = cfg.DOCS / f"plan_ejecucion_{nombre}.txt"
    ruta_archivo.write_text(plan_texto, encoding="utf-8")
    print(f"  -> Plan de ejecución guardado en {ruta_archivo.name}")

def medir_tiempo_mediana(con, query):
    """Realiza calentamiento y retorna la mediana de 5 ejecuciones[cite: 41]."""
    # Warm-up: 3 ejecuciones sin medir[cite: 41]
    for _ in range(3):
        con.execute(query).fetchall()

    # Medir: 5 ejecuciones[cite: 41]
    tiempos = []
    for _ in range(5):
        t0 = time.perf_counter()
        con.execute(query).fetchall()
        tiempos.append((time.perf_counter() - t0) * 1000)

    return statistics.median(tiempos)

def main():
    print("=" * 60)
    print("TALLER 4: DIAGNÓSTICO Y BENCHMARK DUCKDB")
    print("=" * 60)

    con = duckdb.connect()
    # Mapear los archivos Parquet directamente
    con.execute(f"CREATE VIEW fact_lecturas_hora AS SELECT * FROM read_parquet('{cfg.ruta_sql(cfg.GOLD / 'fact_lecturas_hora.parquet')}')")
    con.execute(f"CREATE VIEW dim_tiempo AS SELECT * FROM read_parquet('{cfg.ruta_sql(cfg.GOLD / 'dim_tiempo.parquet')}')")
    con.execute(f"CREATE VIEW dim_turbina AS SELECT * FROM read_parquet('{cfg.ruta_sql(cfg.GOLD / 'dim_turbina.parquet')}')")

    for nombre, query in QUERIES.items():
        print(f"\nProcesando {nombre}...")

        # 1. Obtener y guardar el plan de ejecución (Paso 1 del Taller 4)
        obtener_plan_ejecucion(con, nombre, query)

        # 2. Medir tiempo real con mediana (Paso 2 del Taller 4)
        mediana_ms = medir_tiempo_mediana(con, query)
        print(f"  -> Tiempo de ejecución (Mediana de 5 iteraciones): {mediana_ms:.2f} ms[cite: 41]")

if __name__ == "__main__":
    main()
