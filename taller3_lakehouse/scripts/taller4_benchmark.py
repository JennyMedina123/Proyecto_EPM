import sys
import time
import statistics
import csv
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg


# Crear la carpeta de documentación si no existe.
cfg.DOCS.mkdir(parents=True, exist_ok=True)


# Tres consultas seleccionadas para el diagnóstico inicial.
QUERIES = {
    "Q1_Alertas_Mes": """
        SELECT
            tb.codigo AS turbina,
            t.anio,
            t.mes,
            SUM(f.alertas_criticas) AS total_alertas
        FROM fact_lecturas_hora f
        JOIN dim_tiempo t
            ON f.tiempo_sk = t.tiempo_sk
        JOIN dim_turbina tb
            ON f.turbina_sk = tb.turbina_sk
        WHERE t.mes = 10
          AND t.anio = 2026
        GROUP BY tb.codigo, t.anio, t.mes
        ORDER BY total_alertas DESC
    """,

    "Q2_Comportamiento_Termico": """
        SELECT
            s.ubicacion,
            ROUND(MAX(f.temperatura_max), 2)
                AS temp_max_historica,
            ROUND(AVG(f.temperatura_promedio), 2)
                AS temp_media_global
        FROM fact_lecturas_hora f
        JOIN dim_sensor s
            ON f.sensor_sk = s.sensor_sk
        GROUP BY s.ubicacion
        ORDER BY temp_max_historica DESC
    """,

    "Q3_Dias_Inestabilidad": """
        SELECT
            t.fecha,
            SUM(f.alertas_criticas) AS total_alertas,
            ROUND(AVG(f.vibracion_promedio), 2) AS vib_media
        FROM fact_lecturas_hora f
        JOIN dim_tiempo t
            ON f.tiempo_sk = t.tiempo_sk
        WHERE t.fecha >= DATE '2026-10-17'
          AND t.fecha <= DATE '2026-10-26'
        GROUP BY t.fecha
        ORDER BY total_alertas DESC
        LIMIT 5
    """
}


def obtener_plan_ejecucion(con, nombre, query):
    """Ejecuta EXPLAIN ANALYZE y guarda el plan completo."""

    resultado = con.execute(
        f"EXPLAIN ANALYZE {query}"
    ).fetchdf()

    plan_texto = resultado.iloc[0, 1]

    ruta_plan = cfg.DOCS / f"plan_baseline_{nombre}.txt"

    ruta_plan.write_text(
        str(plan_texto),
        encoding="utf-8"
    )

    print(f"Plan guardado: {ruta_plan.name}")


def medir_tiempos(con, query):
    """Realiza tres calentamientos y cinco mediciones."""

    # Tres ejecuciones de calentamiento, sin medir.
    for _ in range(3):
        con.execute(query).fetchall()

    # Cinco ejecuciones medidas.
    tiempos_ms = []

    for numero in range(1, 6):
        inicio = time.perf_counter()

        con.execute(query).fetchall()

        fin = time.perf_counter()

        tiempo_ms = (fin - inicio) * 1000
        tiempos_ms.append(tiempo_ms)

        print(
            f"Medición {numero}: {tiempo_ms:.3f} ms"
        )

    mediana_ms = statistics.median(tiempos_ms)

    return tiempos_ms, mediana_ms


def main():
    print("=" * 65)
    print("TALLER 4 - BENCHMARK BASELINE CON DUCKDB")
    print("=" * 65)

    con = duckdb.connect()

    tablas = [
        "fact_lecturas_hora",
        "dim_tiempo",
        "dim_turbina",
        "dim_sensor"
    ]

    # Crear vistas sobre los archivos Parquet existentes.
    for tabla in tablas:
        ruta = cfg.GOLD / f"{tabla}.parquet"

        if not ruta.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo: {ruta}"
            )

        con.execute(
            f"""
            CREATE VIEW {tabla} AS
            SELECT *
            FROM read_parquet('{cfg.ruta_sql(ruta)}')
            """
        )

    resultados = []

    try:
        for nombre, query in QUERIES.items():
            print("\n" + "-" * 65)
            print(f"CONSULTA: {nombre}")
            print("-" * 65)

            # Validar que la consulta pueda ejecutarse.
            resultado = con.execute(query).fetchdf()

            print(f"Filas devueltas: {len(resultado)}")

            # Guardar el plan de ejecución.
            obtener_plan_ejecucion(con, nombre, query)

            # Medir tiempos después de tres calentamientos.
            tiempos_ms, mediana_ms = medir_tiempos(
                con, query
            )

            print(
                f"Mediana de cinco mediciones: "
                f"{mediana_ms:.3f} ms"
            )

            registro = {
                "consulta": nombre,
                "motor": "DuckDB",
                "fase": "baseline",
                "calentamientos": 3,
                "medicion_1_ms": tiempos_ms[0],
                "medicion_2_ms": tiempos_ms[1],
                "medicion_3_ms": tiempos_ms[2],
                "medicion_4_ms": tiempos_ms[3],
                "medicion_5_ms": tiempos_ms[4],
                "mediana_ms": mediana_ms,
                "filas_devueltas": len(resultado)
            }

            resultados.append(registro)

    finally:
        con.close()

    # Guardar las mediciones para elaborar la tabla del informe.
    ruta_csv = cfg.DOCS / "benchmark_baseline.csv"

    columnas = [
        "consulta",
        "motor",
        "fase",
        "calentamientos",
        "medicion_1_ms",
        "medicion_2_ms",
        "medicion_3_ms",
        "medicion_4_ms",
        "medicion_5_ms",
        "mediana_ms",
        "filas_devueltas"
    ]

    with ruta_csv.open(
        "w",
        newline="",
        encoding="utf-8-sig"
    ) as archivo:
        escritor = csv.DictWriter(
            archivo,
            fieldnames=columnas
        )
        escritor.writeheader()
        escritor.writerows(resultados)

    print("\n" + "=" * 65)
    print("RESUMEN DEL BENCHMARK INICIAL")
    print("=" * 65)

    for registro in resultados:
        print(
            f"{registro['consulta']}: "
            f"mediana = {registro['mediana_ms']:.3f} ms"
        )

    print(f"\nResultados guardados en: {ruta_csv}")
    print("Benchmark inicial finalizado.")


if __name__ == "__main__":
    main()