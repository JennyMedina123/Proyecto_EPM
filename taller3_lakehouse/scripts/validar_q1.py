
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg

# Consultas original y optimizada.
query_original = """
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
"""

query_optimizada = """
    WITH tiempo_filtrado AS (
        SELECT tiempo_sk, anio, mes
        FROM dim_tiempo
        WHERE mes = 10
          AND anio = 2026
    )
    SELECT
        tb.codigo AS turbina,
        t.anio,
        t.mes,
        SUM(f.alertas_criticas) AS total_alertas
    FROM tiempo_filtrado t
    JOIN fact_lecturas_hora f
        ON f.tiempo_sk = t.tiempo_sk
    JOIN dim_turbina tb
        ON f.turbina_sk = tb.turbina_sk
    GROUP BY tb.codigo, t.anio, t.mes
    ORDER BY total_alertas DESC, turbina ASC
"""

con = duckdb.connect()

try:
    tablas = [
        "fact_lecturas_hora",
        "dim_tiempo",
        "dim_turbina",
        "dim_sensor",
    ]

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

    original = sorted(con.execute(query_original).fetchall())
    optimizada = sorted(con.execute(query_optimizada).fetchall())

    print("Filas de la consulta original:", len(original))
    print("Filas de la consulta optimizada:", len(optimizada))

    if original == optimizada:
        print("VALIDACIÓN CORRECTA: los resultados son idénticos.")
    else:
        print("VALIDACIÓN FALLIDA: los resultados son diferentes.")

        print("\nResultados originales:")
        for fila in original:
            print(fila)

        print("\nResultados optimizados:")
        for fila in optimizada:
            print(fila)

        raise AssertionError(
            "La consulta original y la optimizada producen resultados diferentes."
        )

finally:
    con.close()
