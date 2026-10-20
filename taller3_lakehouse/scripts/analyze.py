"""Paso 5: dos consultas analíticas sobre Gold (DuckDB) y benchmark contra la base operacional.

Cada consulta se escribe dos veces, con el mismo significado de negocio:
  - oltp : sobre las tablas operacionales (lecturas_sensor, sensor, turbina) con JOINs
  - gold : sobre el modelo estrella en Parquet (fact_lecturas + dimensiones) con DuckDB
"""
import statistics
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg
from utilidades import conectar_duckdb, titulo

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 30)
REPETICIONES = 5
TABLAS_GOLD = ["fact_lecturas", "dim_tiempo", "dim_turbina", "dim_sensor"]
salida = []


def log(txt=""):
    print(txt)
    salida.append(str(txt))


# ═════════════════════════════════════════════════════════════════
# LAS DOS CONSULTAS
# ═════════════════════════════════════════════════════════════════
CONSULTAS = {
    "Q1. Degradación: vibración diaria en cojinetes, HIT-T3 vs el resto de turbinas": {
        "gold": """
            SELECT t.fecha,
                   ROUND(AVG(f.vibracion) FILTER (WHERE tb.codigo = 'HIT-T3'), 3)  AS vib_HIT_T3,
                   ROUND(AVG(f.vibracion) FILTER (WHERE tb.codigo <> 'HIT-T3'), 3) AS vib_resto,
                   ROUND(AVG(f.vibracion) FILTER (WHERE tb.codigo = 'HIT-T3')
                       / AVG(f.vibracion) FILTER (WHERE tb.codigo <> 'HIT-T3'), 2) AS razon
            FROM fact_lecturas f
            JOIN dim_tiempo  t  ON f.tiempo_sk  = t.tiempo_sk
            JOIN dim_turbina tb ON f.turbina_sk = tb.turbina_sk
            JOIN dim_sensor  s  ON f.sensor_sk  = s.sensor_sk
            WHERE s.ubicacion LIKE 'Cojinete%'
            GROUP BY t.fecha ORDER BY t.fecha""",
        "oltp": """
            SELECT l.date AS fecha,
                   AVG(CASE WHEN tb.codigo =  'HIT-T3' AND l.vibracion <> -999 THEN l.vibracion END) AS vib_HIT_T3,
                   AVG(CASE WHEN tb.codigo <> 'HIT-T3' AND l.vibracion <> -999 THEN l.vibracion END) AS vib_resto
            FROM lecturas_sensor l
            JOIN sensor  s  ON s.sensor_id  = l.sensor_id
            JOIN turbina tb ON tb.turbina_id = l.turbina_id
            WHERE s.ubicacion LIKE 'Cojinete%'
            GROUP BY l.date ORDER BY l.date""",
    },
    "Q2. Top 10 turbinas por vibración máxima": {
        "gold": """
            SELECT tb.codigo AS turbina,
                   ROUND(MAX(f.vibracion), 2) AS vib_maxima_mms,
                   ROUND(AVG(f.vibracion), 3) AS vib_prom_mms
            FROM fact_lecturas f
            JOIN dim_turbina tb ON f.turbina_sk = tb.turbina_sk
            GROUP BY tb.codigo ORDER BY vib_maxima_mms DESC LIMIT 10""",
        "oltp": """
            SELECT tb.codigo AS turbina,
                   MAX(CASE WHEN l.vibracion <> -999 THEN l.vibracion END) AS vib_maxima_mms,
                   AVG(CASE WHEN l.vibracion <> -999 THEN l.vibracion END) AS vib_prom_mms
            FROM lecturas_sensor l
            JOIN turbina tb ON tb.turbina_id = l.turbina_id
            GROUP BY tb.codigo ORDER BY vib_maxima_mms DESC LIMIT 10""",
    },
}


def medir(fn):
    """Mediana de REPETICIONES ejecuciones, tras una de calentamiento."""
    fn()
    tiempos = []
    for _ in range(REPETICIONES):
        t0 = time.perf_counter()
        df = fn()
        tiempos.append((time.perf_counter() - t0) * 1000)
    return statistics.median(tiempos), len(df)


def main():
    faltan = [t for t in TABLAS_GOLD if not (cfg.GOLD / f"{t}.parquet").exists()]
    if faltan:
        sys.exit(f"Faltan tablas Gold {faltan}. Ejecute primero scripts/transform_gold.py")

    con = conectar_duckdb()
    for t in TABLAS_GOLD:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{cfg.ruta_sql(cfg.GOLD / (t + '.parquet'))}')")

    # ── 1. Resultados de las consultas (DuckDB sobre Gold) ───────
    titulo("Consultas analíticas sobre Gold (DuckDB)")
    for nombre, q in CONSULTAS.items():
        df = con.execute(q["gold"]).fetchdf()
        log(f"\n[{nombre}] — {len(df)} filas")
        log(df.to_string(index=False))

    # ── 2. Benchmark: misma consulta en OLTP vs DuckDB ───────────
    titulo("Benchmark: base operacional vs DuckDB (Parquet)")
    oltp = cfg.conectar_oltp()
    n_oltp = int(pd.read_sql("SELECT COUNT(*) AS n FROM lecturas_sensor", oltp)["n"][0])
    n_gold = int(con.execute("SELECT COUNT(*) FROM fact_lecturas").fetchone()[0])
    log(f"\nFilas -> {cfg.dialecto_oltp()} (OLTP): {n_oltp:,} | Gold (fact_lecturas): {n_gold:,}")
    if n_oltp < n_gold:
        log("AVISO: la base operacional tiene MENOS filas que Gold; el lakehouse parece venir de otra "
            "ejecución.\n       Ejecute 'python main.py --reset' para que la comparación sea equivalente.")
    log(f"Mediana de {REPETICIONES} ejecuciones tras 1 de calentamiento\n")

    filas = []
    for nombre, q in CONSULTAS.items():
        t_o, r_o = medir(lambda: pd.read_sql(q["oltp"], oltp))
        t_g, r_g = medir(lambda: con.execute(q["gold"]).fetchdf())
        filas.append({"consulta": nombre,
                      "oltp_ms": t_o, "duckdb_ms": t_g, "mejora_x": t_o / t_g})
        log(f"{nombre}\n  {cfg.dialecto_oltp():<8} {t_o:>9.1f} ms ({r_o} filas)\n"
            f"  DuckDB   {t_g:>9.1f} ms ({r_g} filas)  -> {t_o / t_g:.1f}x")

    bench = pd.DataFrame(filas).round(2)
    cfg.DOCS.mkdir(parents=True, exist_ok=True)
    bench.to_csv(cfg.DOCS / "benchmark.csv", index=False)
    md = [f"| Motor | Consulta | Tiempo |", "|---|---|---|"]
    for r in bench.itertuples():
        md.append(f"| {cfg.dialecto_oltp()} (OLTP, filas) | {r.consulta} | {r.oltp_ms:.1f} ms |")
        md.append(f"| DuckDB (columnar, Parquet) | {r.consulta} | {r.duckdb_ms:.1f} ms ({r.mejora_x:.1f}x más rápido) |")
    (cfg.DOCS / "benchmark.md").write_text("\n".join(md), encoding="utf-8")
    log("\nTabla para el README (docs/benchmark.md):\n" + "\n".join(md))
    (cfg.DOCS / "resultados_consultas.txt").write_text("\n".join(salida), encoding="utf-8")
    if cfg.dialecto_oltp() == "sqlite":
        oltp.close()


if __name__ == "__main__":
    main()