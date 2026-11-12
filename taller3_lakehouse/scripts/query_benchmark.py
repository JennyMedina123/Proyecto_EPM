"""
query_benchmark.py

Benchmark comparativo de las consultas del catálogo sobre los layouts
físicos de la tabla de hechos (baseline, single_rg, sorted).

Por cada layout y consulta:
    1. Guarda el plan EXPLAIN ANALYZE completo.
    2. Registra métricas del perfilador: filas emitidas por el scan de la
       fact, cardinalidad estimada, row groups leídos / totales y bytes
       leídos de disco.
    3. Mide latencia: 3 ejecuciones de calentamiento + 5 mediciones.

Las mediciones se repiten en RONDAS rondas rotando el orden de los layouts,
de modo que el ruido del entorno (caché del sistema operativo, carga de
CPU) afecte por igual a todos. La latencia final de cada layout es la
mediana de todas sus mediciones (RONDAS x 5).

Factores reportados:
    factor_total    = t_baseline / t_sorted   (efecto observado)
    factor_aislado  = t_single_rg / t_sorted  (efecto del layout, mismo escritor)
Un factor > 1 indica que el layout ordenado es más rápido.

Salidas en docs/performance/:
    plan_<layout>_<consulta>.txt
    benchmark_mediciones.csv   todas las mediciones, por ronda
    benchmark_resumen.csv      una fila por consulta
    benchmark_resumen.md       tabla resumen en Markdown
"""

import csv
import statistics
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import query_catalog as qc

RONDAS = 3


def main():
    faltantes = [str(r) for r in qc.LAYOUTS.values() if not r.exists()]
    if faltantes:
        sys.exit("Faltan layouts. Ejecute primero: python scripts/layout_optimizer.py\n"
                 + "\n".join(faltantes))

    qc.DOCS_PERF.mkdir(parents=True, exist_ok=True)
    version = duckdb.__version__
    layouts = list(qc.LAYOUTS)

    print("=" * 70)
    print(f"QUERY BENCHMARK  |  DuckDB {version}  |  layouts: {', '.join(layouts)}")
    print("=" * 70)

    row_groups = {lay: qc.contar_row_groups(ruta) for lay, ruta in qc.LAYOUTS.items()}
    print("Row groups:", row_groups)

    # ------------------------------------------------------------------
    # 1. Planes y métricas del perfilador (una vez por layout y consulta)
    # ------------------------------------------------------------------
    metricas = {}
    for lay, ruta in qc.LAYOUTS.items():
        con = qc.conectar(ruta)
        try:
            for nombre, query in qc.TODAS.items():
                devueltas = len(con.execute(query).fetchall())
                qc.guardar_plan(con, nombre, lay, query)
                metricas[(lay, nombre)] = {"filas_devueltas": devueltas,
                                           **qc.perfil_consulta(ruta, query)}
        finally:
            con.close()
    print(f"Planes guardados en {qc.DOCS_PERF}")

    # ------------------------------------------------------------------
    # 2. Latencia en rondas con orden rotado
    # ------------------------------------------------------------------
    detalle = []
    tiempos = {}
    for ronda in range(1, RONDAS + 1):
        desplazamiento = (ronda - 1) % len(layouts)
        orden = layouts[desplazamiento:] + layouts[:desplazamiento]
        print(f"\n--- Ronda {ronda}: {' -> '.join(orden)} ---")
        for lay in orden:
            con = qc.conectar(qc.LAYOUTS[lay])
            try:
                for nombre, query in qc.TODAS.items():
                    medidas, mediana = qc.medir(con, query)
                    tiempos.setdefault((lay, nombre), []).extend(medidas)
                    detalle.append({
                        "ronda": ronda, "layout": lay, "consulta": nombre,
                        **{f"medicion_{i}_ms": round(v, 3) for i, v in enumerate(medidas, 1)},
                        "mediana_ms": round(mediana, 3),
                    })
                    print(f"[{lay:9}] {nombre:28} mediana = {mediana:8.3f} ms")
            finally:
                con.close()

    with (qc.DOCS_PERF / "benchmark_mediciones.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(detalle[0].keys()))
        w.writeheader()
        w.writerows(detalle)

    # ------------------------------------------------------------------
    # 3. Resumen por consulta
    # ------------------------------------------------------------------
    resumen = []
    for nombre in qc.TODAS:
        fila = {"consulta": nombre, "motor": f"DuckDB {version}",
                "filas_devueltas": metricas[("baseline", nombre)]["filas_devueltas"]}
        med = {}
        for lay in layouts:
            med[lay] = statistics.median(tiempos[(lay, nombre)])
            m = metricas[(lay, nombre)]
            fila[f"row_groups_{lay}"] = row_groups[lay]
            fila[f"filas_scan_{lay}"] = m["filas_scan"]
            fila[f"filas_estimadas_{lay}"] = m["filas_estimadas"]
            fila[f"rg_leidos_{lay}"] = m["rg_leidos"]
            fila[f"rg_totales_{lay}"] = m["rg_totales"]
            fila[f"bytes_{lay}"] = m["bytes_leidos"]
            fila[f"t_{lay}_ms"] = round(med[lay], 3)
        fila["factor_total"] = round(med["baseline"] / med["sorted"], 2)
        fila["factor_aislado"] = round(med["single_rg"] / med["sorted"], 2)
        resumen.append(fila)

    with (qc.DOCS_PERF / "benchmark_resumen.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(resumen[0].keys()))
        w.writeheader()
        w.writerows(resumen)

    lineas = [
        f"Motor: DuckDB {version}. Latencia = mediana de {RONDAS * 5} mediciones "
        f"({RONDAS} rondas x 5, tras 3 calentamientos por ronda).",
        "",
        "| Consulta | Filas dev. | t baseline (ms) | t single_rg (ms) | t sorted (ms) "
        "| Factor total | Factor aislado | RG leídos single_rg -> sorted | Bytes single_rg -> sorted |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in resumen:
        lineas.append(
            f"| {r['consulta']} | {r['filas_devueltas']} | {r['t_baseline_ms']} "
            f"| {r['t_single_rg_ms']} | {r['t_sorted_ms']} | {r['factor_total']}x "
            f"| {r['factor_aislado']}x "
            f"| {r['rg_leidos_single_rg']}/{r['rg_totales_single_rg']} -> {r['rg_leidos_sorted']}/{r['rg_totales_sorted']} "
            f"| {r['bytes_single_rg']} -> {r['bytes_sorted']} |"
        )
    texto = "\n".join(lineas)
    (qc.DOCS_PERF / "benchmark_resumen.md").write_text(texto + "\n", encoding="utf-8")

    print("\n" + "=" * 70)
    print("RESUMEN")
    print("=" * 70)
    print(texto)
    print(f"\nResultados en: {qc.DOCS_PERF}")


if __name__ == "__main__":
    main()
