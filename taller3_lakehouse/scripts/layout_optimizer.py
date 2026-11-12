"""
layout_optimizer.py

Genera layouts físicos alternativos de la tabla de hechos para evaluar
data skipping, sin modificar Gold:

    single_rg  lakehouse/gold_single_rg/  ordenada por tiempo_sk, 1 row group
    sorted     lakehouse/gold_sorted/     ordenada por tiempo_sk, row groups
                                          de ROW_GROUP_SIZE filas

Con un único row group, las estadísticas min/max de tiempo_sk cubren el
rango completo y el motor nunca puede descartar el bloque. Con varios row
groups ordenados, cada uno cubre un rango estrecho de tiempo_sk y los que
no intersectan el filtro se omiten sin leerse.

Ambos layouts se escriben con el mismo motor (DuckDB) y la misma
configuración; la única diferencia entre ellos es el tamaño de row group.
Así la comparación single_rg -> sorted aísla el efecto del layout, y la
comparación baseline (pyarrow) -> single_rg cuantifica el efecto del
escritor Parquet.

Implementado con DuckDB (COPY ... ORDER BY ... ROW_GROUP_SIZE), sin pyarrow.
"""

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg
import query_catalog as qc


def mostrar_row_groups(ruta: Path, titulo: str) -> list:
    """Imprime los row groups del archivo con su rango de tiempo_sk."""
    grupos = qc.row_groups(ruta)
    total = sum(g[1] for g in grupos)
    tam_kb = ruta.stat().st_size / 1024
    print(f"\n{titulo}: {total:,} filas en {len(grupos)} row group(s) | archivo {tam_kb:,.1f} KB")
    for rg_id, filas, minimo, maximo in grupos:
        print(f"   row group {rg_id:>2}: {filas:>6,} filas | tiempo_sk {minimo:>3} a {maximo:>3}")
    return grupos


def escribir_layout(destino: Path, row_group_size: int) -> None:
    """Reescribe la tabla de hechos ordenada por tiempo_sk con el tamaño de
    row group indicado."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.unlink(missing_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_parquet('{cfg.ruta_sql(qc.FACT_BASELINE)}')
                ORDER BY tiempo_sk
            )
            TO '{cfg.ruta_sql(destino)}'
            (FORMAT parquet, ROW_GROUP_SIZE {row_group_size})
        """)
    finally:
        con.close()


def controles(base: list, nuevo: list, nombre: str) -> None:
    """Verifica que el layout tenga las mismas filas y rangos ordenados."""
    filas_base = sum(g[1] for g in base)
    filas_nuevo = sum(g[1] for g in nuevo)
    assert filas_base == filas_nuevo, f"{nombre}: el número de filas no coincide"

    minimos = [int(g[2]) for g in nuevo]
    maximos = [int(g[3]) for g in nuevo]
    solapados = sum(1 for i in range(1, len(nuevo)) if minimos[i] < maximos[i - 1])

    print(f"   [{nombre}] filas iguales: {filas_base == filas_nuevo} | "
          f"row groups: {len(base)} -> {len(nuevo)} | solapados: {solapados}")


def main():
    print("=" * 70)
    print("LAYOUT OPTIMIZER - fact_lecturas_hora")
    print("=" * 70)

    base = mostrar_row_groups(qc.FACT_BASELINE, "BASELINE  (lakehouse/gold, escrito por pyarrow)")

    escribir_layout(qc.FACT_SINGLE_RG, qc.ROW_GROUP_SIZE_SINGLE)
    single = mostrar_row_groups(qc.FACT_SINGLE_RG, "SINGLE_RG (lakehouse/gold_single_rg, DuckDB)")

    escribir_layout(qc.FACT_SORTED, qc.ROW_GROUP_SIZE)
    ordenado = mostrar_row_groups(qc.FACT_SORTED, "SORTED    (lakehouse/gold_sorted, DuckDB)")

    print("\nControles:")
    controles(base, single, "single_rg")
    controles(base, ordenado, "sorted")
    print("   (solapados = 0 significa que los rangos de tiempo_sk están en orden)")


if __name__ == "__main__":
    main()
