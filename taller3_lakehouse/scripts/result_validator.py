"""
result_validator.py

Validación funcional de los layouts físicos: cada consulta del catálogo
debe devolver exactamente el mismo resultado sobre todos los layouts.

Los layouts single_rg y sorted solo cambian la organización física del
archivo (escritor, orden y tamaño de row group); los datos son los mismos.
Si alguna consulta devolviera un resultado distinto, el layout estaría
alterando el análisis y no podría usarse.

Se comparan los resultados ordenados y con los decimales redondeados a 6
cifras, para no confundir diferencias de punto flotante en los promedios
(el orden de suma puede variar entre layouts) con diferencias reales.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import query_catalog as qc


def normalizar(filas):
    """Ordena las filas y redondea los valores decimales."""
    salida = []
    for fila in filas:
        salida.append(tuple(round(v, 6) if isinstance(v, float) else v for v in fila))
    return sorted(salida, key=str)


def main():
    faltantes = [str(r) for r in qc.LAYOUTS.values() if not r.exists()]
    if faltantes:
        sys.exit("Faltan layouts. Ejecute primero: python scripts/layout_optimizer.py")

    print("=" * 70)
    print("RESULT VALIDATOR - resultados por layout (referencia: baseline)")
    print("=" * 70)

    conexiones = {lay: qc.conectar(ruta) for lay, ruta in qc.LAYOUTS.items()}
    fallos = 0
    try:
        for nombre, query in qc.TODAS.items():
            referencia = normalizar(conexiones["baseline"].execute(query).fetchall())
            estados = []
            for lay, con in conexiones.items():
                if lay == "baseline":
                    continue
                resultado = normalizar(con.execute(query).fetchall())
                iguales = resultado == referencia
                fallos += not iguales
                estados.append(f"{lay}: {'IDÉNTICO' if iguales else 'DIFERENTE'}")
            print(f"{nombre:28} filas={len(referencia):3}  |  " + "  |  ".join(estados))
    finally:
        for con in conexiones.values():
            con.close()

    print()
    if fallos:
        raise SystemExit(f"VALIDACIÓN FALLIDA: {fallos} comparación(es) con resultados distintos.")
    print("VALIDACIÓN CORRECTA: todos los layouts devuelven resultados idénticos.")


if __name__ == "__main__":
    main()
