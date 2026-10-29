from pathlib import Path

import duckdb


def conectar_duckdb(ruta=None):
    return duckdb.connect(str(ruta) if ruta else ":memory:")


def tamano_mb(ruta: Path) -> float:
    ruta = Path(ruta)
    if ruta.is_file():
        return ruta.stat().st_size / 1_048_576
    return sum(p.stat().st_size for p in ruta.rglob("*") if p.is_file()) / 1_048_576


def titulo(texto: str):
    print("\n" + "-" * 70)
    print(f"  {texto}")
    print("-" * 70)
