# config.py
import os
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
LAKEHOUSE = RAIZ / "lakehouse"
BRONZE = LAKEHOUSE / "bronze"
SILVER = LAKEHOUSE / "silver"
GOLD = LAKEHOUSE / "gold"
DOCS = RAIZ / "docs"

# Ya no usamos OLTP_URL de SQLite, dependemos directamente del driver de Cassandra
FRECUENCIA_MIN = int(os.environ.get("EPM_FRECUENCIA_MIN", "5"))
DIAS_INICIALES = int(os.environ.get("EPM_DIAS_INICIALES", "3"))
HORAS_POR_CORRIDA = int(os.environ.get("EPM_HORAS", "6"))
SEMILLA = int(os.environ["EPM_SEMILLA"]) if os.environ.get("EPM_SEMILLA") else 42
LECTURAS_ESPERADAS_HORA = 60 // FRECUENCIA_MIN

def ruta_sql(p: Path) -> str:
    return Path(p).as_posix()
