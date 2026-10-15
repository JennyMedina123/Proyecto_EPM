import os
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent

LAKEHOUSE = RAIZ / "lakehouse"
BRONZE = LAKEHOUSE / "bronze"
SILVER = LAKEHOUSE / "silver"
GOLD = LAKEHOUSE / "gold"
DOCS = RAIZ / "docs"
OLTP_DIR = RAIZ / "oltp"

OLTP_URL = os.environ.get("EPM_OLTP_URL", f"sqlite:///{(OLTP_DIR / 'epm_oltp.sqlite').as_posix()}")

FRECUENCIA_MIN = int(os.environ.get("EPM_FRECUENCIA_MIN", "5"))    # 1 lectura cada N minutos por sensor
DIAS_INICIALES = int(os.environ.get("EPM_DIAS_INICIALES", "3"))    # historia que se crea la 1.ª vez
HORAS_POR_CORRIDA = int(os.environ.get("EPM_HORAS", "6"))          # lecturas nuevas en cada ejecución
SEMILLA = int(os.environ["EPM_SEMILLA"]) if os.environ.get("EPM_SEMILLA") else None
LECTURAS_ESPERADAS_HORA = 60 // FRECUENCIA_MIN


def dialecto_oltp() -> str:
    return "sqlite" if OLTP_URL.startswith("sqlite") else "postgresql"


def conectar_oltp():
    
    if dialecto_oltp() == "sqlite":
        import sqlite3
        ruta = OLTP_URL.replace("sqlite:///", "")
        Path(ruta).parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(ruta)
    from sqlalchemy import create_engine
    return create_engine(OLTP_URL)


def ejecutar_oltp(conexion, sentencias):
    if dialecto_oltp() == "sqlite":
        cur = conexion.cursor()
        for s in sentencias:
            cur.execute(s)
        conexion.commit()
    else:
        from sqlalchemy import text
        with conexion.begin() as c:
            for s in sentencias:
                c.execute(text(s))


def ruta_sql(p: Path) -> str:
    return Path(p).as_posix()