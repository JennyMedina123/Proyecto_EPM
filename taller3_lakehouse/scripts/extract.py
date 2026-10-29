import csv
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
from cassandra.cluster import Cluster

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg
from utilidades import conectar_duckdb, titulo

LOTE = datetime.now().strftime("%Y%m%d_%H%M%S")
TABLAS_CATALOGO = ["central", "turbina", "sensor"] # Eliminada auditoria para simplificar en NoSQL
CONTROL = cfg.BRONZE / "_control_extracciones.csv"

def extraer_desde_cassandra(duck):
    titulo("Extrayendo Telemetría desde Cassandra (Capa de Velocidad)")

    # Conexión a Cassandra
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('epm_iot')

    # Extracción siguiendo las directrices del Taller 3[cite: 15]
    print("  Consultando lecturas en Cassandra...")
    rows = session.execute('SELECT * FROM lecturas_sensor')
    df = pd.DataFrame(list(rows))

    if df.empty:
        print("  No hay datos en Cassandra.")
        return

    ruta = cfg.BRONZE / "lecturas_sensor" / f"lecturas_{LOTE}.parquet"
    ruta.parent.mkdir(parents=True, exist_ok=True)

    # Añadir metadata
    df['_extraido_en'] = datetime.now()
    df['_fuente'] = 'cassandra'
    df['_lote'] = LOTE

    # Guardado nativo a Parquet usando Pandas
    df.to_parquet(ruta, index=False)
    print(f"  + {len(df):,} filas guardadas en Bronze -> {ruta.relative_to(cfg.RAIZ).as_posix()}")

    cluster.shutdown()

def main():
    cfg.BRONZE.mkdir(parents=True, exist_ok=True)
    duck = conectar_duckdb()
    print(f"Lote de extracción: {LOTE}")
    extraer_desde_cassandra(duck)
    print("\nExtracción completa. Verifique con: python scripts/verificar_bronze.py")

if __name__ == "__main__":
    main()
