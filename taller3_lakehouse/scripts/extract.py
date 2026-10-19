import csv
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg
from utilidades import conectar_duckdb, titulo

LOTE = datetime.now().strftime("%Y%m%d_%H%M%S")
TABLAS_CATALOGO = ["central", "turbina", "sensor", "sensor_auditoria"]
CONTROL = cfg.BRONZE / "_control_extracciones.csv"


def guardar_bronze(duck, df, ruta, fuente):
    if ruta.exists():
        raise FileExistsError(f"Bronze es inmutable y ya existe: {ruta}")
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    duck.register("_df_bronze", df)
    duck.execute(f"""
        COPY (
            SELECT *,
                   TIMESTAMP '{ahora}' AS _extraido_en,
                   '{fuente}'          AS _fuente,
                   '{LOTE}'            AS _lote
            FROM _df_bronze
        ) TO '{cfg.ruta_sql(ruta)}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    duck.unregister("_df_bronze")
    registrar_control(ruta, len(df), fuente)
    return len(df)


def registrar_control(ruta, filas, fuente):
    nuevo = not CONTROL.exists()
    with open(CONTROL, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if nuevo:
            w.writerow(["lote", "extraido_en", "fuente", "archivo", "filas"])
        w.writerow([LOTE, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), fuente,
                    ruta.relative_to(cfg.BRONZE).as_posix(), filas])


def extraer_catalogo(oltp, duck):
    titulo("Catálogo (foto completa)")
    for tabla in TABLAS_CATALOGO:
        df = pd.read_sql(f"SELECT * FROM {tabla}", oltp)
        ruta = cfg.BRONZE / "catalogo" / tabla / f"{tabla}_{LOTE}.parquet"
        guardar_bronze(duck, df, ruta, cfg.dialecto_oltp())
        print(f"  {tabla:<17} {len(df):>8,} filas -> {ruta.relative_to(cfg.RAIZ).as_posix()}")


def marca_de_agua(duck):
    if not any((cfg.BRONZE / "lecturas_sensor").glob("fecha=*/*.parquet")):
        return 0
    patron = cfg.ruta_sql(cfg.BRONZE / "lecturas_sensor" / "*" / "*.parquet")
    return int(duck.execute(f"SELECT MAX(lectura_id) FROM read_parquet('{patron}')").fetchone()[0])


def extraer_lecturas(oltp, duck):
    titulo("Telemetría lecturas_sensor (incremental por marca de agua)")
    fuente = cfg.dialecto_oltp()
    wm = marca_de_agua(duck)
    dias = pd.read_sql(f"SELECT DISTINCT date FROM lecturas_sensor WHERE lectura_id > {wm} ORDER BY date",
                       oltp)["date"].tolist()
    print(f"  Marca de agua: lectura_id > {wm:,} | días con lecturas nuevas: {len(dias)}")

    total = 0
    for dia in dias:
        df = pd.read_sql(f"SELECT * FROM lecturas_sensor WHERE lectura_id > {wm} AND date = '{dia}' "
                         f"ORDER BY lectura_id", oltp)
        ruta = cfg.BRONZE / "lecturas_sensor" / f"fecha={dia}" / f"lecturas_{LOTE}.parquet"
        total += guardar_bronze(duck, df, ruta, fuente)
        print(f"  fecha={dia}  +{len(df):>8,} filas")
    if total == 0:
        print("  No hay lecturas nuevas: ejecute primero python scripts/generar_oltp.py")
    else:
        print(f"  Lecturas nuevas en Bronze: {total:,}  (nueva marca de agua: {marca_de_agua(duck):,})")


def main():
    cfg.BRONZE.mkdir(parents=True, exist_ok=True)
    oltp = cfg.conectar_oltp()
    duck = conectar_duckdb()
    print(f"Lote de extracción: {LOTE}")
    extraer_catalogo(oltp, duck)
    extraer_lecturas(oltp, duck)
    print("\nExtracción completa. Verifique con: python scripts/verificar_bronze.py")


if __name__ == "__main__":
    main()