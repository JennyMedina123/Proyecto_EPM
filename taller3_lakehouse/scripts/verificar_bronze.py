import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg
from utilidades import conectar_duckdb, tamano_mb, titulo

LECTURAS = cfg.ruta_sql(cfg.BRONZE / "lecturas_sensor" / "*" / "*.parquet")


def ultimo(tabla):
    archivos = sorted((cfg.BRONZE / "catalogo" / tabla).glob(f"{tabla}_*.parquet"))
    if not archivos:
        sys.exit(f"No hay Bronze para {tabla}. Ejecute primero: python scripts/extract.py")
    return cfg.ruta_sql(archivos[-1])


def main():
    con = conectar_duckdb()
    con.execute(f"CREATE VIEW b_lecturas AS SELECT * FROM read_parquet('{LECTURAS}', hive_partitioning = true)")
    salida = []

    def mostrar(texto=""):
        print(texto)
        salida.append(texto)

    titulo("1. Conteos y esquema")
    r = con.execute("""
        SELECT COUNT(*) AS filas, COUNT(DISTINCT fecha) AS dias, MIN(fecha) AS desde, MAX(fecha) AS hasta,
               COUNT(DISTINCT _lote) AS lotes, MIN(_extraido_en) AS primera_extraccion,
               MAX(_extraido_en) AS ultima_extraccion
        FROM b_lecturas""").fetchdf()
    print(r.to_string(index=False))
    esquema = con.execute("DESCRIBE SELECT * FROM b_lecturas").fetchdf()
    print("\n  Esquema de bronze/lecturas_sensor:")
    print(esquema[["column_name", "column_type"]].to_string(index=False))
    for tabla in ["central", "turbina", "sensor", "sensor_auditoria"]:
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{ultimo(tabla)}')").fetchone()[0]
        print(f"  catálogo {tabla:<17} {int(n):>6,} filas (último lote)")

    titulo("2. Controles de la extracción")
    repetidos = con.execute("SELECT COUNT(*) - COUNT(DISTINCT lectura_id) FROM b_lecturas").fetchone()[0]
    print(f"  Lecturas copiadas más de una vez (debe ser 0): {int(repetidos)}")
    lotes = con.execute("""SELECT _lote, COUNT(*) AS filas, MIN(lectura_id) AS desde_id, MAX(lectura_id) AS hasta_id
                           FROM b_lecturas GROUP BY _lote ORDER BY _lote""").fetchdf()
    print(f"  Lotes de extracción de lecturas: {len(lotes)}")
    print(lotes.to_string(index=False))
    try:
        oltp = cfg.conectar_oltp()
        fuente = int(pd.read_sql("SELECT COUNT(*) AS n FROM lecturas_sensor", oltp)["n"][0])
        bronze = int(r["filas"][0])
        print(f"  Filas en la fuente = {fuente:,} | filas en Bronze = {bronze:,} -> "
              f"{'OK, copia completa' if fuente == bronze else 'DIFERENCIA (hay lecturas sin extraer)'}")
    except Exception as e:
        print(f"  (No se pudo consultar la fuente OLTP para comparar: {type(e).__name__})")
    print(f"  Tamaño de Bronze en disco: {tamano_mb(cfg.BRONZE):,.1f} MB")

    titulo("3. Perfil de calidad del dato crudo (lo que Silver debe resolver)")
    p = con.execute("""
        SELECT COUNT(*) AS filas,
               COUNT(*) FILTER (WHERE temperatura IS NULL OR isnan(temperatura)) AS nulos_temperatura,
               COUNT(*) FILTER (WHERE presion     IS NULL OR isnan(presion))     AS nulos_presion,
               COUNT(*) FILTER (WHERE vibracion   IS NULL OR isnan(vibracion))   AS nulos_vibracion,
               COUNT(*) FILTER (WHERE temperatura = -999 OR vibracion = -999 OR presion = -999) AS centinela_999,
               COUNT(*) FILTER (WHERE sensor_id <> lower(sensor_id))  AS sensor_id_mayusculas,
               COUNT(*) FILTER (WHERE sensor_id <> trim(sensor_id))   AS sensor_id_espacios,
               COUNT(*) FILTER (WHERE CAST("timestamp" AS VARCHAR) LIKE '%T%') AS timestamp_con_T
        FROM b_lecturas""").fetchdf().iloc[0]
    dups = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT (lower(trim(sensor_id)), replace(CAST("timestamp" AS VARCHAR), 'T', ' ')))
        FROM b_lecturas""").fetchone()[0]
    huerf = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT lower(trim(sensor_id))) FROM b_lecturas
        WHERE lower(trim(sensor_id)) NOT IN (SELECT lower(sensor_id) FROM read_parquet('{ultimo("sensor")}'))
        """).fetchone()
    rango = con.execute("""
        SELECT MIN(CAST(replace(CAST("timestamp" AS VARCHAR), 'T', ' ') AS TIMESTAMP)),
               MAX(CAST(replace(CAST("timestamp" AS VARCHAR), 'T', ' ') AS TIMESTAMP))
        FROM b_lecturas""").fetchone()
    t_ini, t_fin = pd.Timestamp(rango[0]), pd.Timestamp(rango[1])
    instantes = int((t_fin - t_ini).total_seconds() // (cfg.FRECUENCIA_MIN * 60)) + 1
    sensores = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{ultimo('sensor')}')").fetchone()[0])
    esperadas = sensores * instantes

    n = int(p["filas"])
    pct = lambda x: f"{int(x):>9,}  ({int(x) / n * 100:5.2f} %)"
    mostrar("=" * 66)
    mostrar("  PERFIL DE CALIDAD — ZONA BRONZE (lecturas_sensor, dato crudo)")
    mostrar("=" * 66)
    mostrar(f"Filas en Bronze                         : {n:>9,}")
    mostrar(f"Rango de lecturas                       : {t_ini} -> {t_fin}")
    mostrar(f"Lotes de extracción                     : {len(lotes)}")
    mostrar("")
    mostrar(f"Nulos en temperatura                    : {pct(p['nulos_temperatura'])}")
    mostrar(f"Nulos en presión                        : {pct(p['nulos_presion'])}")
    mostrar(f"Nulos en vibración                      : {pct(p['nulos_vibracion'])}")
    mostrar(f"Filas con valor centinela -999          : {pct(p['centinela_999'])}")
    mostrar(f"sensor_id en MAYÚSCULAS                 : {pct(p['sensor_id_mayusculas'])}")
    mostrar(f"sensor_id con espacios al final         : {pct(p['sensor_id_espacios'])}")
    mostrar(f"timestamp con formato 'AAAA-MM-DDTHH..' : {pct(p['timestamp_con_T'])}")
    mostrar(f"Duplicados (mismo sensor e instante)    : {pct(dups)}")
    mostrar(f"Lecturas de sensores fuera del catálogo : {pct(huerf[0])}  -> {int(huerf[1])} sensor(es) desconocido(s)")
    mostrar(f"Completitud (filas / esperadas)         : {n / esperadas * 100:.2f} % de {esperadas:,} lecturas esperadas")
    mostrar("")
    mostrar("Recomendaciones para Silver: tipar timestamp (2 formatos), normalizar sensor_id")
    mostrar("(lower + trim), convertir -999 y valores fuera de rango físico en NULL, eliminar")
    mostrar("duplicados por (sensor_id, timestamp) y apartar las lecturas de sensores desconocidos.")
    mostrar("=" * 66)

    destino = cfg.DOCS / "perfil_bronze.txt"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text("\n".join(salida), encoding="utf-8")
    print(f"\nPerfil guardado en {destino.relative_to(cfg.RAIZ).as_posix()}")


if __name__ == "__main__":
    main()
