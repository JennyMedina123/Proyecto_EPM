from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg

def leer_bronze():
    # Usamos rglob para que Python baje por todas las subcarpetas automáticamente
    archivos = sorted((cfg.BRONZE / "lecturas_sensor").rglob("*.parquet"))

    if not archivos:
        raise FileNotFoundError("No se encontraron archivos Parquet de lecturas en Bronze.")

    return pd.concat([pd.read_parquet(archivo) for archivo in archivos], ignore_index=True)

def leer_catalogo_sensores():
    carpeta = cfg.BRONZE / "catalogo" / "sensor"
    archivos = sorted(carpeta.glob("sensor_*.parquet"))
    if not archivos:
        raise FileNotFoundError("No se encontró el catálogo de sensores en Bronze.")
    catalogo = pd.read_parquet(archivos[-1])
    # En Cassandra, el sensor_id es un UUID almacenado como string, validamos que no sea nulo.
    return set(catalogo["sensor_id"].dropna().astype(str).str.strip().str.lower())

def transformar_silver():
    print("\nIniciando transformación de Bronze a Silver...")
    df = leer_bronze()
    total_bronze = len(df)

    # 1. Normalizar identificadores (sensor_id y turbina_id generados como UUIDs)
    df["sensor_id"] = df["sensor_id"].astype("string").str.strip().str.lower()
    if "turbina_id" in df.columns:
        df["turbina_id"] = df["turbina_id"].astype("string").str.strip().str.lower()

    # 2. Convertir y tipar fechas correctamente[cite: 17]
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["fecha"] = df["timestamp"].dt.date

    # 3. Limpiar y validar las mediciones (-999 a nulo)[cite: 17]
    metricas = ["temperatura", "presion", "vibracion"]
    for columna in metricas:
        df[columna] = pd.to_numeric(df[columna], errors="coerce")
        df.loc[df[columna] == -999, columna] = float("nan")

    # Rangos físicos permitidos
    rangos = {"temperatura": (-10, 120), "presion": (0, 100), "vibracion": (0, 50)}
    for columna, (minimo, maximo) in rangos.items():
        fuera_de_rango = (df[columna] < minimo) | (df[columna] > maximo)
        df.loc[fuera_de_rango, columna] = float("nan")

    # 4. Eliminar filas con identificador inválido (obligatorio para la calidad)[cite: 17]
    validos = df.loc[~(df["sensor_id"].isna() | df["timestamp"].isna())].copy()

    # 5. Detectar sensores huerfanos (No registrados en catálogo)
    sensores_conocidos = leer_catalogo_sensores()
    silver = validos.loc[validos["sensor_id"].isin(sensores_conocidos)].copy()

    # 6. Eliminar duplicados exactos[cite: 18]
    filas_antes = len(silver)
    silver = silver.drop_duplicates(subset=["sensor_id", "timestamp"], keep="first").reset_index(drop=True)

    # 7. Guardar resultados
    cfg.SILVER.mkdir(parents=True, exist_ok=True)
    silver.to_parquet(cfg.SILVER / "lecturas_silver.parquet", index=False, compression="snappy")

    print(f"Filas Bronze originales: {total_bronze:>10,}")
    print(f"Filas Silver resultantes: {len(silver):>10,}")
    print(f"Filas eliminadas: {total_bronze - len(silver):>10,}")
    print(f"Silver guardado en {cfg.SILVER}")

if __name__ == "__main__":
    transformar_silver()
