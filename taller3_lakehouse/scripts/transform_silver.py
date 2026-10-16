
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg


def leer_bronze():
    """Lee todas las lecturas almacenadas en la capa Bronze."""
    archivos = sorted(
        (cfg.BRONZE / "lecturas_sensor").glob("*/*.parquet")
    )

    if not archivos:
        raise FileNotFoundError(
            "No se encontraron archivos Parquet de lecturas en Bronze."
        )

    dataframes = [pd.read_parquet(archivo) for archivo in archivos]
    return pd.concat(dataframes, ignore_index=True)


def leer_catalogo_sensores():
    """Lee la versión más reciente del catálogo de sensores."""
    carpeta = cfg.BRONZE / "catalogo" / "sensor"
    archivos = sorted(carpeta.glob("sensor_*.parquet"))

    if not archivos:
        raise FileNotFoundError(
            "No se encontró el catálogo de sensores en Bronze."
        )

    catalogo = pd.read_parquet(archivos[-1])

    if "sensor_id" not in catalogo.columns:
        raise ValueError(
            "El catálogo no contiene la columna sensor_id."
        )

    return set(
        catalogo["sensor_id"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.lower()
    )


def transformar_silver():
    print("\nIniciando transformación de Bronze a Silver...")

    # 1. Leer las fuentes
    df = leer_bronze()
    total_bronze = len(df)

    # 2. Normalizar identificadores y fechas
    df["sensor_id"] = (
        df["sensor_id"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    df["timestamp"] = pd.to_datetime(
        df["timestamp"].astype("string").str.strip().str.replace(
            "T", " ", regex=False
        ),
        errors="coerce"
    )

    df["fecha"] = df["timestamp"].dt.date

     
    # 3. Limpiar y validar las mediciones
    metricas = ["temperatura", "presion", "vibracion"]
    valores_centinela = {}

    for columna in metricas:
        df[columna] = pd.to_numeric(
            df[columna], errors="coerce"
        )

        # Contar los -999 antes de convertirlos en nulos
        valores_centinela[columna] = int(
            (df[columna] == -999).sum()
        )

        df.loc[df[columna] == -999, columna] = float("nan")


    # Aplicar rangos válidos sin eliminar la lectura completa
    rangos = {
        "temperatura": (-10, 120),
        "presion": (0, 100),
        "vibracion": (0, 50),
    }

        # Contar y corregir valores fuera de rango
    valores_fuera_rango = {}

    for columna, (minimo, maximo) in rangos.items():
        fuera_de_rango = (
            (df[columna] < minimo) | (df[columna] > maximo)
        )

        valores_fuera_rango[columna] = int(fuera_de_rango.sum())

        df.loc[fuera_de_rango, columna] = float("nan")
    # 4. Separar registros con identificador o fecha inválidos
    sin_clave = (
        df["sensor_id"].isna()
        | df["sensor_id"].eq("")
        | df["timestamp"].isna()
    )

    cuarentena_calidad = df.loc[sin_clave].copy()
    validos = df.loc[~sin_clave].copy()

    # 5. Detectar sensores que no están registrados en el catálogo
    sensores_conocidos = leer_catalogo_sensores()

    desconocido = ~validos["sensor_id"].isin(sensores_conocidos)

    cuarentena_sensores = validos.loc[desconocido].copy()
    silver = validos.loc[~desconocido].copy()

    # 6. Eliminar duplicados por sensor e instante
    filas_antes = len(silver)

    silver = (
        silver.sort_values("lectura_id")
        .drop_duplicates(
            subset=["sensor_id", "timestamp"],
            keep="first"
        )
        .reset_index(drop=True)
    )

    duplicados = filas_antes - len(silver)

    # 7. Guardar los resultados
    cfg.SILVER.mkdir(parents=True, exist_ok=True)

    silver.to_parquet(
        cfg.SILVER / "lecturas_silver.parquet",
        index=False,
        compression="snappy"
    )

    cuarentena_sensores.to_parquet(
        cfg.SILVER / "cuarentena_sensores.parquet",
        index=False,
        compression="snappy"
    )

    cuarentena_calidad.to_parquet(
        cfg.SILVER / "cuarentena_calidad.parquet",
        index=False,
        compression="snappy"
    )

    # 8. Mostrar resumen de calidad
    resumen = pd.DataFrame([
        {"indicador": "Filas originales en Bronze",
         "cantidad": total_bronze},
        {"indicador": "Filas válidas en Silver",
         "cantidad": len(silver)},
        {"indicador": "Duplicados eliminados",
         "cantidad": duplicados},
        {"indicador": "Valores centinela -999 en temperatura",
         "cantidad": valores_centinela["temperatura"]},
        {"indicador": "Valores centinela -999 en presion",
            "cantidad": valores_centinela["presion"]},
        {"indicador": "Valores centinela -999 en vibracion",
            "cantidad": valores_centinela["vibracion"]},
        {"indicador": "Sensores desconocidos en cuarentena",
         "cantidad": len(cuarentena_sensores)},
        {"indicador": "Registros con clave o fecha inválida",
         "cantidad": len(cuarentena_calidad)},
        {"indicador": "Temperaturas fuera de rango corregidas",
         "cantidad": valores_fuera_rango["temperatura"]},
        {"indicador": "Presiones fuera de rango corregidas",
         "cantidad": valores_fuera_rango["presion"]},
        {"indicador": "Vibraciones fuera de rango corregidas",
         "cantidad": valores_fuera_rango["vibracion"]}, 
    ])

    resumen.to_csv(
        cfg.SILVER / "informe_calidad.csv",
        index=False,
        encoding="utf-8"
    )

    print("\nTransformación finalizada.")
    print(resumen.to_string(index=False))
    print(f"\nArchivos generados en: {cfg.SILVER}")

    # 9. Generar resumen estadístico de las mediciones
    metricas = ["temperatura", "presion", "vibracion"]

    estadisticas = []

    for columna in metricas:
        estadisticas.append({
            "metrica": columna,
            "valores_validos": int(silver[columna].count()),
            "promedio": silver[columna].mean(),
            "minimo": silver[columna].min(),
            "maximo": silver[columna].max()
        })

    estadisticas_df = pd.DataFrame(estadisticas)

    estadisticas_df.to_csv(
        cfg.SILVER / "estadisticas_silver.csv",
        index=False,
        encoding="utf-8"
    )

    print("\nResumen estadístico de Silver:")
    print(estadisticas_df.to_string(index=False))


if __name__ == "__main__":
    transformar_silver()
