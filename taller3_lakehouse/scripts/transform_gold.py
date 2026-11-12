from pathlib import Path
import pandas as pd
import config as cfg

def leer_ultimo_parquet(carpeta):
    archivos = sorted(Path(carpeta).glob("*.parquet"))
    return pd.read_parquet(archivos[-1])

def normalizar_id(df, columna):
    if columna in df.columns:
        df[columna] = df[columna].astype("string").str.strip().str.lower()
    return df

def construir_gold():
    print("\n" + "=" * 60)
    print("INICIANDO TRANSFORMACION GOLD - LECTURAS POR HORA")
    print("=" * 60)
    cfg.GOLD.mkdir(parents=True, exist_ok=True)

    # 1. Leer capa Silver
    lecturas = pd.read_parquet(cfg.SILVER / "lecturas_silver.parquet")
    normalizar_id(lecturas, "sensor_id")
    normalizar_id(lecturas, "turbina_id")
    lecturas["timestamp"] = pd.to_datetime(lecturas["timestamp"])

    # 2. Leer Catálogos y construir Dimensiones Básicas
    base_catalogos = cfg.BRONZE / "catalogo"
    centrales = leer_ultimo_parquet(base_catalogos / "central")
    turbinas = leer_ultimo_parquet(base_catalogos / "turbina")
    sensores = leer_ultimo_parquet(base_catalogos / "sensor")

    for df, cols in [(centrales, ["central_id"]), (turbinas, ["turbina_id", "central_id"]), (sensores, ["sensor_id", "turbina_id"])]:
        for c in cols: normalizar_id(df, c)

    # DIM_TURBINA
    dim_turbina = turbinas.drop_duplicates(subset=["turbina_id"]).copy()
    dim_turbina.insert(0, "turbina_sk", range(1, len(dim_turbina) + 1))
    dim_turbina.to_parquet(cfg.GOLD / "dim_turbina.parquet", index=False)

    # DIM_SENSOR
    dim_sensor = sensores.drop_duplicates(subset=["sensor_id"]).copy()
    dim_sensor.insert(0, "sensor_sk", range(1, len(dim_sensor) + 1))
    dim_sensor.to_parquet(cfg.GOLD / "dim_sensor.parquet", index=False)

    # 3. Transformación al Hecho Analítico Central: Agrupación por Hora
    hechos = lecturas.copy()
    # Redondeamos el timestamp a la hora exacta
    hechos['hora'] = hechos['timestamp'].dt.floor('h')

    # Realizamos la agregación analítica requerida
    fact_agrupada = hechos.groupby(['hora', 'sensor_id', 'turbina_id']).agg(
        temperatura_promedio=('temperatura', 'mean'),
        temperatura_max=('temperatura', 'max'),
        presion_promedio=('presion', 'mean'),
        vibracion_promedio=('vibracion', 'mean'),
        vibracion_max=('vibracion', 'max'),
        total_lecturas_hora=('vibracion', 'count'),
        # Lógica de alertas: cuántas mediciones en esa hora superaron 7.1 mm/s de vibración
        alertas_criticas=('vibracion', lambda x: (x > 7.1).sum())
    ).reset_index()

    # 4. Creación de DIM_TIEMPO basada en las horas agrupadas
    timestamps = fact_agrupada[['hora']].drop_duplicates().sort_values('hora').reset_index(drop=True)
    timestamps.insert(0, "tiempo_sk", range(1, len(timestamps) + 1))
    timestamps["fecha"] = timestamps["hora"].dt.date
    timestamps["anio"] = timestamps["hora"].dt.year
    timestamps["mes"] = timestamps["hora"].dt.month
    timestamps["dia"] = timestamps["hora"].dt.day
    timestamps["hora_dia"] = timestamps["hora"].dt.hour
    timestamps.to_parquet(cfg.GOLD / "dim_tiempo.parquet", index=False)

    # 5. Cruce de Claves Subrogadas (SKs) para la FACT_TABLE
    # Unir tiempo_sk
    fact_agrupada = fact_agrupada.merge(timestamps[['hora', 'tiempo_sk']], on='hora', how='left')
    # Unir turbina_sk
    fact_agrupada = fact_agrupada.merge(dim_turbina[['turbina_id', 'turbina_sk']], on='turbina_id', how='left')
    # Unir sensor_sk
    fact_agrupada = fact_agrupada.merge(dim_sensor[['sensor_id', 'sensor_sk']], on='sensor_id', how='left')

    # Seleccionar columnas finales de la FACT_TABLE
    columnas_fact = [
        "tiempo_sk", "turbina_sk", "sensor_sk",
        "temperatura_promedio", "temperatura_max", "presion_promedio",
        "vibracion_promedio", "vibracion_max",
        "total_lecturas_hora", "alertas_criticas"
    ]
    fact_final = fact_agrupada[columnas_fact].copy()

    # Escritura de la tabla de hechos ordenada por tiempo_sk.
    # Con el volumen actual (~17 mil filas), row_group_size=100_000 deja todo el
    # archivo en un único row group: el motor no puede omitir bloques usando sus
    # estadísticas min/max (data skipping). El efecto de row groups más pequeños
    # se evalúa en scripts/layout_optimizer.py sin modificar esta capa.
    fact_final = fact_final.sort_values('tiempo_sk')

    fact_final.to_parquet(
        cfg.GOLD / "fact_lecturas_hora.parquet",
        index=False,
        row_group_size=100_000
    )
    print(f"FACT_LECTURAS_HORA guardada: {len(fact_final):,} filas agregadas.")

if __name__ == "__main__":
    construir_gold()
