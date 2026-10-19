from pathlib import Path

import pandas as pd
import config as cfg


# ============================================================
# TRANSFORMACION GOLD - MINI LAKEHOUSE EPM
# ============================================================

SALIDA = cfg.GOLD


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def leer_ultimo_parquet(carpeta):
    """Lee el archivo Parquet mas reciente de una carpeta."""

    carpeta = Path(carpeta)

    if not carpeta.exists():
        raise FileNotFoundError(
            f"No existe la carpeta: {carpeta}"
        )

    archivos = sorted(carpeta.glob("*.parquet"))

    if not archivos:
        raise FileNotFoundError(
            f"No hay archivos Parquet en: {carpeta}"
        )

    return pd.read_parquet(archivos[-1])


def normalizar_id(df, columna):
    """Normaliza identificadores."""

    if columna in df.columns:
        df[columna] = (
            df[columna]
            .astype("string")
            .str.strip()
            .str.lower()
        )

    return df


def guardar_parquet(df, nombre):
    """Guarda una tabla Gold."""

    ruta = SALIDA / nombre
    df.to_parquet(ruta, index=False)

    print(f"Guardado: {ruta} ({len(df):,} filas)")


# ============================================================
# PROCESO PRINCIPAL
# ============================================================

def construir_gold():

    print("\n" + "=" * 60)
    print("INICIANDO TRANSFORMACION GOLD - EPM")
    print("=" * 60)

    SALIDA.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------
    # 1. LEER SILVER
    # --------------------------------------------------------

    ruta_silver = cfg.SILVER / "lecturas_silver.parquet"

    if not ruta_silver.exists():
        raise FileNotFoundError(
            f"No existe el archivo Silver: {ruta_silver}"
        )

    lecturas = pd.read_parquet(ruta_silver)

    if lecturas.empty:
        raise ValueError("Silver no contiene lecturas.")

    print(f"\nLecturas Silver: {len(lecturas):,}")

    for columna in ["sensor_id", "turbina_id"]:
        normalizar_id(lecturas, columna)

    lecturas["timestamp"] = pd.to_datetime(
        lecturas["timestamp"],
        errors="coerce"
    )

    lecturas = lecturas.dropna(
        subset=["sensor_id", "timestamp"]
    ).copy()

    # --------------------------------------------------------
    # 2. LEER CATALOGOS BRONZE
    # --------------------------------------------------------

    base_catalogos = cfg.BRONZE / "catalogo"

    centrales = leer_ultimo_parquet(
        base_catalogos / "central"
    )

    turbinas = leer_ultimo_parquet(
        base_catalogos / "turbina"
    )

    sensores = leer_ultimo_parquet(
        base_catalogos / "sensor"
    )

    for df, columnas in [
        (centrales, ["central_id"]),
        (turbinas, ["turbina_id", "central_id"]),
        (sensores, ["sensor_id", "turbina_id"]),
    ]:
        for columna in columnas:
            normalizar_id(df, columna)

    # --------------------------------------------------------
    # 3. DIMENSION CENTRAL
    # --------------------------------------------------------

    columnas_central = [
        "central_id",
        "nombre",
        "municipio",
        "departamento",
        "rio",
        "capacidad_mw",
    ]

    dim_central = centrales[
        [c for c in columnas_central if c in centrales.columns]
    ].copy()

    dim_central = (
        dim_central
        .drop_duplicates(subset=["central_id"])
        .sort_values("central_id")
        .reset_index(drop=True)
    )

    dim_central.insert(
        0,
        "central_sk",
        range(1, len(dim_central) + 1)
    )

    guardar_parquet(
        dim_central,
        "dim_central.parquet"
    )

    # --------------------------------------------------------
    # 4. DIMENSION TURBINA
    # --------------------------------------------------------

    columnas_turbina = [
        "turbina_id",
        "codigo",
        "central_id",
        "tipo",
        "capacidad_mw",
        "fecha_puesta_servicio",
    ]

    dim_turbina = turbinas[
        [c for c in columnas_turbina if c in turbinas.columns]
    ].copy()

    dim_turbina = dim_turbina.drop_duplicates(
        subset=["turbina_id"]
    )

    dim_turbina = dim_turbina.merge(
        dim_central[["central_id", "central_sk"]],
        on="central_id",
        how="left",
        validate="many_to_one"
    )

    dim_turbina = (
        dim_turbina
        .sort_values("turbina_id")
        .reset_index(drop=True)
    )

    dim_turbina.insert(
        0,
        "turbina_sk",
        range(1, len(dim_turbina) + 1)
    )

    guardar_parquet(
        dim_turbina,
        "dim_turbina.parquet"
    )

    # --------------------------------------------------------
    # 5. DIMENSION SENSOR E HISTORIAL
    # --------------------------------------------------------

    columnas_actuales = [
        "sensor_id",
        "codigo",
        "turbina_id",
        "ubicacion",
        "fabricante",
        "modelo",
        "firmware",
        "umbral_vibracion",
        "umbral_temperatura",
        "fecha_instalacion",
        "actualizado_en",
    ]

    sensores_actuales = sensores[
        [c for c in columnas_actuales if c in sensores.columns]
    ].copy()

    sensores_actuales = sensores_actuales.drop_duplicates(
        subset=["sensor_id"]
    )

    # Relacionar cada sensor con su turbina y central.

    relacion_turbina = dim_turbina[
        [
            "turbina_id",
            "central_id",
            "turbina_sk",
            "central_sk",
        ]
    ].copy()

    sensores_actuales = sensores_actuales.merge(
        relacion_turbina,
        on="turbina_id",
        how="left",
        validate="many_to_one"
    )

    # Intentar leer el historial de auditoria.

    auditoria = None

    try:
        auditoria = leer_ultimo_parquet(
            base_catalogos / "sensor_auditoria"
        )
    except FileNotFoundError:
        print(
            "\nAviso: no existe auditoria. "
            "Se utilizara el catalogo actual."
        )

    if (
        auditoria is not None
        and not auditoria.empty
        and "sensor_id" in auditoria.columns
        and "fecha_cambio" in auditoria.columns
    ):

        normalizar_id(auditoria, "sensor_id")

        auditoria["fecha_cambio"] = pd.to_datetime(
            auditoria["fecha_cambio"],
            errors="coerce"
        )

        auditoria = auditoria.dropna(
            subset=["sensor_id", "fecha_cambio"]
        ).copy()

        if "auditoria_id" in auditoria.columns:
            auditoria = auditoria.sort_values("auditoria_id")

        auditoria = auditoria.drop_duplicates(
            subset=["sensor_id", "fecha_cambio"],
            keep="last"
        )

        auditoria = auditoria.sort_values(
            ["sensor_id", "fecha_cambio"]
        ).reset_index(drop=True)

        # Cada registro representa una version del sensor.

        auditoria["fecha_inicio"] = auditoria["fecha_cambio"]

        auditoria["fecha_fin"] = (
            auditoria.groupby("sensor_id")["fecha_inicio"]
            .shift(-1)
        )

        auditoria["es_actual"] = auditoria["fecha_fin"].isna()

        columnas_historial = [
            "sensor_id",
            "fabricante",
            "modelo",
            "firmware",
            "umbral_vibracion",
            "umbral_temperatura",
            "fecha_inicio",
            "fecha_fin",
            "es_actual",
        ]

        columnas_historial = [
            c for c in columnas_historial
            if c in auditoria.columns
        ]

        historia = auditoria[columnas_historial].copy()

        # Se agregan atributos actuales que no estan en la
        # auditoria, evitando duplicar columnas compartidas.

        atributos_versionados = [
            "fabricante",
            "modelo",
            "firmware",
            "umbral_vibracion",
            "umbral_temperatura",
        ]

        columnas_complementarias = [
            c for c in sensores_actuales.columns
            if c != "sensor_id"
            and c not in atributos_versionados
        ]

        complementos = sensores_actuales[
            ["sensor_id"] + columnas_complementarias
        ].copy()

        dim_sensor = historia.merge(
            complementos,
            on="sensor_id",
            how="left",
            validate="many_to_one"
        )

        # Completar atributos historicos faltantes con los
        # valores disponibles en el catalogo actual.

        for atributo in atributos_versionados:

            if atributo in sensores_actuales.columns:

                valores_actuales = sensores_actuales[
                    ["sensor_id", atributo]
                ].drop_duplicates(subset=["sensor_id"])

                dim_sensor = dim_sensor.merge(
                    valores_actuales,
                    on="sensor_id",
                    how="left",
                    suffixes=("", "_catalogo"),
                    validate="many_to_one"
                )

                columna_catalogo = f"{atributo}_catalogo"

                if columna_catalogo in dim_sensor.columns:

                    if atributo in dim_sensor.columns:
                        dim_sensor[atributo] = (
                            dim_sensor[atributo]
                            .combine_first(dim_sensor[columna_catalogo])
                        )
                    else:
                        dim_sensor[atributo] = dim_sensor[
                            columna_catalogo
                        ]

                    dim_sensor = dim_sensor.drop(
                        columns=[columna_catalogo]
                    )

        # Agregar sensores que no tengan registros de auditoria.

        sensores_con_historia = set(
            dim_sensor["sensor_id"].dropna()
        )

        sensores_sin_historia = sensores_actuales[
            ~sensores_actuales["sensor_id"].isin(
                sensores_con_historia
            )
        ].copy()

        if not sensores_sin_historia.empty:

            fecha_minima = lecturas["timestamp"].min()

            if "fecha_instalacion" in sensores_sin_historia.columns:
                fecha_instalacion = pd.to_datetime(
                    sensores_sin_historia["fecha_instalacion"],
                    errors="coerce"
                )
            else:
                fecha_instalacion = pd.Series(
                    pd.NaT,
                    index=sensores_sin_historia.index
                )

            sensores_sin_historia["fecha_inicio"] = (
                fecha_instalacion.fillna(fecha_minima)
            )

            sensores_sin_historia["fecha_fin"] = pd.NaT
            sensores_sin_historia["es_actual"] = True

            dim_sensor = pd.concat(
                [dim_sensor, sensores_sin_historia],
                ignore_index=True,
                sort=False
            )

    else:

        # Sin auditoria, cada sensor tiene una version actual.

        dim_sensor = sensores_actuales.copy()

        if "fecha_instalacion" in dim_sensor.columns:
            fecha_inicio = pd.to_datetime(
                dim_sensor["fecha_instalacion"],
                errors="coerce"
            )
        else:
            fecha_inicio = pd.Series(
                pd.NaT,
                index=dim_sensor.index
            )

        dim_sensor["fecha_inicio"] = fecha_inicio.fillna(
            lecturas["timestamp"].min()
        )

        dim_sensor["fecha_fin"] = pd.NaT
        dim_sensor["es_actual"] = True

    # Preparar fechas y eliminar versiones duplicadas.

    dim_sensor["fecha_inicio"] = pd.to_datetime(
        dim_sensor["fecha_inicio"],
        errors="coerce"
    )

    dim_sensor["fecha_fin"] = pd.to_datetime(
        dim_sensor["fecha_fin"],
        errors="coerce"
    )

    dim_sensor = dim_sensor.dropna(
        subset=["sensor_id", "fecha_inicio"]
    ).copy()

    dim_sensor = (
        dim_sensor
        .sort_values(["sensor_id", "fecha_inicio"])
        .drop_duplicates(
            subset=["sensor_id", "fecha_inicio"],
            keep="last"
        )
        .reset_index(drop=True)
    )

    dim_sensor.insert(
        0,
        "sensor_sk",
        range(1, len(dim_sensor) + 1)
    )

    guardar_parquet(
        dim_sensor,
        "dim_sensor.parquet"
    )

    # --------------------------------------------------------
    # 6. DIMENSION TIEMPO
    # --------------------------------------------------------

    timestamps = (
        lecturas[["timestamp"]]
        .drop_duplicates()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    timestamps.insert(
        0,
        "tiempo_sk",
        range(1, len(timestamps) + 1)
    )

    timestamps["fecha"] = timestamps["timestamp"].dt.date
    timestamps["anio"] = timestamps["timestamp"].dt.year
    timestamps["mes"] = timestamps["timestamp"].dt.month
    timestamps["dia"] = timestamps["timestamp"].dt.day
    timestamps["hora"] = timestamps["timestamp"].dt.hour
    timestamps["minuto"] = timestamps["timestamp"].dt.minute
    timestamps["dia_semana"] = timestamps["timestamp"].dt.dayofweek

    timestamps["es_fin_de_semana"] = (
        timestamps["dia_semana"] >= 5
    )

    guardar_parquet(
        timestamps,
        "dim_tiempo.parquet"
    )

    # --------------------------------------------------------
    # 7. TABLA DE HECHOS
    # Granularidad: una lectura por sensor y timestamp.
    # --------------------------------------------------------

    hechos = lecturas.copy()

    historia_para_cruce = dim_sensor[
        [
            "sensor_id",
            "sensor_sk",
            "fecha_inicio",
            "fecha_fin",
        ]
    ].copy()

    asignaciones = []

    # Se procesa cada sensor por separado para que el cruce
    # temporal no duplique la columna sensor_id.

    for sensor_id, grupo in hechos.groupby(
        "sensor_id",
        sort=False
    ):

        versiones = historia_para_cruce[
            historia_para_cruce["sensor_id"] == sensor_id
        ].sort_values("fecha_inicio")

        grupo = grupo.sort_values("timestamp").copy()

        if versiones.empty:

            grupo["sensor_sk"] = pd.NA

        else:

            # Solo se incluyen las columnas necesarias para el
            # cruce: no se incorpora un segundo sensor_id.

            versiones_cruce = versiones[
                [
                    "sensor_sk",
                    "fecha_inicio",
                    "fecha_fin",
                ]
            ].copy()

            grupo = pd.merge_asof(
                grupo.sort_values("timestamp"),
                versiones_cruce.sort_values("fecha_inicio"),
                left_on="timestamp",
                right_on="fecha_inicio",
                direction="backward"
            )

            # Si la lectura es anterior a la primera version,
            # asignar la primera version disponible.

            sin_version = grupo["sensor_sk"].isna()

            if sin_version.any():
                primera_sk = versiones.iloc[0]["sensor_sk"]

                grupo.loc[
                    sin_version,
                    "sensor_sk"
                ] = primera_sk

            # Validar el periodo de vigencia de la version.

            fuera_vigencia = (
                grupo["fecha_fin"].notna()
                & (
                    grupo["timestamp"]
                    >= grupo["fecha_fin"]
                )
            )

            if fuera_vigencia.any():

                versiones_actuales = versiones[
                    versiones["fecha_fin"].isna()
                ]

                if not versiones_actuales.empty:

                    sk_actual = versiones_actuales.iloc[-1][
                        "sensor_sk"
                    ]

                    grupo.loc[
                        fuera_vigencia,
                        "sensor_sk"
                    ] = sk_actual

        asignaciones.append(grupo)

    hechos = pd.concat(
        asignaciones,
        ignore_index=True
    )

    # Retirar columnas auxiliares del cruce historico.

    hechos = hechos.drop(
        columns=["fecha_inicio", "fecha_fin"],
        errors="ignore"
    )

    # Añadir clave de tiempo.

    hechos = hechos.merge(
        timestamps[["timestamp", "tiempo_sk"]],
        on="timestamp",
        how="left",
        validate="many_to_one"
    )

    # Añadir claves de turbina y central.
    # Primero se eliminan posibles columnas de central_id
    # que ya vengan desde Silver.

    hechos = hechos.drop(
        columns=["central_id", "turbina_sk", "central_sk"],
        errors="ignore"
    )

    hechos = hechos.merge(
        dim_turbina[
            [
                "turbina_id",
                "turbina_sk",
                "central_id",
                "central_sk",
            ]
        ],
        on="turbina_id",
        how="left",
        validate="many_to_one"
    )

    # Seleccionar columnas finales.

    columnas_hechos = [
        "lectura_id",
        "timestamp",
        "fecha",
        "sensor_id",
        "sensor_sk",
        "tiempo_sk",
        "turbina_id",
        "turbina_sk",
        "central_id",
        "central_sk",
        "temperatura",
        "presion",
        "vibracion",
    ]

    hechos = hechos[
        [c for c in columnas_hechos if c in hechos.columns]
    ].copy()

    # Medida aditiva para contar lecturas.

    hechos["cantidad_lecturas"] = 1

    # Indicadores de disponibilidad de las metricas.

    for metrica in [
        "temperatura",
        "presion",
        "vibracion",
    ]:

        if metrica in hechos.columns:
            hechos[f"{metrica}_disponible"] = (
                hechos[metrica].notna()
            )

    guardar_parquet(
        hechos,
        "fact_lecturas.parquet"
    )

    # --------------------------------------------------------
    # 8. VALIDACIONES
    # --------------------------------------------------------

    print("\n" + "=" * 60)
    print("VALIDACIONES GOLD")
    print("=" * 60)

    print(f"Dimension central: {len(dim_central):,}")
    print(f"Dimension turbina: {len(dim_turbina):,}")
    print(f"Dimension sensor: {len(dim_sensor):,}")
    print(f"Dimension tiempo: {len(timestamps):,}")
    print(f"Tabla de hechos: {len(hechos):,}")

    if {"sensor_id", "timestamp"}.issubset(hechos.columns):

        duplicados = hechos.duplicated(
            subset=["sensor_id", "timestamp"]
        ).sum()

        print(
            "Duplicados sensor_id + timestamp: "
            f"{duplicados:,}"
        )

    for clave in [
        "sensor_sk",
        "tiempo_sk",
        "turbina_sk",
        "central_sk",
    ]:

        if clave in hechos.columns:

            faltantes = hechos[clave].isna().sum()

            print(
                f"Claves {clave} sin asignar: {faltantes:,}"
            )

    print("\nTransformacion Gold finalizada.")


if __name__ == "__main__":
    construir_gold()