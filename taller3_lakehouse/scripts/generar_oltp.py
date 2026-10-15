import argparse
import sys
import time
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as cfg  # noqa: E402

rng = np.random.default_rng(cfg.SEMILLA)


def uuid_det(texto: str) -> str:
    """UUID determinístico: el mismo código siempre produce el mismo UUID."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"epm.{texto}"))



CENTRALES = [
    # id,  nombre,         municipio,     rio,     MW,   n_turbinas, tipo,     presion_base (bar)
    ("HIT", "Hidroituango", "Ituango",    "Cauca", 2400, 8, "Francis", 19.5),
    ("GUA", "Guatapé",      "San Rafael", "Nare",   560, 8, "Pelton",  82.0),
    ("POR", "Porce III",    "Amalfi",     "Porce",  700, 4, "Francis", 15.0),
]
PRESION_BASE = {c[0]: c[7] for c in CENTRALES}

UBICACIONES = {
    # ubicación:                (temp_base °C, vib_base mm/s, umbral_temp °C)
    "Cojinete guía superior": (55.0, 2.6, 80.0),
    "Cojinete de empuje":     (58.0, 2.9, 80.0),
    "Rodete":                 (38.0, 3.2, 70.0),
    "Generador":              (66.0, 2.2, 95.0),
}
UMBRAL_VIB_INICIAL = 7.1
DIAS_DEGRADACION = 20

DDL = [
    """CREATE TABLE IF NOT EXISTS central (
        central_id VARCHAR(10) PRIMARY KEY, nombre VARCHAR(60), municipio VARCHAR(60),
        departamento VARCHAR(60), rio VARCHAR(60), capacidad_mw DOUBLE PRECISION)""",
    """CREATE TABLE IF NOT EXISTS turbina (
        turbina_id VARCHAR(36) PRIMARY KEY, codigo VARCHAR(20), central_id VARCHAR(10) REFERENCES central,
        tipo VARCHAR(20), capacidad_mw DOUBLE PRECISION, fecha_puesta_servicio VARCHAR(10))""",
    """CREATE TABLE IF NOT EXISTS sensor (
        sensor_id VARCHAR(36) PRIMARY KEY, codigo VARCHAR(20), turbina_id VARCHAR(36) REFERENCES turbina,
        ubicacion VARCHAR(40), fabricante VARCHAR(40), modelo VARCHAR(40), firmware VARCHAR(10),
        umbral_vibracion DOUBLE PRECISION, umbral_temperatura DOUBLE PRECISION,
        fecha_instalacion VARCHAR(10), actualizado_en VARCHAR(19))""",
    """CREATE TABLE IF NOT EXISTS sensor_auditoria (
        auditoria_id INTEGER PRIMARY KEY, sensor_id VARCHAR(36), fabricante VARCHAR(40),
        modelo VARCHAR(40), firmware VARCHAR(10), umbral_vibracion DOUBLE PRECISION,
        umbral_temperatura DOUBLE PRECISION, fecha_cambio VARCHAR(19), tipo_cambio VARCHAR(20))""",
    """CREATE TABLE IF NOT EXISTS lecturas_sensor (
        lectura_id BIGINT PRIMARY KEY, sensor_id VARCHAR(40), turbina_id VARCHAR(36),
        date VARCHAR(10), timestamp VARCHAR(19), temperatura DOUBLE PRECISION,
        presion DOUBLE PRECISION, vibracion DOUBLE PRECISION)""",
    "CREATE INDEX IF NOT EXISTS idx_lect_sensor_ts ON lecturas_sensor (sensor_id, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_lect_date ON lecturas_sensor (date)",
]
TABLAS = ["lecturas_sensor", "sensor_auditoria", "sensor", "turbina", "central"]


def leer(con, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, con)


def insertar(con, tabla: str, df: pd.DataFrame):
    extra = {"method": "multi", "chunksize": 5_000} if cfg.dialecto_oltp() == "postgresql" \
        else {"chunksize": 20_000}
    df.to_sql(tabla, con, if_exists="append", index=False, **extra)


def crear_catalogo(con, inicio: pd.Timestamp):
    centrales, turbinas, sensores = [], [], []
    for cid, nombre, muni, rio, mw, n_turb, tipo, _ in CENTRALES:
        centrales.append({"central_id": cid, "nombre": nombre, "municipio": muni,
                          "departamento": "Antioquia", "rio": rio, "capacidad_mw": mw})
        for t in range(1, n_turb + 1):
            tcod = f"{cid}-T{t}"
            turbinas.append({"turbina_id": uuid_det(tcod), "codigo": tcod, "central_id": cid,
                             "tipo": tipo, "capacidad_mw": round(mw / n_turb, 1),
                             "fecha_puesta_servicio": f"{2011 + t % 9}-0{1 + t % 9}-15"})
            for s, (ubic, (_, _, umbral_t)) in enumerate(UBICACIONES.items(), start=1):
                scod = f"{tcod}-S{s}"
                sensores.append({"sensor_id": uuid_det(scod), "codigo": scod,
                                 "turbina_id": uuid_det(tcod), "ubicacion": ubic,
                                 "fabricante": "SKF", "modelo": "CMSS 2200", "firmware": "v2.1",
                                 "umbral_vibracion": UMBRAL_VIB_INICIAL, "umbral_temperatura": umbral_t,
                                 "fecha_instalacion": "2024-01-15",
                                 "actualizado_en": inicio.strftime("%Y-%m-%d %H:%M:%S")})
    sensores = pd.DataFrame(sensores)
    auditoria = sensores[["sensor_id", "fabricante", "modelo", "firmware",
                          "umbral_vibracion", "umbral_temperatura"]].copy()
    auditoria.insert(0, "auditoria_id", range(1, len(auditoria) + 1))
    auditoria["fecha_cambio"] = "2024-01-15 00:00:00"
    auditoria["tipo_cambio"] = "ALTA"

    insertar(con, "central", pd.DataFrame(centrales))
    insertar(con, "turbina", pd.DataFrame(turbinas))
    insertar(con, "sensor", sensores)
    insertar(con, "sensor_auditoria", auditoria)
    print(f"  Catálogo creado: {len(centrales)} centrales, {len(turbinas)} turbinas, {len(sensores)} sensores")


def estado_actual(con) -> dict:
    r = leer(con, """SELECT MAX(lectura_id) AS max_id,
                            MIN(replace(timestamp, 'T', ' ')) AS primera,
                            MAX(replace(timestamp, 'T', ' ')) AS ultima
                     FROM lecturas_sensor""").iloc[0]
    if pd.isna(r["max_id"]):
        return {"max_id": 0, "primera": None, "ultima": None}
    return {"max_id": int(r["max_id"]), "primera": pd.Timestamp(r["primera"]),
            "ultima": pd.Timestamp(r["ultima"])}



def simular_valores(s: pd.DataFrame, tiempos: pd.DatetimeIndex, inicio_sim: pd.Timestamp,
                    eventos: bool) -> pd.DataFrame:
    n_t, n_s = len(tiempos), len(s)
    hora = tiempos.hour.values[:, None] + tiempos.minute.values[:, None] / 60
    carga = 0.65 + 0.15 * np.sin((hora - 9) / 24 * 2 * np.pi) + 0.18 * ((hora >= 18) & (hora < 21))

    temp_base = s["ubicacion"].map(lambda u: UBICACIONES[u][0]).values[None, :]
    vib_base = s["ubicacion"].map(lambda u: UBICACIONES[u][1]).values[None, :]
    pres_base = s["central_id"].map(PRESION_BASE).values[None, :]
    off = s["turbina_cod"].map(lambda c: (int(uuid_det(c)[:4], 16) % 100) / 100 - 0.5).values[None, :]

    temperatura = temp_base + 4 * off + 10 * carga + rng.normal(0, 1.2, (n_t, n_s))
    presion = pres_base * (0.97 + 0.05 * carga) + rng.normal(0, 0.25, (n_t, n_s))
    vibracion = vib_base + 0.7 * off + 1.1 * carga + np.abs(rng.normal(0, 0.35, (n_t, n_s)))

    dias = ((tiempos - inicio_sim).total_seconds().values / 86400)[:, None]
    rampa = np.clip(dias / DIAS_DEGRADACION, 0, 1.3)
    es_hit3 = s["turbina_cod"].eq("HIT-T3").values[None, :]
    es_coj = s["ubicacion"].str.startswith("Cojinete").values[None, :]
    vibracion = vibracion + es_hit3 * (1.5 + 2.5 * es_coj) * rampa ** 1.5
    temperatura = temperatura + es_hit3 * es_coj * 10 * rampa ** 1.5

    if eventos and n_t > 12 and rng.random() < 0.25:
        turb = rng.choice(s["turbina_cod"].unique().tolist())
        ini = int(rng.integers(0, n_t - 1))
        dur = int(rng.integers(60, 181) // cfg.FRECUENCIA_MIN)
        filas = slice(ini, min(ini + dur, n_t))
        cols = s["turbina_cod"].eq(turb).values
        vibracion[filas, cols] += rng.uniform(4.5, 7.5, vibracion[filas, cols].shape)
        print(f"  [evento] Cavitación en {turb} desde {tiempos[ini]:%Y-%m-%d %H:%M}")

    return pd.DataFrame({
        "sensor_id": np.tile(s["sensor_id"].values, n_t),
        "turbina_id": np.tile(s["turbina_id"].values, n_t),
        "ts": np.repeat(tiempos.values, n_s),
        "temperatura": temperatura.ravel().round(2),
        "presion": presion.ravel().round(2),
        "vibracion": vibracion.ravel().round(3),
        "_turbina": np.tile(s["turbina_cod"].values, n_t),
    })


def quitar_lecturas(df: pd.DataFrame, eventos: bool) -> pd.DataFrame:
    perdidas = rng.random(len(df)) < 0.005
    if eventos and rng.random() < 0.15:
        turb = rng.choice(df["_turbina"].unique().tolist())
        t0 = pd.Timestamp(rng.choice(df["ts"].unique()))
        horas = int(rng.integers(1, 5))
        perdidas |= (df["_turbina"] == turb) & (df["ts"] >= t0) & (df["ts"] < t0 + pd.Timedelta(hours=horas))
        print(f"  [evento] {turb} fuera de línea {horas} h desde {t0:%Y-%m-%d %H:%M}")
    return df[~perdidas].drop(columns="_turbina").reset_index(drop=True)


def ensuciar(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    for col in ["temperatura", "presion", "vibracion"]:
        df.loc[rng.random(n) < 0.004, col] = np.nan
    df.loc[rng.random(n) < 0.001, "vibracion"] = -999.0
    df.loc[rng.random(n) < 0.0005, "temperatura"] = -999.0

    ts_txt = df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    formato_t = rng.random(n) < 0.03
    ts_txt.loc[formato_t] = df.loc[formato_t, "ts"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["timestamp"] = ts_txt
    df["date"] = df["ts"].dt.strftime("%Y-%m-%d")

    mayus = rng.random(n) < 0.02
    df.loc[mayus, "sensor_id"] = df.loc[mayus, "sensor_id"].str.upper()
    espacios = rng.random(n) < 0.005 
    df.loc[espacios, "sensor_id"] = df.loc[espacios, "sensor_id"] + "  "

    dups = df[rng.random(n) < 0.003]
    huerf = df[rng.random(n) < 0.0003].copy()
    huerf["sensor_id"] = uuid_det("sensor-de-pruebas-no-registrado")
    return pd.concat([df, dups, huerf]).sort_values("ts", kind="stable").reset_index(drop=True)


def cambios_de_configuracion(con, desde: pd.Timestamp, hasta: pd.Timestamp):
    if rng.random() >= 0.30:
        return
    sensores = leer(con, "SELECT * FROM sensor")
    max_aud = int(leer(con, "SELECT MAX(auditoria_id) AS m FROM sensor_auditoria")["m"][0])
    elegidos = sensores.sample(n=int(rng.integers(1, 5)), random_state=int(rng.integers(1_000_000)))
    segundos = max(int((hasta - desde).total_seconds()), 1)
    sentencias, filas = [], []
    for _, s in elegidos.iterrows():
        cuando = (desde + pd.Timedelta(seconds=int(rng.integers(0, segundos)))).floor("min")
        tipo = str(rng.choice(["FIRMWARE", "RECALIBRACION", "REEMPLAZO"]))
        nuevo = {"fabricante": s["fabricante"], "modelo": s["modelo"], "firmware": s["firmware"],
                 "umbral_vibracion": float(s["umbral_vibracion"]),
                 "umbral_temperatura": float(s["umbral_temperatura"])}
        if tipo == "FIRMWARE":
            nuevo["firmware"] = f"v{3 + int(rng.integers(0, 3))}.{int(rng.integers(0, 10))}"
        elif tipo == "RECALIBRACION":
            nuevo["umbral_vibracion"] = float(rng.choice([6.0, 6.5, 7.1, 7.5]))
        else:
            nuevo.update(fabricante="Bently Nevada", modelo="3500/42M", firmware="v1.4")
        max_aud += 1
        filas.append({"auditoria_id": max_aud, "sensor_id": s["sensor_id"], **nuevo,
                      "fecha_cambio": cuando.strftime("%Y-%m-%d %H:%M:%S"), "tipo_cambio": tipo})
        sentencias.append(
            f"UPDATE sensor SET fabricante='{nuevo['fabricante']}', modelo='{nuevo['modelo']}', "
            f"firmware='{nuevo['firmware']}', umbral_vibracion={nuevo['umbral_vibracion']}, "
            f"actualizado_en='{cuando:%Y-%m-%d %H:%M:%S}' WHERE sensor_id='{s['sensor_id']}'")
        print(f"  [cambio] {s['codigo']}: {tipo} el {cuando:%Y-%m-%d %H:%M} -> {nuevo['firmware']}, "
              f"umbral vib {nuevo['umbral_vibracion']}")
    cfg.ejecutar_oltp(con, sentencias)
    insertar(con, "sensor_auditoria", pd.DataFrame(filas))


def generar_bloque(con, desde: pd.Timestamp, pasos: int, inicio_sim: pd.Timestamp,
                   siguiente_id: int, eventos: bool = True) -> pd.DataFrame:
    s = leer(con, """SELECT s.sensor_id, s.turbina_id, s.ubicacion, t.codigo AS turbina_cod, t.central_id
                     FROM sensor s JOIN turbina t ON t.turbina_id = s.turbina_id ORDER BY s.codigo""")
    tiempos = pd.date_range(desde, periods=pasos, freq=f"{cfg.FRECUENCIA_MIN}min")
    df = ensuciar(quitar_lecturas(simular_valores(s, tiempos, inicio_sim, eventos), eventos))
    df.insert(0, "lectura_id", np.arange(siguiente_id, siguiente_id + len(df)))
    df = df[["lectura_id", "sensor_id", "turbina_id", "date", "timestamp",
             "temperatura", "presion", "vibracion"]]
    insertar(con, "lecturas_sensor", df)
    if eventos:
        cambios_de_configuracion(con, tiempos[0], tiempos[-1])
    return df


def modo_en_vivo(con, segundos: int):
    print(f"  Modo en vivo durante {segundos} s (Ctrl+C para detener)")
    fin = time.time() + segundos
    while time.time() < fin:
        e = estado_actual(con)
        desde = e["ultima"] + pd.Timedelta(minutes=cfg.FRECUENCIA_MIN)
        df = generar_bloque(con, desde, 1, e["primera"], e["max_id"] + 1, eventos=False)
        vib = pd.to_numeric(df["vibracion"], errors="coerce")
        print(f"  {desde:%Y-%m-%d %H:%M}  +{len(df):>3} lecturas | vibración máx "
              f"{vib[vib > 0].max():.2f} mm/s")
        time.sleep(2)


def main():
    p = argparse.ArgumentParser(description="Simulador de sensores IoT de EPM")
    p.add_argument("--horas", type=int, default=cfg.HORAS_POR_CORRIDA, help="horas simuladas a agregar")
    p.add_argument("--en-vivo", type=int, metavar="SEGUNDOS", help="lecturas en tiempo real")
    p.add_argument("--reiniciar", action="store_true", help="borra la BD y empieza de cero")
    args = p.parse_args()

    t0 = time.time()
    print(f"Fuente OLTP: {cfg.OLTP_URL}  ({cfg.dialecto_oltp()})")
    con = cfg.conectar_oltp()
    if args.reiniciar:
        cfg.ejecutar_oltp(con, [f"DROP TABLE IF EXISTS {t}" for t in TABLAS])
        print("  Base operacional reiniciada.")
    cfg.ejecutar_oltp(con, DDL)

    e = estado_actual(con)
    if e["ultima"] is None:
        # Primera vez: catálogo + historia inicial que termina "ahora"
        ahora = pd.Timestamp.now().floor(f"{cfg.FRECUENCIA_MIN}min")
        desde = ahora - pd.Timedelta(days=cfg.DIAS_INICIALES)
        if leer(con, "SELECT COUNT(*) AS n FROM sensor")["n"][0] == 0:
            crear_catalogo(con, desde)
        pasos = cfg.DIAS_INICIALES * 24 * cfg.LECTURAS_ESPERADAS_HORA
        inicio_sim = desde
        print(f"  Primera ejecución: historia inicial de {cfg.DIAS_INICIALES} días")
    else:
        desde = e["ultima"] + pd.Timedelta(minutes=cfg.FRECUENCIA_MIN)
        pasos = args.horas * cfg.LECTURAS_ESPERADAS_HORA
        inicio_sim = e["primera"]

    if args.en_vivo and e["ultima"] is not None:
        modo_en_vivo(con, args.en_vivo)
    else:
        df = generar_bloque(con, desde, pasos, inicio_sim, e["max_id"] + 1)
        hasta = desde + pd.Timedelta(minutes=cfg.FRECUENCIA_MIN * (pasos - 1))
        print(f"  Lecturas nuevas: {len(df):,}  ({desde:%Y-%m-%d %H:%M} -> {hasta:%Y-%m-%d %H:%M})")

    total = int(leer(con, "SELECT COUNT(*) AS n FROM lecturas_sensor")["n"][0])
    print(f"  Total en la base operacional: {total:,} lecturas")
    if cfg.dialecto_oltp() == "sqlite":
        con.close()
    print(f"Simulación lista en {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()