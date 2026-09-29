"""
generar_datos.py  |  Simulación de telemetría EPM para PostgreSQL
Genera 100.000 lecturas: 20 sensores x 5 días x 1.000 lecturas/día
Salida (carpeta ./data, montada en el contenedor como /data):
  - sensores.csv  -> tabla sensor
  - lecturas.csv  -> tabla lecturas_sensor (se carga con COPY)
Uso:  python generar_datos.py
Sólo usa la librería estándar de Python.
"""
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

SENSORES = 20
DIAS = 5
LECTURAS_POR_DIA = 1000
FECHA_INICIO = datetime(2026, 9, 1, tzinfo=timezone(timedelta(hours=-5)))  # hora Colombia
TURBINAS = [f"a0000000-0000-0000-0000-00000000000{i}" for i in range(1, 7)]

random.seed(42)
salida = Path(__file__).parent / "data"
salida.mkdir(exist_ok=True)

sensores = [
    (f"b0000000-0000-0000-0000-{i:012d}", TURBINAS[(i - 1) % len(TURBINAS)], f"SEN-{i:03d}")
    for i in range(1, SENSORES + 1)
]

with open(salida / "sensores.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["sensor_id", "turbina_id", "codigo"])
    w.writerows(sensores)

intervalo = timedelta(seconds=86400 / LECTURAS_POR_DIA)  # 86,4 s entre lecturas
total = 0
with open(salida / "lecturas.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["sensor_id", "ts", "temperatura", "presion", "vibracion"])
    for sensor_id, _, _ in sensores:
        for d in range(DIAS):
            inicio_dia = FECHA_INICIO + timedelta(days=d)
            for n in range(LECTURAS_POR_DIA):
                ts = inicio_dia + n * intervalo
                vib = random.gauss(3.0, 0.8)
                if random.random() < 0.01:          # 1 % de picos (posibles alertas)
                    vib += random.uniform(4, 6)
                w.writerow([
                    sensor_id,
                    ts.isoformat(timespec="milliseconds"),
                    round(random.gauss(60, 4), 2),
                    round(random.gauss(12, 0.8), 2),
                    round(max(vib, 0), 3),
                ])
                total += 1

print(f"Sensores generados : {len(sensores)}")
print(f"Lecturas generadas : {total:,}".replace(",", "."))
print(f"Particiones lógicas (sensor, día): {SENSORES * DIAS}")
print(f"Archivos en        : {salida.resolve()}")
