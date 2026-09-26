import uuid
from datetime import datetime, timedelta

OUTPUT = "carga_100000.cql"

TOTAL_SENSORES = 20
DIAS = 5
LECTURAS_POR_DIA = 1000

registros = 0

with open(OUTPUT, "w", encoding="utf-8") as f:

    for sensor_num in range(1, TOTAL_SENSORES + 1):

        sensor_id = uuid.UUID(
            f"550e8400-e29b-41d4-a716-44665544{sensor_num:04d}"
        )

        turbina_id = uuid.UUID(
            f"6ba7b810-9dad-11d1-80b4-00c04fd4{sensor_num:04d}"
        )

        for dia in range(DIAS):

            fecha = datetime(2026, 9, 19) + timedelta(days=dia)

            for lectura_num in range(LECTURAS_POR_DIA):

                timestamp = fecha + timedelta(seconds=86 * lectura_num)

                temperatura = 70 + sensor_num * 0.5 + lectura_num * 0.001
                presion = 110 + sensor_num * 0.8 + lectura_num * 0.002
                vibracion = 2.0 + sensor_num * 0.1 + lectura_num * 0.0005

                f.write(
                    "INSERT INTO epm_iot.lecturas_sensor "
                    "(sensor_id, turbina_id, date, timestamp, "
                    "temperatura, presion, vibracion) VALUES "
                    f"({sensor_id}, {turbina_id}, "
                    f"'{fecha.strftime('%Y-%m-%d')}', "
                    f"'{timestamp.strftime('%Y-%m-%d %H:%M:%S')}', "
                    f"{temperatura:.2f}, {presion:.2f}, {vibracion:.3f});\n"
                )

                registros += 1

print(f"Archivo generado: {OUTPUT}")
print(f"Registros generados: {registros}")
print(f"Sensores: {TOTAL_SENSORES}")
print(f"Días: {DIAS}")