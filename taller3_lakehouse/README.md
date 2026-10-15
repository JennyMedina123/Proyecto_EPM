# Telemetría IoT EPM — Mini-Lakehouse

Taller 3 · Tópicos Avanzados en Bases de Datos · ITM 2026

## Descripción

Capa analítica de la Arquitectura Lambda propuesta para EPM (Hidroituango, Guatapé y Porce III).
Un simulador de sensores IoT alimenta una base operacional (OLTP); sus datos se copian a un lakehouse
con zonas Bronze / Silver / Gold en Parquet, consultado con DuckDB.

## Estado del proyecto

| Zona | Responsable | Estado | Scripts |
|---|---|---|---|
| Simulación OLTP + Bronze | Juan Camilo Gutierrez Medina | Listo | `generar_oltp.py`, `extract.py`, `verificar_bronze.py` |
| Silver | (pendiente) | Pendiente | `transform_silver.py` |
| Gold | (pendiente) | Pendiente | `transform_gold.py` |
| Análisis y benchmark | (pendiente) | Pendiente | `analyze.py` |

## Requisitos

- Python 3.10 o superior
- No necesita ninguna base de datos instalada: la fuente operacional se simula en SQLite.

## Cómo ejecutar

```bash
git clone https://github.com/<usuario>/<repositorio>.git
cd <repositorio>
python -m venv .venv
.venv\Scripts\activate          # Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Cada ejecución de `python main.py`:
1. Simula que los sensores envían 6 horas nuevas de lecturas aleatorias (la primera vez, 3 días de historia).
2. Extrae a Bronze solo las lecturas nuevas.
3. Verifica Bronze y actualiza `docs/perfil_bronze.txt`.
4. Ejecuta Silver, Gold y análisis cuando sus scripts existan.

Otros comandos útiles:

| Comando | Qué hace |
|---|---|
| `python main.py --reset` | Borra `lakehouse/` y la base simulada; empieza de cero |
| `python scripts/generar_oltp.py --horas 24` | Agrega 24 horas de lecturas |
| `python scripts/generar_oltp.py --en-vivo 30` | Durante 30 s llega una lectura por sensor cada 2 s |
| `python scripts/verificar_bronze.py` | Controles y perfil de calidad de Bronze |

Parámetros (variables de entorno): `EPM_FRECUENCIA_MIN` (5), `EPM_DIAS_INICIALES` (3), `EPM_HORAS` (6),
`EPM_SEMILLA` (vacío = aleatorio).

## Estructura

```
├── lakehouse/bronze/        datos crudos en Parquet (se sube al repositorio)
├── oltp/                    base operacional simulada (NO se sube; se genera)
├── scripts/
│   ├── config.py            rutas, conexión y parámetros compartidos
│   ├── utilidades.py        funciones comunes
│   ├── generar_oltp.py      simulador de sensores (fuente OLTP)
│   ├── extract.py           OLTP -> Bronze
│   └── verificar_bronze.py  controles y perfil de calidad
├── docs/perfil_bronze.txt   problemas de calidad medidos en Bronze
├── main.py                  ejecuta todo el pipeline
└── requirements.txt
```

## Zona Bronze

**Fuente simulada:** 3 centrales, 20 turbinas y 80 sensores; 1 lectura cada 5 minutos por sensor.
Mismas tablas del entregable anterior: `central -> turbina -> sensor`, `sensor_auditoria` (historial de
cambios de configuración) y `lecturas_sensor` (estructura de la tabla de Cassandra).

**Reglas de Bronze:**
- **Copia tal cual:** no se limpia, no se cambian tipos, no se corrige nada.
- **Inmutable:** un archivo escrito nunca se modifica ni se sobrescribe.
- **Incremental:** marca de agua sobre `lectura_id`; cada extracción copia solo lo nuevo.
- **Auditable:** cada fila lleva `_extraido_en`, `_fuente` y `_lote`; cada archivo queda en
  `lakehouse/bronze/_control_extracciones.csv`.

**Archivos:**

| Ruta | Contenido |
|---|---|
| `lakehouse/bronze/lecturas_sensor/fecha=AAAA-MM-DD/lecturas_<lote>.parquet` | Lecturas, una carpeta por día, un archivo por lote |
| `lakehouse/bronze/catalogo/<tabla>/<tabla>_<lote>.parquet` | Foto completa del catálogo en cada extracción |
| `lakehouse/bronze/_control_extracciones.csv` | Bitácora de extracciones |

## Para quien construye Silver

**Leer las lecturas de Bronze:**

```python
import duckdb
df = duckdb.sql("""
    SELECT * FROM read_parquet('lakehouse/bronze/lecturas_sensor/*/*.parquet',
                               hive_partitioning = true)
""").df()
```

**Leer el catálogo:** usar el archivo más reciente de cada carpeta (el nombre termina en el lote
`AAAAMMDD_HHMMSS`, así que el último en orden alfabético es el más nuevo).

**Esquema de `lecturas_sensor`:**

| Columna | Tipo en Bronze | Nota |
|---|---|---|
| `lectura_id` | BIGINT | Único por lectura recibida (los reenvíos tienen otro id) |
| `sensor_id` | VARCHAR | UUID; a veces en MAYÚSCULAS o con espacios al final |
| `turbina_id` | VARCHAR | UUID de la turbina |
| `date` | VARCHAR | Día de la lectura, `AAAA-MM-DD` |
| `timestamp` | VARCHAR | Texto en dos formatos: `AAAA-MM-DD HH:MM:SS` y `AAAA-MM-DDTHH:MM:SS` |
| `temperatura` | DOUBLE | °C; puede ser NULL o -999 |
| `presion` | DOUBLE | bar; puede ser NULL |
| `vibracion` | DOUBLE | mm/s; puede ser NULL o -999 |
| `_extraido_en`, `_fuente`, `_lote` | auditoría | Agregadas por Bronze |
| `fecha` | DATE | Viene del nombre de la carpeta (partición Hive) |

**Problemas que Silver debe resolver** (cifras actualizadas en `docs/perfil_bronze.txt`):

| Problema | Aprox. | Sugerencia |
|---|---|---|
| `timestamp` como texto con dos formatos | 3 % con `T` | `replace(timestamp, 'T', ' ')` y convertir a TIMESTAMP |
| `sensor_id` en mayúsculas o con espacios | 2,5 % | `lower(trim(sensor_id))` |
| Valores -999 (error del datalogger) | 0,15 % | Convertir a NULL |
| Nulos en las métricas | 0,4 % | No imputar; dejar NULL |
| Duplicados (mismo sensor e instante) | 0,3 % | Conservar uno por `(sensor_id, timestamp)` |
| Sensor que no está en el catálogo | 0,03 % | Apartar en una cuarentena |

**Para Gold:** `sensor_auditoria` guarda cada cambio de firmware, umbral o equipo con su
`fecha_cambio`. Sirve para construir `DIM_SENSOR` como SCD Tipo 2.

**Para que `main.py` ejecute Silver automáticamente:** crear `scripts/transform_silver.py`, leer de
`config.BRONZE` y escribir en `config.SILVER`.

## Integrantes

- Jenny Elizabeth Medina
- Andres Valderrama Acero
- Breiner Andres Herrera Rendón
- Juan Camilo Gutierrez Medina

## Docente

Roberto Carlos Rahamut Suteu — ITM 2026