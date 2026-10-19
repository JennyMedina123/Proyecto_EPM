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
| Silver | Jenny Elizabeth Medina | Implementado y validado | `transform_silver.py` |
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
├── lakehouse/
│   ├── bronze/               datos crudos en Parquet
│   └── silver/               datos limpios, cuarentenas e informes
├── oltp/                     base operacional simulada
├── scripts/
│   ├── config.py             rutas y parámetros compartidos
│   ├── utilidades.py         funciones comunes
│   ├── generar_oltp.py       simulador de sensores
│   ├── extract.py            OLTP -> Bronze
│   ├── verificar_bronze.py   controles de calidad de Bronze
│   └── transform_silver.py  transformación Bronze -> Silver
├── docs/
│   └── perfil_bronze.txt     perfil de calidad de Bronze
├── main.py                  ejecuta el pipeline
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

## Zona Silver

**Responsable:** Jenny Elizabeth Medina.

### Objetivo

La capa Silver transforma las lecturas provenientes de Bronze para mejorar su calidad, normalizar los datos y separar los registros que no cumplen las validaciones. Los datos limpios quedan preparados para su posterior utilización en Gold.

### Proceso de transformación

La transformación se implementó en `scripts/transform_silver.py` y realiza las siguientes operaciones:

1. **Lectura de Bronze:** carga los archivos Parquet de lecturas y el catálogo de sensores más reciente.
2. **Normalización de identificadores:** elimina espacios al inicio y al final de `sensor_id` y convierte el identificador a minúsculas.
3. **Normalización de fechas:** convierte `timestamp` a un tipo de fecha y hora, normalizando los formatos que contienen `T`. La columna `fecha` se deriva de la fecha de la lectura.
4. **Limpieza de mediciones:** convierte los valores centinela `-999` en valores nulos, sin imputar los datos faltantes.
5. **Validación de rangos:** revisa las mediciones de temperatura, presión y vibración. Los valores fuera de los rangos definidos se convierten en nulos, conservando las filas.
6. **Cuarentena por calidad:** separa los registros con identificador de sensor vacío o fecha y hora inválida.
7. **Cuarentena de sensores:** separa las lecturas cuyo `sensor_id` no aparece en el catálogo de sensores.
8. **Eliminación de duplicados:** identifica duplicados mediante `(sensor_id, timestamp)` y conserva el primer registro según el orden de `lectura_id`.
9. **Generación de informes:** registra los indicadores de calidad y las estadísticas de las mediciones.

### Archivos generados

Los resultados se almacenan en `lakehouse/silver/`:

| Archivo | Descripción |
|---|---|
| `lecturas_silver.parquet` | Lecturas limpias y transformadas. |
| `cuarentena_sensores.parquet` | Registros asociados a sensores desconocidos. |
| `cuarentena_calidad.parquet` | Registros con identificador o fecha y hora inválida. |
| `informe_calidad.csv` | Indicadores de calidad y resultados de la transformación. |
| `estadisticas_silver.csv` | Valores válidos, promedio, mínimo y máximo de cada medición. |

### Resultados de la ejecución validada

En la ejecución comprobada se obtuvieron los siguientes resultados:

| Indicador | Resultado |
|---|---:|
| Filas originales en Bronze | 92.377 |
| Filas resultantes en Silver | 92.085 |
| Duplicados eliminados | 267 |
| Sensores desconocidos en cuarentena | 25 |
| Registros en cuarentena por clave o fecha inválida | 0 |
| Duplicados restantes por sensor e instante | 0 |
| Valores `-999` restantes en Silver | 0 |

Los valores centinela se reemplazan por nulos y las filas se conservan. En esta ejecución no se encontraron mediciones fuera de los rangos configurados.

Las estadísticas de temperatura, presión y vibración están disponibles en `estadisticas_silver.csv`. Los resultados corresponden a la ejecución validada y pueden variar si se generan nuevos datos en Bronze.

### Ejecución individual

Desde la carpeta `taller3_lakehouse`, ejecutar:

```bash
python scripts/transform_silver.py
```

El script genera los archivos Parquet y CSV descritos anteriormente.

### Integración con el pipeline

El archivo `main.py` incluye el paso `Limpieza -> Silver`, que ejecuta `scripts/transform_silver.py` después de la verificación de Bronze. La integración completa con Gold y el análisis queda pendiente de los respectivos scripts.


## Integrantes

- Jenny Elizabeth Medina
- Andres Valderrama Acero
- Breiner Andres Herrera Rendón
- Juan Camilo Gutierrez Medina

## Docente

Roberto Carlos Rahamut Suteu — ITM 2026