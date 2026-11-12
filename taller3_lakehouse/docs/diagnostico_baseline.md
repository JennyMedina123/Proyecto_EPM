# Diagnóstico baseline

Proyecto IoT Industrial EPM · Mini-Lakehouse · capa Gold en Parquet

| Parámetro | Valor |
|---|---|
| Motor | DuckDB 2.0.0 sobre archivos Parquet |
| Tabla de hechos | `lakehouse/gold/fact_lecturas_hora.parquet` — 16.872 filas (80 sensores × 211 horas), 371 KB |
| Rango temporal | 211 horas, del 17 al 26 de octubre de 2026 (10 días) — `verificar_fechas_taller4.py` |
| Organización física | Ordenada por `tiempo_sk`, **1 solo row group** (`row_group_size=100_000` > 16.872 filas) |
| Medición | 3 calentamientos + 5 ejecuciones por ronda, 3 rondas con orden rotado; mediana de 15 mediciones |
| Evidencia | `docs/performance/plan_baseline_*.txt`, `docs/performance/benchmark_resumen.csv` |

## Consultas analizadas y su origen en el M3

Las tres consultas provienen de las consultas analíticas del M3 (`scripts/analyze.py`). Q2 es idéntica. A Q1 y Q3 se les añadió un filtro temporal que representa su uso real (reporte de un mes, rango de días); Q1 además omite la columna de temperatura promedio y el `LIMIT 5`.

| Consulta | Origen en el M3 | Cambio respecto al M3 |
|---|---|---|
| Q1_Alertas_Mes | Q1 de `analyze.py` | + `WHERE t.mes = 10 AND t.anio = 2026`; sin `temp_promedio_mensual` ni `LIMIT 5` |
| Q2_Comportamiento_Termico | Q2 de `analyze.py` | Ninguno |
| Q3_Dias_Inestabilidad | Q3 de `analyze.py` | + `WHERE t.fecha >= DATE '2026-10-17' AND t.fecha <= DATE '2026-10-26'` |

Con los datos de prueba esos filtros abarcan todo el rango disponible; ese hecho es parte central del diagnóstico.

## Revisión de los 5 indicadores

| Indicador | ¿Aparece? | Evidencia en los planes |
|---|---|---|
| 1. Acceso secuencial en tabla grande | **Sí, en Q1, Q2 y Q3** | El TABLE_SCAN de la fact emite 16.872 filas; las consultas devuelven 20, 4 y 5 |
| 2. Cardinalidad estimada muy alejada de la real | **Sí en Q2, sin efecto en el plan** | En el scan de la fact, estimadas = reales = 16.872 en Q1, Q2 y Q3. En Q2 el HASH_GROUP_BY estimó 80 grupos y produjo 4 ubicaciones (20×, sobre el umbral de 10×); no alteró ninguna decisión, porque Hash Group By es el algoritmo adecuado para cualquier número de grupos. En la consulta sonda Q3S el scan estimó 16.872 filas y emitió 1.040 (16×): el optimizador no anticipa el filtro dinámico que llega desde el join. Fuente: perfil JSON de DuckDB |
| 3. Algoritmo de JOIN ineficiente | **No** | Todos los joins son HASH_JOIN con la dimensión como lado de construcción; no hay NESTED_LOOP_JOIN ni CROSS_PRODUCT |
| 4. Data skipping ausente | **Sí, en Q1 y Q3** | `Row Groups Scanned: 1 / 1`. Con un único row group cuyas estadísticas cubren `tiempo_sk` 1–211, ningún filtro temporal puede descartar datos. Q3 (10 días) y Q3S (1 día) leen los mismos 149.089 bytes |
| 5. Conversión implícita de tipos | **No** | `anio` y `mes` son enteros comparados con enteros; `fecha` es DATE comparada con `DATE '...'`. Las conversiones `CAST` / `__internal_compress_integral` del plan son compresiones internas de DuckDB, no conversiones sobre la columna filtrada |

---

## Q1 — Alertas críticas por turbina y mes

| Campo | Valor |
|---|---|
| Nombre de la query | Q1_Alertas_Mes — total de alertas críticas por turbina en octubre de 2026 |
| Tiempo baseline (mediana) | **11,35 ms** |
| Motor usado | DuckDB 2.0.0 sobre Parquet |
| Operador más costoso | **HASH_JOIN** entre la fact pre-agregada y `dim_tiempo`: **10,0 ms de 17,1 ms** del plan (el resto de operadores marca 0 µs). DuckDB 2.0 aplica *partial aggregate pushdown*: agrupa la fact por (`tiempo_sk`, turbina) antes del join (16.872 → 4.218 filas) |
| Problema identificado | **Acceso secuencial completo por un filtro no selectivo.** Las 211 filas de `dim_tiempo` cumplen `mes = 10 AND anio = 2026`; DuckDB lo detecta con las estadísticas min/max y elimina el filtro (el scan de `dim_tiempo` no muestra filtro sobre `mes` ni `anio`). Como la fact pre-agregada es el lado de construcción del join, el filtro dinámico de tiempo (`tiempo_sk >= 1 AND tiempo_sk <= 211`, el rango completo) viaja hacia `dim_tiempo` y no hacia la fact, que solo recibe un filtro por `turbina_sk`: ningún predicado temporal llega a la fact, y con un único row group se leen las 16.872 filas para devolver 20 |
| Indicador que lo revela | **Indicador 1** (16.872 filas leídas → 20 devueltas), con causa estructural en el **indicador 4** (`Row Groups Scanned: 1 / 1`) |

## Q2 — Comportamiento térmico por ubicación del sensor

| Campo | Valor |
|---|---|
| Nombre de la query | Q2_Comportamiento_Termico — temperatura máxima histórica y promedio por ubicación |
| Tiempo baseline (mediana) | **7,92 ms** |
| Motor usado | DuckDB 2.0.0 sobre Parquet |
| Operador más costoso | **TABLE_SCAN** de la fact. En el plan de texto todos los operadores marcan 0 µs (por debajo de la resolución del formato), por lo que el criterio es el trabajo realizado: es el único operador que lee datos de disco (182,7 KB, columnas `sensor_sk`, `temperatura_max`, `temperatura_promedio`) y el que produce más filas (16.872); en el perfil JSON detallado es también el de mayor tiempo (~1 ms) |
| Problema identificado | **Acceso secuencial completo inherente a la consulta.** Q2 no tiene WHERE: calcula un máximo y un promedio sobre toda la historia. Leer el 100 % de la fact es lo que la consulta pide, no una ineficiencia del motor. El HASH_JOIN con `dim_sensor` (80 filas) es el algoritmo correcto |
| Indicador que lo revela | **Indicador 1** (16.872 leídas → 4 devueltas); el **indicador 2** aparece en el HASH_GROUP_BY (80 grupos estimados, 4 reales) sin efecto en el plan. **Ninguna intervención del catálogo aplica**: I-1 a I-6 reducen lecturas cuando existe un filtro. La solución es de diseño (pre-agregación) y se trata en la reflexión arquitectural |

## Q3 — Días con mayor inestabilidad

| Campo | Valor |
|---|---|
| Nombre de la query | Q3_Dias_Inestabilidad — 5 días con más alertas críticas y su vibración media |
| Tiempo baseline (mediana) | **5,83 ms** |
| Motor usado | DuckDB 2.0.0 sobre Parquet |
| Operador más costoso | **TABLE_SCAN** de la fact, con el mismo criterio que Q2: todos los operadores marcan 0 µs; es el único que lee datos (149,0 KB) y el que produce más filas (16.872). El resto del plan es eficiente: PERFECT_HASH_GROUP_BY (10 fechas) y TOP_N en lugar de ordenar todo |
| Problema identificado | **Mismo patrón que Q1.** El rango `2026-10-17` a `2026-10-26` coincide con todo el contenido de `dim_tiempo`; el filtro se elimina (el scan de `dim_tiempo` lee 211 filas sin filtro sobre `fecha`) y la fact se lee completa porque tiene un único row group. Contraste con la sonda Q3S (un día): allí el plan sí muestra `Filters: fecha = '2026-10-26'::DATE` en `dim_tiempo` (13 filas) y la fact emite solo 1.040 filas, pero aun así lee los mismos 149,0 KB con `Row Groups Scanned: 1 / 1` |
| Indicador que lo revela | **Indicador 1** (16.872 leídas → 5 devueltas), con causa en el **indicador 4** (`Row Groups Scanned: 1 / 1`) |

---

## Observación transversal: costo fijo del motor

En Q2 y Q3 el plan reporta 8,3 ms y 6,4 ms de tiempo total, pero todos sus operadores marcan 0 µs: casi todo el tiempo transcurre fuera de los operadores (análisis del SQL, planificación, apertura de archivos y coordinación de hilos). En Q1, el único operador visible (HASH_JOIN, 10,0 ms) explica algo más de la mitad del total. Con 371 KB de datos residentes en la caché del sistema operativo, la latencia está dominada por costos fijos (análisis, planificación, coordinación de hilos) y no por la lectura de datos. Esto anticipa que una intervención física tendrá efecto medible en I/O pero no necesariamente en tiempo.

## Conclusión y decisión de intervención

| Query | Problema | Indicador | Intervención |
|---|---|---|---|
| Q1 | Scan completo; filtro no selectivo; 1 row group | 1 (causa: 4) | **I-3** — ordenar por `tiempo_sk` con row groups pequeños |
| Q2 | Scan completo inherente (sin WHERE) | 1 (y 2, sin efecto) | Ninguna del catálogo; se justifica y se discute en la reflexión |
| Q3 | Scan completo; filtro no selectivo; 1 row group | 1 (causa: 4) | **I-3** — misma intervención que Q1 |

Como los filtros de Q1 y Q3 abarcan todos los datos existentes, **se espera que I-3 no reduzca sus lecturas**. Para verificar que la técnica funciona cuando el filtro sí es selectivo, se incluye la consulta sonda **Q3S** (Q3 restringida al 26 de octubre). Se descartó la reescritura con CTE de la entrega anterior: no corresponde a ningún indicador y su plan resultó idéntico al original.
