# EPM IoT: ¿por qué no una base relacional?

**Laboratorio de comparación con PostgreSQL 16 en Docker**

El informe original implementa la capa HOT en Cassandra y justifica una arquitectura Lambda (Cassandra + ClickHouse + Iceberg). Este laboratorio **no vuelve a montar la parte NoSQL**. Lo que hace es construir el mismo modelo en una base relacional, bien diseñada y con particionamiento, y **medir** qué pasa frente a las restricciones de EPM:

1. **85.000 eventos/s** sostenidos, 24x7.
2. **Alerta temprana < 2 ms**, aunque al mismo tiempo corran consultas históricas.
3. **Retención de 7 años** (CREG) para auditoría y entrenamiento de IA.
4. **Expiración de la capa HOT** a las 24 h sin degradar el motor.

La idea es que PostgreSQL sirva de grupo de control: si incluso con buenas prácticas no cumple, queda demostrado con datos por qué se necesita un motor NoSQL/columnar.

---

## Contenido

```
epm-postgres/
├── docker-compose.yml            # postgres:16, 2 CPU, 4 GB RAM (resultados comparables)
├── generar_datos.py              # 100.000 lecturas simuladas -> data/*.csv
├── sql/
│   ├── 01_esquema.sql            # Parte 1: réplica funcional del informe
│   ├── 02_datos_prueba.sql
│   ├── 03_consultas.sql
│   ├── 04_carga_masiva.sql
│   ├── 05_rendimiento.sql
│   ├── 06_bench_ingesta.sql      # Parte 2: E1 ingesta 85.000 ev/s
│   ├── 08_almacenamiento_analitica.sql   # E4 filas vs columnas, 7 años
│   ├── 09_expiracion_ttl.sql     # E5 DELETE vs DROP (TTL)
│   └── 10_resumen.sql            # Veredicto frente a los requisitos
└── bench/
    ├── 07_concurrencia.sh        # E2 ingesta realista + E3 interferencia (pgbench)
    ├── ingesta_fila.sql  lectura_puntual.sql  analitica.sql
    └── ejecutar_todo.sh          # Corre 01 -> 10 en orden
```

## Ejecución

```bash
docker compose up -d
python generar_datos.py
docker exec -it postgres-epm bash /bench/ejecutar_todo.sh     # ≈ 3-5 minutos
```

También se puede correr cada paso por separado:

```bash
docker exec -it postgres-epm psql -U epm -d epm_iot -f /scripts/06_bench_ingesta.sql
docker exec -it postgres-epm bash /bench/07_concurrencia.sh
```

Todas las métricas quedan en la tabla `bench.resultados`, y `10_resumen.sql` imprime el veredicto.

> Los números de este README salieron de una corrida con 2 CPU y PostgreSQL 16 por defecto. En otro equipo cambian los valores absolutos, pero las proporciones y conclusiones se mantienen.

---

## Parte 1. Réplica funcional (scripts 01-05)

Primero se demuestra que el modelo relacional **sí resuelve** las consultas del informe. El problema no es de funcionalidad.

| Cassandra (informe) | PostgreSQL |
|---|---|
| Keyspace `EPM_IOT` | BD `epm_iot`, esquema `epm_iot` |
| Tabla desnormalizada `lecturas_sensor` | `central` → `turbina` → `sensor` → `lecturas_sensor` con FK |
| Partition key `(sensor_id, date)` | `PARTITION BY RANGE (ts)`, una tabla física por día |
| Clustering `timestamp DESC` | `PRIMARY KEY (sensor_id, ts)`, recorrida con *Index Scan Backward* |
| `TTL 86400` | `purgar_particiones('24 hours')` (DETACH + DROP) |
| `TRACING ON` | `EXPLAIN (ANALYZE, BUFFERS)` |

| Consulta | Resultado |
|---|---|
| 2.1 Última lectura | 14:10, vibración 3.8, **0,05 ms**, lee 1 sola partición |
| 2.2 Última hora | 3 lecturas (14:10, 14:05, 14:00) |
| 2.3 Promedio por turbina | 3.50. Con JOIN promedia **todos** los sensores de la turbina, algo que Cassandra no permite en una sola consulta |
| Carga de 100.000 filas (COPY) | ≈1,2 s |
| Filtro por vibración sin clave | Cassandra lo rechaza (`ALLOW FILTERING`). PostgreSQL lo ejecuta, pero recorre todas las particiones (≈200 veces más lento). Con un índice secundario baja a 0,8 ms |

A esta escala (100.000 filas, sin concurrencia) **PostgreSQL es igual o más rápido que Cassandra**. Los experimentos siguientes muestran qué pasa cuando se acerca a la carga real.

---

## Parte 2. Experimentos de impacto

### E1. Ingesta de 85.000 eventos/s (`06_bench_ingesta.sql`)

**Qué se mide.** Se crean los 85.000 sensores reales. Luego se simulan 30 segundos de telemetría: en cada segundo entran 85.000 filas con un solo `INSERT … SELECT` y un `COMMIT`. Es el **mejor caso posible** para PostgreSQL (carga masiva desde una sesión). Si insertar un segundo de datos tarda más de 1.000 ms, la base se va atrasando.

| Escenario | ms promedio por "segundo" | Segundos atrasados (de 30) | Atraso acumulado | Filas/s | % de lo requerido | WAL por fila | Amplificación de escritura |
|---|---|---|---|---|---|---|---|
| A: PK + FK | 706,6 | 2 | 0 s | 120.294 | 141 % | 287 B | **8x** |
| B: + 3 índices analíticos | 1.433,6 | **28** | **13 s** | 59.293 | **70 %** | 576 B | **16x** |

**Qué se observa**

- Solo con la PK cumple, pero con poco margen: dos segundos ya superan los 1.000 ms.
- Al agregar los índices que pediría la analítica (vibración, temperatura, tiempo), la ingesta cae 51 % y la base acumula 13 s de atraso en apenas 30 s. En un día serían horas.
- Cada lectura de 36 bytes útiles genera 287-576 bytes de WAL, antes de escribir la tabla y el índice. Ese es el "bloqueo de IOPS" que menciona el informe.

**Por qué ocurre en el modelo relacional.** Cada fila escribe:

1. El WAL.
2. La página del heap con una cabecera MVCC de 23 bytes.
3. Una entrada en **cada** B-tree, en posiciones aleatorias.
4. Una verificación de la FK contra `sensor`.

Todo eso ocurre en un único nodo primario.

**Cómo lo resuelve Cassandra.** Usa un árbol LSM: cada escritura es un *append* secuencial al commitlog y a la memtable, sin leer antes de escribir, sin FK y sin B-trees. Además es *masterless*: cualquier nodo acepta escrituras, así que la capacidad crece agregando nodos. ClickHouse, por su parte, recibe lotes que escribe como bloques columnares inmutables.

### E2. Ingesta realista: un evento por transacción (`bench/07_concurrencia.sh`)

**Qué se mide.** En producción los eventos no llegan en un lote perfecto por segundo, sino uno a uno desde el broker. Con pgbench se simulan 8 conexiones, cada una insertando una fila por transacción.

| Configuración | Filas/s | p99 del INSERT | % de lo requerido |
|---|---|---|---|
| `synchronous_commit = on` (durable) | **8.032** | 2,47 ms | **9,4 %** |
| `synchronous_commit = off` (puede perder datos si se cae) | 14.632 | 1,57 ms | 17 % |

**Qué se observa.** Solo se alcanza el 9 % de la carga. Harían falta unas 10 instancias como esta, y PostgreSQL no reparte escrituras entre nodos de forma nativa (hay un solo primario; las réplicas son de lectura). Incluso sacrificando durabilidad, no llega ni a la quinta parte.

**Por qué.** Cada `COMMIT` durable espera un `fsync` del WAL. Además, cada transacción paga análisis, planificación, bloqueos y verificación de FK.

**NoSQL.** Cassandra sincroniza el commitlog de forma periódica y protege la durabilidad con el factor de replicación (RF=3) en lugar de con un fsync por escritura. Así la carga se reparte entre todos los nodos del anillo.

### E3. Interferencia OLTP vs OLAP y latencia < 2 ms (`bench/07_concurrencia.sh`)

**Qué se mide.** La consulta de alerta temprana (última lectura de un sensor al azar entre los 85.000) se lanza desde 4 conexiones durante 20 s, en tres situaciones sobre la misma base:

| Escenario | p50 | p95 | p99 | Máx | Consultas > 2 ms |
|---|---|---|---|---|---|
| a) Sola | 0,22 ms | 0,56 ms | **0,91 ms** | 41 ms | 0,2 % |
| b) + ingesta concurrente | 0,42 ms | 1,27 ms | 2,04 ms | 56 ms | 1,1 % |
| c) + ingesta + 2 consultas analíticas históricas | 0,27 ms | 4,05 ms | **7,92 ms** | 74 ms | **12,4 %** |

La consulta analítica (promedio de vibración por turbina sobre todo el histórico, ≈6 M filas) tardó **5,1 s de mediana**.

**Qué se observa.**

- Sin carga, PostgreSQL cumple el requisito (p99 = 0,9 ms).
- Con la ingesta, el p99 ya queda en el límite.
- Cuando además corren consultas analíticas en la misma base, **el p99 se multiplica por 9 y 1 de cada 8 alertas llega tarde**. En un sistema de prevención de fallas en turbinas eso es inaceptable.

Esta es exactamente la "interferencia mutua entre consultas analíticas históricas y transacciones operacionales" que describe el informe, ahora medida.

**Por qué.** Las tres cargas compiten por los mismos recursos del único servidor: CPU, `shared_buffers`, disco y locks internos. Además, cada escaneo analítico lee filas completas (ver E4).

**Solución Lambda.** Hay aislamiento físico: Cassandra solo atiende la capa de velocidad y ClickHouse atiende la analítica en otro clúster, alimentado desde Kafka. Una consulta pesada no toca los recursos de las alertas.

### E4. Almacenamiento por filas vs columnar (`08_almacenamiento_analitica.sql`)

**Qué se mide.** El tamaño real en disco de una partición de 2,55 M filas y los bytes que PostgreSQL lee para calcular `AVG(vibracion)`.

| Métrica | Valor medido |
|---|---|
| Bytes útiles por lectura (uuid + ts + 3 reales) | 36 B |
| Bytes reales por lectura (tabla + índice PK) | **112,9 B** (3,1x) |
| Datos leídos para `AVG(vibracion)` sobre 5,8 M filas | 379 MB, cuando la columna ocupa 22 MB: **17x de amplificación de lectura** |
| Lo que leería un motor columnar (columna sola, 10:1) | ≈2,3 MB |

**Proyección a la escala EPM** (85.000 sensores, 1 lectura/s), con el tamaño medido:

| Capa | PostgreSQL | Cassandra (informe, 50 B) | Columnar 10:1 (ClickHouse / Parquet) |
|---|---|---|---|
| HOT 24 h | 0,83 TB | 0,37 TB | 0,04 TB |
| WARM 30 días | 24,9 TB | 11,0 TB | 1,1 TB |
| COLD 7 años (CREG) | **2.120 TB (2,1 PB)** | 939 TB | **94 TB** |

- PostgreSQL necesita **22,6 veces más disco** que el almacenamiento columnar para cumplir la retención de la CREG.
- Solo el índice PK de **un día** pesaría **328 GB**. Para responder en menos de 2 ms, ese índice debería caber en RAM.

**Por qué.** El almacenamiento por filas no comprime filas pequeñas (TOAST solo actúa sobre valores de más de 2 kB) y obliga a leer todas las columnas. En cambio, en un motor columnar los valores de una columna quedan contiguos y son muy parecidos entre sí (timestamps crecientes, vibraciones cercanas), así que Delta + LZ4/ZSTD los comprime alrededor de 10:1. Además, una consulta solo lee las columnas que usa.

### E5. Expiración de la capa HOT, el "TTL" (`09_expiracion_ttl.sql`)

**Qué se mide.** Se comparan dos formas de expirar un día con 2,55 M lecturas.

| Paso | Tiempo | WAL generado | Tamaño en disco |
|---|---|---|---|
| A1 `DELETE` del día | **1.555 ms** | **303 MB** (más que los propios datos) | 275 MB (no se libera nada) |
| A2 `SELECT COUNT(*)` sobre la partición ya "vacía" | 140 ms | 6 MB (¡un SELECT que escribe!) | 275 MB |
| A3 `VACUUM` | 896 ms | 59 MB | 109 MB (el índice no se reduce) |
| A4 `SELECT` después de VACUUM | 0,1 ms | 0 | 109 MB |
| **B1 `DETACH` + `DROP` de la partición** | **1,7 ms** | 68 kB | eliminada |

- El `DELETE` deja 2,55 M tuplas muertas. Es el equivalente directo de los *tombstones* de Cassandra que describe el informe: leer la partición vacía cuesta 1.400 veces más que después del VACUUM.
- Escalado a un día real de EPM (7.344 M filas, unas 2.880 veces más), un `DELETE` tomaría **alrededor de 75 minutos y generaría unos 870 GB de WAL** cada día.
- Con particiones diarias, el relacional **sí** logra una expiración limpia, comparable al TTL con compactación por ventanas de tiempo de Cassandra. Es el único requisito que cumple, y solo porque se diseñó el particionamiento pensando en eso.

---

## Veredicto (`10_resumen.sql`)

| Requisito EPM | Objetivo | PostgreSQL medido | Resultado |
|---|---|---|---|
| Ingesta bulk, solo PK+FK | ≥ 85.000 filas/s | 120.294 filas/s | Cumple (mejor caso, sin margen para índices) |
| Ingesta bulk + índices analíticos | ≥ 85.000 filas/s | 59.293 filas/s | **No cumple** |
| Ingesta realista (1 evento/tx) | ≥ 85.000 filas/s | 8.032 filas/s | **No cumple** (9 %) |
| Alerta temprana sin carga (p99) | < 2 ms | 0,91 ms | Cumple |
| Alerta con ingesta + analítica (p99) | < 2 ms | 7,92 ms | **No cumple** |
| Retención 7 años | ≈ 94 TB columnar | 2.120 TB (22,6x) | **No cumple** |
| Expiración con DELETE | sin basura | 1,5 s + 303 MB WAL + VACUUM | **No cumple** |
| Expiración con DROP de partición | sin basura | 1,7 ms | Cumple |

## Conclusiones

1. **El modelo relacional no falla por diseño lógico, sino por su motor físico.** Resuelve todas las consultas y, a pequeña escala y sin concurrencia, incluso es más rápido. Los problemas aparecen al combinar volumen, concurrencia y cargas mixtas, que es exactamente la situación de EPM.
2. **Escritura:** B-tree + WAL + MVCC + FK generan de 8 a 16 veces más escritura que el dato útil, en un único nodo primario. El árbol LSM y la arquitectura *masterless* de Cassandra convierten eso en escrituras secuenciales repartidas entre nodos.
3. **Latencia:** compartir el servidor entre alertas y analítica multiplica por 9 el p99. La arquitectura Lambda separa físicamente las cargas: Cassandra para la capa de velocidad y ClickHouse/Iceberg para la analítica y la capa histórica.
4. **Almacenamiento:** el formato por filas ocupa 22 veces más que el columnar comprimido y lee 17 veces más datos en analítica. Para 7 años de CREG es la diferencia entre ≈94 TB y más de 2 PB.
5. **Lo que el relacional sí aporta:** integridad referencial, JOINs (promedio por turbina y por central), índices secundarios ad hoc y transacciones ACID. Por eso, en una arquitectura de **persistencia políglota**, PostgreSQL es la mejor opción para el catálogo de activos (centrales, turbinas, sensores, mantenimientos, usuarios). Lo que no conviene es usarlo como almacén de la telemetría.

---

## Notas

- Reiniciar desde cero: `docker compose down -v && docker compose up -d`, o volver a ejecutar `01_esquema.sql`, que borra y recrea los esquemas `epm_iot` y `bench`.
- Parámetros de `07_concurrencia.sh` por variables de entorno: `DURACION`, `CLIENTES_INGESTA`, `CLIENTES_LECTURA`, `CLIENTES_ANALITICA`. Ejemplo: `docker exec -e DURACION=60 -it postgres-epm bash /bench/07_concurrencia.sh`.
- `09_expiracion_ttl.sql` borra datos; por eso va al final.
- Este laboratorio no ejecuta Cassandra ni ClickHouse. Los valores de referencia NoSQL/columnar provienen del informe original (50 B por evento, compresión 10:1).
