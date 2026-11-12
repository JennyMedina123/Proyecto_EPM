"""
query_catalog.py

Catálogo de consultas analíticas sobre la capa Gold y utilidades de
perfilamiento (vistas DuckDB, planes EXPLAIN ANALYZE, metadatos Parquet
y medición de latencia).

Centraliza el SQL para que cualquier comparación entre layouts físicos
(baseline vs. ordenado) ejecute exactamente las mismas sentencias; la única
variable es el archivo Parquet de la tabla de hechos.

Dependencias: duckdb (no requiere pyarrow).
"""

import json
import statistics
import tempfile
import time
from pathlib import Path

import duckdb

import config as cfg

# ---------------------------------------------------------------------------
# Layouts físicos de la tabla de hechos
# ---------------------------------------------------------------------------

# Layout baseline: la tabla de hechos tal como la genera transform_gold.py.
FACT_BASELINE = cfg.GOLD / "fact_lecturas_hora.parquet"

# Layout ordenado: copia de la tabla de hechos ordenada por tiempo_sk y con
# row groups pequeños. Vive en una carpeta aparte para no alterar Gold.
GOLD_SORTED = cfg.LAKEHOUSE / "gold_sorted"
FACT_SORTED = GOLD_SORTED / "fact_lecturas_hora.parquet"

# Tamaño de row group del layout ordenado. Cada hora tiene 80 filas
# (una por sensor): 2.000 filas ~ 25 horas, es decir ~1 día por row group.
# Así cada bloque cubre un rango estrecho de tiempo_sk y DuckDB puede
# descartarlo usando sus estadísticas min/max (data skipping).
ROW_GROUP_SIZE = 2_000

# Layout de control: la misma tabla reescrita por DuckDB en UN solo row group.
# El baseline lo escribe pyarrow (transform_gold.py) y el layout ordenado lo
# escribe DuckDB; cada escritor codifica y comprime distinto. Este layout
# separa ese efecto: single_rg -> sorted aísla el cambio de row groups.
GOLD_SINGLE_RG = cfg.LAKEHOUSE / "gold_single_rg"
FACT_SINGLE_RG = GOLD_SINGLE_RG / "fact_lecturas_hora.parquet"
ROW_GROUP_SIZE_SINGLE = 1_000_000

LAYOUTS = {
    "baseline": FACT_BASELINE,
    "single_rg": FACT_SINGLE_RG,
    "sorted": FACT_SORTED,
}

# Carpeta de salida: planes de ejecución y resultados de benchmark.
DOCS_PERF = cfg.DOCS / "performance"

# ---------------------------------------------------------------------------
# Consultas analíticas
# ---------------------------------------------------------------------------

CONSULTAS = {
    "Q1_Alertas_Mes": """
        SELECT
            tb.codigo AS turbina,
            t.anio,
            t.mes,
            SUM(f.alertas_criticas) AS total_alertas
        FROM fact_lecturas_hora f
        JOIN dim_tiempo t
            ON f.tiempo_sk = t.tiempo_sk
        JOIN dim_turbina tb
            ON f.turbina_sk = tb.turbina_sk
        WHERE t.mes = 10
          AND t.anio = 2026
        GROUP BY tb.codigo, t.anio, t.mes
        ORDER BY total_alertas DESC
    """,

    "Q2_Comportamiento_Termico": """
        SELECT
            s.ubicacion,
            ROUND(MAX(f.temperatura_max), 2) AS temp_max_historica,
            ROUND(AVG(f.temperatura_promedio), 2) AS temp_media_global
        FROM fact_lecturas_hora f
        JOIN dim_sensor s
            ON f.sensor_sk = s.sensor_sk
        GROUP BY s.ubicacion
        ORDER BY temp_max_historica DESC
    """,

    "Q3_Dias_Inestabilidad": """
        SELECT
            t.fecha,
            SUM(f.alertas_criticas) AS total_alertas,
            ROUND(AVG(f.vibracion_promedio), 2) AS vib_media
        FROM fact_lecturas_hora f
        JOIN dim_tiempo t
            ON f.tiempo_sk = t.tiempo_sk
        WHERE t.fecha >= DATE '2026-10-17'
          AND t.fecha <= DATE '2026-10-26'
        GROUP BY t.fecha
        ORDER BY total_alertas DESC
        LIMIT 5
    """,
}

# ---------------------------------------------------------------------------
# Consulta sonda (filtro de alta selectividad)
# ---------------------------------------------------------------------------
# Variante de Q3 restringida a un solo día. Permite verificar si el layout
# ordenado activa el data skipping cuando el predicado temporal descarta
# la mayor parte de los datos.
CONSULTAS_SONDA = {
    "Q3S_Filtro_Un_Dia": """
        SELECT
            t.fecha,
            SUM(f.alertas_criticas) AS total_alertas,
            ROUND(AVG(f.vibracion_promedio), 2) AS vib_media
        FROM fact_lecturas_hora f
        JOIN dim_tiempo t
            ON f.tiempo_sk = t.tiempo_sk
        WHERE t.fecha = DATE '2026-10-26'
        GROUP BY t.fecha
    """,
}

TODAS = {**CONSULTAS, **CONSULTAS_SONDA}

DIMENSIONES = ["dim_tiempo", "dim_turbina", "dim_sensor"]


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def conectar(fact_path: Path) -> duckdb.DuckDBPyConnection:
    """Abre una conexión nueva y registra vistas sobre los Parquet de Gold.

    La tabla de hechos se lee desde `fact_path` (layout baseline u ordenado);
    las dimensiones siempre desde Gold. Una conexión por layout evita que la
    caché de una corrida favorezca a la otra.
    """
    if not fact_path.exists():
        raise FileNotFoundError(f"No se encontró: {fact_path}")

    con = duckdb.connect()
    con.execute(
        "CREATE VIEW fact_lecturas_hora AS "
        f"SELECT * FROM read_parquet('{cfg.ruta_sql(fact_path)}')"
    )
    for dim in DIMENSIONES:
        ruta = cfg.GOLD / f"{dim}.parquet"
        con.execute(
            f"CREATE VIEW {dim} AS "
            f"SELECT * FROM read_parquet('{cfg.ruta_sql(ruta)}')"
        )
    return con


def row_groups(fact_path: Path) -> list:
    """Row groups del Parquet: (id, filas, tiempo_sk mínimo, tiempo_sk máximo).

    Usa parquet_metadata() de DuckDB. El mínimo y el máximo de cada bloque
    son las estadísticas con las que el motor decide si puede omitirlo.
    """
    con = duckdb.connect()
    try:
        return con.execute(
            "SELECT row_group_id, row_group_num_rows, stats_min, stats_max "
            f"FROM parquet_metadata('{cfg.ruta_sql(fact_path)}') "
            "WHERE path_in_schema = 'tiempo_sk' "
            "ORDER BY row_group_id"
        ).fetchall()
    finally:
        con.close()


def contar_row_groups(fact_path: Path) -> int:
    """Número de row groups del archivo Parquet."""
    return len(row_groups(fact_path))


def guardar_plan(con, nombre: str, layout: str, query: str) -> Path:
    """Ejecuta EXPLAIN ANALYZE y guarda el plan completo en texto."""
    DOCS_PERF.mkdir(parents=True, exist_ok=True)
    plan = con.execute(f"EXPLAIN ANALYZE {query}").fetchall()[0][1]
    ruta = DOCS_PERF / f"plan_{layout}_{nombre}.txt"
    ruta.write_text(str(plan), encoding="utf-8")
    return ruta


def _hijos(nodo):
    """Hijos de un nodo del perfil, en los dos formatos conocidos de DuckDB
    (1.x usa "children" en todos los niveles; 2.x usa "operator" en la raíz)."""
    if isinstance(nodo, list):
        return nodo
    return nodo.get("children") or nodo.get("operator") or []


def _scans(nodo, encontrados=None):
    """Recorre el árbol del perfil y devuelve los operadores de escaneo con
    sus métricas: filas emitidas, filas estimadas, row groups leídos y
    row groups totales del archivo."""
    if encontrados is None:
        encontrados = []
    if isinstance(nodo, dict):
        tipo = str(nodo.get("type", nodo.get("operator_type", nodo.get("name", ""))))
        if "SCAN" in tipo.upper():
            extra = nodo.get("extra_info", {})
            encontrados.append({
                "filas": nodo.get("intermediate_rows",
                                  nodo.get("operator_cardinality", nodo.get("cardinality"))),
                "estimadas": extra.get("Estimated Cardinality") if isinstance(extra, dict) else None,
                "rg_leidos": nodo.get("row_groups_scanned"),
                "rg_totales": nodo.get("total_row_groups_to_scan"),
                "extra": str(extra),
            })
    for hijo in _hijos(nodo):
        _scans(hijo, encontrados)
    return encontrados


def perfil_consulta(fact_path: Path, query: str) -> dict:
    """Métricas de ejecución de una consulta, tomadas del perfilador JSON.

    filas_scan      filas que emite el scan de la tabla de hechos, después de
                    aplicar los filtros que el motor empuja al scan.
    filas_estimadas cardinalidad que el optimizador estimó para ese scan.
    rg_leidos       row groups de la fact que el scan leyó realmente.
    rg_totales      row groups que tiene el archivo de la fact.
    bytes_leidos    bytes leídos de disco por toda la consulta.

    rg_leidos / rg_totales y bytes_leidos reflejan el data skipping: los row
    groups descartados por estadísticas no se leen.

    Usa una conexión propia que se cierra al terminar, para que la
    configuración del perfilador no afecte el formato de los planes que se
    generen después en otras conexiones.
    """
    vacio = {"filas_scan": None, "filas_estimadas": None,
             "rg_leidos": None, "rg_totales": None, "bytes_leidos": None}
    con = conectar(fact_path)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            salida = Path(tmp) / "perfil.json"
            try:
                con.execute("PRAGMA enable_profiling = 'json'")
                con.execute(f"PRAGMA profiling_output = '{cfg.ruta_sql(salida)}'")
                con.execute(query).fetchall()
                con.execute("PRAGMA disable_profiling")
                arbol = json.loads(salida.read_text(encoding="utf-8"))
            except Exception:
                return vacio
    finally:
        con.close()

    resultado = dict(vacio)
    if isinstance(arbol, dict) and isinstance(arbol.get("io"), dict):
        resultado["bytes_leidos"] = arbol["io"].get("total_bytes_read")

    scans = [s for s in _scans(arbol) if isinstance(s["filas"], (int, float))]
    if scans:
        por_nombre = [s for s in scans if "fact_lecturas_hora" in s["extra"]]
        fact = por_nombre[0] if por_nombre else max(scans, key=lambda s: s["filas"])
        resultado["filas_scan"] = int(fact["filas"])
        est = fact["estimadas"]
        resultado["filas_estimadas"] = int(est) if str(est).isdigit() else est
        resultado["rg_leidos"] = fact["rg_leidos"]
        resultado["rg_totales"] = fact["rg_totales"]
    return resultado


def medir(con, query: str, calentamientos: int = 3, mediciones: int = 5):
    """Latencia de una consulta: descarta `calentamientos` ejecuciones,
    mide `mediciones` y devuelve (lista de tiempos en ms, mediana)."""
    for _ in range(calentamientos):
        con.execute(query).fetchall()

    tiempos = []
    for _ in range(mediciones):
        t0 = time.perf_counter()
        con.execute(query).fetchall()
        tiempos.append((time.perf_counter() - t0) * 1000)

    return tiempos, statistics.median(tiempos)
