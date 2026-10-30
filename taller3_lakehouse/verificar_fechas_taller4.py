import sys
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))
import config as cfg

con = duckdb.connect()

ruta = cfg.GOLD / "dim_tiempo.parquet"

consulta = f"""
SELECT
    MIN(fecha) AS fecha_inicial,
    MAX(fecha) AS fecha_final,
    COUNT(*) AS total_registros,
    COUNT(DISTINCT fecha) AS dias_distintos
FROM read_parquet('{cfg.ruta_sql(ruta)}')
"""

print("=" * 55)
print("VERIFICACIÓN DEL RANGO TEMPORAL DE GOLD")
print("=" * 55)
print(con.execute(consulta).fetchdf().to_string(index=False))

con.close()
