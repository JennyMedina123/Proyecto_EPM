| Motor | Consulta | Tiempo |
|---|---|---|
| sqlite (OLTP, filas) | Q1. Degradación: vibración diaria en cojinetes, HIT-T3 vs el resto de turbinas | 147.4 ms |
| DuckDB (columnar, Parquet) | Q1. Degradación: vibración diaria en cojinetes, HIT-T3 vs el resto de turbinas | 16.6 ms (8.9x más rápido) |
| sqlite (OLTP, filas) | Q2. Top 10 turbinas por vibración máxima (priorizar mantenimiento) | 197.5 ms |
| DuckDB (columnar, Parquet) | Q2. Top 10 turbinas por vibración máxima (priorizar mantenimiento) | 11.1 ms (17.9x más rápido) |