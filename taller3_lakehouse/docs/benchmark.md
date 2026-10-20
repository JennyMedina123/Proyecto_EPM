| Motor | Consulta | Tiempo |
|---|---|---|
| sqlite (OLTP, filas) | Q1. Degradación: vibración diaria en cojinetes, HIT-T3 vs el resto de turbinas | 47.0 ms |
| DuckDB (columnar, Parquet) | Q1. Degradación: vibración diaria en cojinetes, HIT-T3 vs el resto de turbinas | 12.7 ms (3.7x más rápido) |
| sqlite (OLTP, filas) | Q2. Top 10 turbinas por vibración máxima | 62.6 ms |
| DuckDB (columnar, Parquet) | Q2. Top 10 turbinas por vibración máxima | 8.0 ms (7.8x más rápido) |