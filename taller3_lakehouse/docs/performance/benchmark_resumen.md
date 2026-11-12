Motor: DuckDB 2.0.0. Latencia = mediana de 15 mediciones (3 rondas x 5, tras 3 calentamientos por ronda).

| Consulta | Filas dev. | t baseline (ms) | t single_rg (ms) | t sorted (ms) | Factor total | Factor aislado | RG leídos single_rg -> sorted | Bytes single_rg -> sorted |
|---|---|---|---|---|---|---|---|---|
| Q1_Alertas_Mes | 20 | 11.352 | 10.852 | 11.506 | 0.99x | 0.94x | 1/1 -> 9/9 | 38218 -> 40694 |
| Q2_Comportamiento_Termico | 4 | 7.922 | 7.004 | 7.494 | 1.06x | 0.93x | 1/1 -> 9/9 | 166486 -> 210213 |
| Q3_Dias_Inestabilidad | 5 | 5.833 | 6.515 | 5.994 | 0.97x | 1.09x | 1/1 -> 9/9 | 137370 -> 433055 |
| Q3S_Filtro_Un_Dia | 1 | 5.391 | 4.863 | 5.736 | 0.94x | 0.85x | 1/1 -> 2/9 | 137370 -> 66440 |
