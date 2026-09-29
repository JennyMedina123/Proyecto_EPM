-- =====================================================================
-- 10_resumen.sql | Resumen: requisitos de EPM vs. lo medido en PostgreSQL
-- =====================================================================
SET search_path TO epm_iot, public;

\echo '=== Todas las métricas medidas ==='
SELECT experimento, metrica, valor, unidad
FROM bench.resultados ORDER BY experimento, metrica;

\echo '=== Veredicto frente a los requisitos de EPM ==='
WITH r AS (SELECT metrica, valor FROM bench.resultados)
SELECT requisito, objetivo, medido,
       CASE WHEN NOT cumple THEN 'NO CUMPLE'
            WHEN o = 1 AND (SELECT valor FROM r WHERE metrica = 'filas/s A: PK + FK') < 100000
                 THEN 'AL LÍMITE (sin margen)'
            ELSE 'CUMPLE' END AS resultado
FROM (
  SELECT 1 AS o, 'Ingesta sostenida (bulk, sólo PK+FK)' AS requisito, '>= 85.000 filas/s' AS objetivo,
         (SELECT valor FROM r WHERE metrica = 'filas/s A: PK + FK') || ' filas/s' AS medido,
         (SELECT valor FROM r WHERE metrica = 'filas/s A: PK + FK') >= 85000 AS cumple
  UNION ALL
  SELECT 2, 'Ingesta sostenida (bulk + índices analíticos)', '>= 85.000 filas/s',
         (SELECT valor FROM r WHERE metrica = 'filas/s B: PK + FK + 3 índices') || ' filas/s',
         (SELECT valor FROM r WHERE metrica = 'filas/s B: PK + FK + 3 índices') >= 85000
  UNION ALL
  SELECT 3, 'Ingesta realista (1 evento por transacción)', '>= 85.000 filas/s',
         (SELECT valor FROM r WHERE metrica = 'filas/s commit síncrono') || ' filas/s',
         (SELECT valor FROM r WHERE metrica = 'filas/s commit síncrono') >= 85000
  UNION ALL
  SELECT 4, 'Alerta temprana sin carga (p99)', '< 2 ms',
         (SELECT valor FROM r WHERE metrica = 'p99 lectura sola') || ' ms',
         (SELECT valor FROM r WHERE metrica = 'p99 lectura sola') < 2
  UNION ALL
  SELECT 5, 'Alerta temprana con ingesta + analítica (p99)', '< 2 ms',
         (SELECT valor FROM r WHERE metrica = 'p99 lectura + ingesta + analítica') || ' ms',
         (SELECT valor FROM r WHERE metrica = 'p99 lectura + ingesta + analítica') < 2
  UNION ALL
  SELECT 6, 'Retención 7 años CREG (almacenamiento)', '≈ 94 TB (columnar 10:1)',
         (SELECT valor FROM r WHERE metrica = 'TB 7 años PostgreSQL') || ' TB ('
           || ROUND((SELECT valor FROM r WHERE metrica = 'TB 7 años PostgreSQL')
                  / (SELECT valor FROM r WHERE metrica = 'TB 7 años columnar 10:1'), 1) || 'x)',
         FALSE
  UNION ALL
  SELECT 7, 'Expiración sin "tombstones" (DELETE)', 'sin basura ni VACUUM',
         (SELECT valor FROM r WHERE metrica = 'ms A1 DELETE del día') || ' ms + VACUUM',
         FALSE
  UNION ALL
  SELECT 8, 'Expiración por partición (DROP)', 'sin basura ni VACUUM',
         (SELECT valor FROM r WHERE metrica = 'ms B1 DETACH + DROP de la partición') || ' ms',
         TRUE
) v ORDER BY o;
