-- ================================================================
-- Workload Mix Summary Report
-- ================================================================
-- Run this after all workload experiments to generate summary
-- ================================================================

\echo ================================================================
\echo WORKLOAD MIX EXPERIMENT - FINAL SUMMARY
\echo ================================================================

\echo ----------------------------------------------------------------
\echo READ-HEAVY (95% reads, 5% updates)
\echo ----------------------------------------------------------------
SELECT query_mode, 
       COUNT(*) as queries,
       ROUND(AVG(time_ms)::numeric, 3) as avg_ms,
       ROUND(STDDEV(time_ms)::numeric, 3) as stddev_ms
FROM workload_results 
WHERE experiment_id = 'read_heavy_q1' AND operation_type = 'query'
GROUP BY query_mode ORDER BY query_mode;

\echo ----------------------------------------------------------------
\echo BALANCED (50% reads, 50% updates)
\echo ----------------------------------------------------------------
SELECT query_mode, 
       COUNT(*) as queries,
       ROUND(AVG(time_ms)::numeric, 3) as avg_ms,
       ROUND(STDDEV(time_ms)::numeric, 3) as stddev_ms
FROM workload_results 
WHERE experiment_id = 'balanced_q1' AND operation_type = 'query'
GROUP BY query_mode ORDER BY query_mode;

\echo ----------------------------------------------------------------
\echo UPDATE-HEAVY (10% reads, 90% updates)
\echo ----------------------------------------------------------------
SELECT query_mode, 
       COUNT(*) as queries,
       ROUND(AVG(time_ms)::numeric, 3) as avg_ms,
       ROUND(STDDEV(time_ms)::numeric, 3) as stddev_ms
FROM workload_results 
WHERE experiment_id = 'update_heavy_q1' AND operation_type = 'query'
GROUP BY query_mode ORDER BY query_mode;

\echo ----------------------------------------------------------------
\echo COMPARISON TABLE (Avg Query Time in ms)
\echo ----------------------------------------------------------------
SELECT 
    'Read-Heavy (95/5)' as workload,
    MAX(CASE WHEN query_mode = 'G' THEN ROUND(avg_ms, 2) END) as "Q(G)",
    MAX(CASE WHEN query_mode = 'MV' THEN ROUND(avg_ms, 2) END) as "Q(MV)",
    MAX(CASE WHEN query_mode = 'SSR' THEN ROUND(avg_ms, 2) END) as "Q(SSR)"
FROM (
    SELECT query_mode, AVG(time_ms) as avg_ms
    FROM workload_results 
    WHERE experiment_id = 'read_heavy_q1' AND operation_type = 'query'
    GROUP BY query_mode
) t
UNION ALL
SELECT 
    'Balanced (50/50)',
    MAX(CASE WHEN query_mode = 'G' THEN ROUND(avg_ms, 2) END),
    MAX(CASE WHEN query_mode = 'MV' THEN ROUND(avg_ms, 2) END),
    MAX(CASE WHEN query_mode = 'SSR' THEN ROUND(avg_ms, 2) END)
FROM (
    SELECT query_mode, AVG(time_ms) as avg_ms
    FROM workload_results 
    WHERE experiment_id = 'balanced_q1' AND operation_type = 'query'
    GROUP BY query_mode
) t
UNION ALL
SELECT 
    'Update-Heavy (10/90)',
    MAX(CASE WHEN query_mode = 'G' THEN ROUND(avg_ms, 2) END),
    MAX(CASE WHEN query_mode = 'MV' THEN ROUND(avg_ms, 2) END),
    MAX(CASE WHEN query_mode = 'SSR' THEN ROUND(avg_ms, 2) END)
FROM (
    SELECT query_mode, AVG(time_ms) as avg_ms
    FROM workload_results 
    WHERE experiment_id = 'update_heavy_q1' AND operation_type = 'query'
    GROUP BY query_mode
) t;

\echo ----------------------------------------------------------------
\echo SPEEDUP RATIOS (G/MV and G/SSR)
\echo ----------------------------------------------------------------
WITH avg_times AS (
    SELECT experiment_id,
           MAX(CASE WHEN query_mode = 'G' THEN avg_ms END) as g_ms,
           MAX(CASE WHEN query_mode = 'MV' THEN avg_ms END) as mv_ms,
           MAX(CASE WHEN query_mode = 'SSR' THEN avg_ms END) as ssr_ms
    FROM (
        SELECT experiment_id, query_mode, AVG(time_ms) as avg_ms
        FROM workload_results 
        WHERE operation_type = 'query'
        GROUP BY experiment_id, query_mode
    ) t
    GROUP BY experiment_id
)
SELECT 
    CASE experiment_id 
        WHEN 'read_heavy_q1' THEN 'Read-Heavy'
        WHEN 'balanced_q1' THEN 'Balanced'
        WHEN 'update_heavy_q1' THEN 'Update-Heavy'
    END as workload,
    ROUND((g_ms / mv_ms)::numeric, 1) as "G/MV speedup",
    ROUND((g_ms / ssr_ms)::numeric, 1) as "G/SSR speedup"
FROM avg_times
ORDER BY experiment_id;
