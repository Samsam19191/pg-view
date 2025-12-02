-- ================================================================
-- PROV Benchmark Queries: Q3 and Q4
-- ================================================================
-- Based on the paper's query definitions for V3
--
-- Q3: MATCH (a:REVISION)-[u:USED]->(e1:E), (e2:E)-[g:GENBY]->(a:REVISION), 
--           (e2:E)-[e3:DERBY]->(e1:E)
--     FROM v3 WHERE e1 < 1000
--     RETURN (a),(u),(e1),(e2),(g),(e3)
--
-- Q4: MATCH (e2:E)-[u:MULTIDERBY2]->(e1:E)
--     FROM v3 WHERE e1 < 1000
--     RETURN (e1),(e2),(u)
--
-- Note: Q3 queries the zoom-in transformation result
--       Q4 queries the MULTIDERBY2 shortcut edges
-- ================================================================

\timing on

\echo ================================================================
\echo Q3 BENCHMARK - REVISION zoom-in pattern
\echo ================================================================
\echo
\echo Q3 Definition:
\echo   MATCH (a:REVISION)-[u:USED]->(e1:E), 
\echo         (e2:E)-[g:GENBY]->(a:REVISION),
\echo         (e2:E)-[e3:DERBY]->(e1:E)
\echo   FROM v3 WHERE e1 < 1000
\echo

-- ----------------------------------------
-- Q3(G) - Query over base graph
-- ----------------------------------------
\echo Q3(G) - Base graph (simulating transformation on-the-fly):
\echo   This finds DERBY edges and projects the transformation pattern

SELECT COUNT(*) AS result
FROM e_g d
JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
WHERE d._3 = 'DERBY' AND e1._0 < 1000;

SELECT COUNT(*) AS result
FROM e_g d
JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
WHERE d._3 = 'DERBY' AND e1._0 < 1000;

SELECT COUNT(*) AS result
FROM e_g d
JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
WHERE d._3 = 'DERBY' AND e1._0 < 1000;

-- ----------------------------------------
-- Q3(MV) - Query over materialized view V3
-- ----------------------------------------
\echo Q3(MV) - Materialized V3 view:
\echo   Queries the transformed graph with REVISION, USED, GENBY

SELECT COUNT(*) AS result
FROM v3_nodes a
JOIN v3_edges u ON u.from_node = a.node_id AND u.label = 'USED'
JOIN v3_nodes e1 ON u.to_node = e1.node_id AND e1.label = 'E'
JOIN v3_edges g ON g.to_node = a.node_id AND g.label = 'GENBY'
JOIN v3_nodes e2 ON g.from_node = e2.node_id AND e2.label = 'E'
WHERE a.label = 'REVISION' AND e1.node_id < 1000;

SELECT COUNT(*) AS result
FROM v3_nodes a
JOIN v3_edges u ON u.from_node = a.node_id AND u.label = 'USED'
JOIN v3_nodes e1 ON u.to_node = e1.node_id AND e1.label = 'E'
JOIN v3_edges g ON g.to_node = a.node_id AND g.label = 'GENBY'
JOIN v3_nodes e2 ON g.from_node = e2.node_id AND e2.label = 'E'
WHERE a.label = 'REVISION' AND e1.node_id < 1000;

SELECT COUNT(*) AS result
FROM v3_nodes a
JOIN v3_edges u ON u.from_node = a.node_id AND u.label = 'USED'
JOIN v3_nodes e1 ON u.to_node = e1.node_id AND e1.label = 'E'
JOIN v3_edges g ON g.to_node = a.node_id AND g.label = 'GENBY'
JOIN v3_nodes e2 ON g.from_node = e2.node_id AND e2.label = 'E'
WHERE a.label = 'REVISION' AND e1.node_id < 1000;

-- ----------------------------------------
-- Q3(SSR) - Query using SSR index
-- ----------------------------------------
\echo Q3(SSR) - SSR index (INDEX_v3_q3):

SELECT COUNT(*) AS result
FROM INDEX_v3_q3
WHERE e1 < 1000;

SELECT COUNT(*) AS result
FROM INDEX_v3_q3
WHERE e1 < 1000;

SELECT COUNT(*) AS result
FROM INDEX_v3_q3
WHERE e1 < 1000;


\echo ================================================================
\echo Q4 BENCHMARK - MULTIDERBY2 shortcut pattern
\echo ================================================================
\echo
\echo Q4 Definition:
\echo   MATCH (e2:E)-[u:MULTIDERBY2]->(e1:E)
\echo   FROM v3 WHERE e1 < 1000
\echo

-- ----------------------------------------
-- Q4(G) - Query over base graph
-- ----------------------------------------
\echo Q4(G) - Base graph (computing 2-hop DERBY chain):

SELECT COUNT(*) AS result
FROM e_g d1
JOIN n_g e1 ON d1._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d1._1 = e2._0 AND e2._1 = 'E'
JOIN e_g d2 ON d2._2 = e2._0 AND d2._3 = 'DERBY'
JOIN n_g e3 ON d2._1 = e3._0 AND e3._1 = 'E'
WHERE d1._3 = 'DERBY' AND e1._0 < 1000;

SELECT COUNT(*) AS result
FROM e_g d1
JOIN n_g e1 ON d1._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d1._1 = e2._0 AND e2._1 = 'E'
JOIN e_g d2 ON d2._2 = e2._0 AND d2._3 = 'DERBY'
JOIN n_g e3 ON d2._1 = e3._0 AND e3._1 = 'E'
WHERE d1._3 = 'DERBY' AND e1._0 < 1000;

SELECT COUNT(*) AS result
FROM e_g d1
JOIN n_g e1 ON d1._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d1._1 = e2._0 AND e2._1 = 'E'
JOIN e_g d2 ON d2._2 = e2._0 AND d2._3 = 'DERBY'
JOIN n_g e3 ON d2._1 = e3._0 AND e3._1 = 'E'
WHERE d1._3 = 'DERBY' AND e1._0 < 1000;

-- ----------------------------------------
-- Q4(MV) - Query over materialized view V3
-- ----------------------------------------
\echo Q4(MV) - Materialized V3 view (MULTIDERBY2 edges):

SELECT COUNT(*) AS result
FROM v3_edges u
JOIN v3_nodes e1 ON u.to_node = e1.node_id AND e1.label = 'E'
JOIN v3_nodes e2 ON u.from_node = e2.node_id AND e2.label = 'E'
WHERE u.label = 'MULTIDERBY2' AND e1.node_id < 1000;

SELECT COUNT(*) AS result
FROM v3_edges u
JOIN v3_nodes e1 ON u.to_node = e1.node_id AND e1.label = 'E'
JOIN v3_nodes e2 ON u.from_node = e2.node_id AND e2.label = 'E'
WHERE u.label = 'MULTIDERBY2' AND e1.node_id < 1000;

SELECT COUNT(*) AS result
FROM v3_edges u
JOIN v3_nodes e1 ON u.to_node = e1.node_id AND e1.label = 'E'
JOIN v3_nodes e2 ON u.from_node = e2.node_id AND e2.label = 'E'
WHERE u.label = 'MULTIDERBY2' AND e1.node_id < 1000;

-- ----------------------------------------
-- Q4(SSR) - Query using SSR index
-- ----------------------------------------
\echo Q4(SSR) - SSR index (INDEX_v3_q4):

SELECT COUNT(*) AS result
FROM INDEX_v3_q4
WHERE e1 < 1000;

SELECT COUNT(*) AS result
FROM INDEX_v3_q4
WHERE e1 < 1000;

SELECT COUNT(*) AS result
FROM INDEX_v3_q4
WHERE e1 < 1000;


\echo ================================================================
\echo SANITY CHECK - All modes should return same results
\echo ================================================================

\echo Q3 Results Comparison:
SELECT 'Q3(G)' AS mode, COUNT(*) AS result
FROM e_g d
JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
WHERE d._3 = 'DERBY' AND e1._0 < 1000
UNION ALL
SELECT 'Q3(MV)', COUNT(*)
FROM v3_nodes a
JOIN v3_edges u ON u.from_node = a.node_id AND u.label = 'USED'
JOIN v3_nodes e1 ON u.to_node = e1.node_id AND e1.label = 'E'
JOIN v3_edges g ON g.to_node = a.node_id AND g.label = 'GENBY'
JOIN v3_nodes e2 ON g.from_node = e2.node_id AND e2.label = 'E'
WHERE a.label = 'REVISION' AND e1.node_id < 1000
UNION ALL
SELECT 'Q3(SSR)', COUNT(*) FROM INDEX_v3_q3 WHERE e1 < 1000;

\echo Q4 Results Comparison:
SELECT 'Q4(G)' AS mode, COUNT(*) AS result
FROM e_g d1
JOIN n_g e1 ON d1._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d1._1 = e2._0 AND e2._1 = 'E'
JOIN e_g d2 ON d2._2 = e2._0 AND d2._3 = 'DERBY'
JOIN n_g e3 ON d2._1 = e3._0 AND e3._1 = 'E'
WHERE d1._3 = 'DERBY' AND e1._0 < 1000
UNION ALL
SELECT 'Q4(MV)', COUNT(*)
FROM v3_edges u
JOIN v3_nodes e1 ON u.to_node = e1.node_id AND e1.label = 'E'
JOIN v3_nodes e2 ON u.from_node = e2.node_id AND e2.label = 'E'
WHERE u.label = 'MULTIDERBY2' AND e1.node_id < 1000
UNION ALL
SELECT 'Q4(SSR)', COUNT(*) FROM INDEX_v3_q4 WHERE e1 < 1000;

\timing off

\echo ================================================================
\echo BENCHMARK COMPLETE
\echo ================================================================
