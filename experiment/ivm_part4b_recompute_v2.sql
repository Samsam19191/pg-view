-- ================================================================
-- LSQB IVM Experiment - Part 4b: Naive Recomputation V2
-- ================================================================

\timing on

\echo ================================================================
\echo NAIVE RECOMPUTATION - V2
\echo ================================================================

DROP TABLE IF EXISTS V2_nodes CASCADE;
DROP TABLE IF EXISTS V2_edges CASCADE;

\echo Recomputing V2_nodes...
CREATE TABLE V2_nodes AS
SELECT DISTINCT node_id, label FROM (
    SELECT n_p1._0 AS node_id, n_p1._1 AS label
    FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
    UNION
    SELECT n_p2._0, n_p2._1 FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
    UNION
    SELECT n_p3._0, n_p3._1 FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
    UNION
    SELECT n_c1._0, n_c1._1 FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
    UNION
    SELECT n_c2._0, n_c2._1 FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
    UNION
    SELECT n_c3._0, n_c3._1 FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
    UNION
    SELECT n_country._0, n_country._1 FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
) AS v2_all_nodes;

\echo Recomputing V2_edges...
CREATE TABLE V2_edges AS
SELECT DISTINCT edge_id, from_node, to_node, label FROM (
    SELECT e1._0 AS edge_id, e1._1 AS from_node, e1._2 AS to_node, e1._3 AS label
    FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
    UNION
    SELECT e2._0, e2._1, e2._2, e2._3 FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
    UNION
    SELECT e7._0, e7._1, e7._2, e7._3 FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
    UNION
    SELECT e9._0, e9._1, e9._2, e9._3 FROM n_g n_p1
    JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
    JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
    JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
    JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
    JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
    JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
    JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
    JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
    JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
    JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
    JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
    JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
    JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
    JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
    WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287
) AS v2_all_edges;

-- Recreate indexes
CREATE INDEX idx_v2_nodes_id ON v2_nodes(node_id);
CREATE INDEX idx_v2_nodes_label ON v2_nodes(label);
CREATE INDEX idx_v2_edges_from ON v2_edges(from_node);
CREATE INDEX idx_v2_edges_to ON v2_edges(to_node);
CREATE INDEX idx_v2_edges_label ON v2_edges(label);

\echo V2 recomputed - row counts:
SELECT 'v2_nodes' AS tbl, COUNT(*) FROM v2_nodes
UNION ALL SELECT 'v2_edges', COUNT(*) FROM v2_edges;

\echo ================================================================
\echo NAIVE RECOMPUTATION - INDEX_v2 (SSR)
\echo ================================================================

DROP TABLE IF EXISTS INDEX_v2 CASCADE;

CREATE TABLE INDEX_v2 AS
SELECT DISTINCT
    n_p1._0 AS person1, n_p2._0 AS person2, n_p3._0 AS person3,
    n_c1._0 AS city1, n_c2._0 AS city2, n_c3._0 AS city3,
    n_country._0 AS country,
    e1._0 AS e1, e2._0 AS e2, e3._0 AS e3, e4._0 AS e4,
    e5._0 AS e5, e6._0 AS e6, e7._0 AS e7, e9._0 AS e9
FROM n_g n_p1
JOIN e_g e1 ON e1._1 = n_p1._0 AND e1._3 = 'ISLOCATEDIN'
JOIN n_g n_c1 ON e1._2 = n_c1._0 AND n_c1._1 = 'City'
JOIN e_g e2 ON e2._1 = n_c1._0 AND e2._3 = 'ISPARTOF'
JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
JOIN e_g e7 ON e7._1 = n_p1._0 AND e7._3 = 'KNOWS'
JOIN n_g n_p2 ON e7._2 = n_p2._0 AND n_p2._1 = 'Person'
JOIN e_g e3 ON e3._1 = n_p2._0 AND e3._3 = 'ISLOCATEDIN'
JOIN n_g n_c2 ON e3._2 = n_c2._0 AND n_c2._1 = 'City'
JOIN e_g e4 ON e4._1 = n_c2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
JOIN e_g e9 ON e9._1 = n_p2._0 AND e9._3 = 'KNOWS'
JOIN n_g n_p3 ON e9._2 = n_p3._0 AND n_p3._1 = 'Person'
JOIN e_g e5 ON e5._1 = n_p3._0 AND e5._3 = 'ISLOCATEDIN'
JOIN n_g n_c3 ON e5._2 = n_c3._0 AND n_c3._1 = 'City'
JOIN e_g e6 ON e6._1 = n_c3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
WHERE n_p1._1 = 'Person' AND n_p1._0 >= 723388 AND n_p1._0 <= 727287;

CREATE INDEX idx_index_v2_person1 ON INDEX_v2 (person1);

\echo INDEX_v2 recomputed - row count:
SELECT COUNT(*) FROM INDEX_v2;

\timing off
