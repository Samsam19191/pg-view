-- ================================================================
-- LSQB IVM Experiment - Part 4: Naive Recomputation
-- ================================================================
-- Fully recompute all materialized views and SSR tables
-- ================================================================

\timing on

\echo ================================================================
\echo NAIVE RECOMPUTATION - V1
\echo ================================================================

DROP TABLE IF EXISTS V1_nodes CASCADE;
DROP TABLE IF EXISTS V1_edges CASCADE;

\echo Recomputing V1_nodes...
CREATE TABLE V1_nodes AS
SELECT DISTINCT node_id, label FROM (
    SELECT n_person1._0 AS node_id, n_person1._1 AS label
    FROM n_g n_person1
    JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
    JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
    JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
    JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
    JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
    JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
    JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
    WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400
    UNION
    SELECT n_person2._0, n_person2._1
    FROM n_g n_person1
    JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
    JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
    JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
    JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
    JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
    JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
    JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
    WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400
    UNION
    SELECT n_comment._0, n_comment._1
    FROM n_g n_person1
    JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
    JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
    JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
    JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
    JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
    JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
    JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
    WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400
    UNION
    SELECT n_post._0, n_post._1
    FROM n_g n_person1
    JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
    JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
    JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
    JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
    JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
    JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
    JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
    WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400
) AS v1_all_nodes;

\echo Recomputing V1_edges...
CREATE TABLE V1_edges AS
SELECT DISTINCT edge_id, from_node, to_node, label FROM (
    SELECT e1._0 AS edge_id, e1._1 AS from_node, e1._2 AS to_node, e1._3 AS label
    FROM n_g n_person1
    JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
    JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
    JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
    JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
    JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
    JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
    JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
    WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400
    UNION
    SELECT e3._0, e3._1, e3._2, e3._3
    FROM n_g n_person1
    JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
    JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
    JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
    JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
    JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
    JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
    JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
    WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400
    UNION
    SELECT e4._0, e4._1, e4._2, e4._3
    FROM n_g n_person1
    JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
    JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
    JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
    JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
    JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
    JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
    JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
    WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400
    UNION
    SELECT e5._0, e5._1, e5._2, e5._3
    FROM n_g n_person1
    JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
    JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
    JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
    JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
    JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
    JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
    JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
    WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400
) AS v1_all_edges;

-- Recreate indexes
CREATE INDEX idx_v1_nodes_id ON v1_nodes(node_id);
CREATE INDEX idx_v1_nodes_label ON v1_nodes(label);
CREATE INDEX idx_v1_edges_from ON v1_edges(from_node);
CREATE INDEX idx_v1_edges_to ON v1_edges(to_node);
CREATE INDEX idx_v1_edges_label ON v1_edges(label);

\echo V1 recomputed - row counts:
SELECT 'v1_nodes' AS tbl, COUNT(*) FROM v1_nodes
UNION ALL SELECT 'v1_edges', COUNT(*) FROM v1_edges;

\echo ================================================================
\echo NAIVE RECOMPUTATION - INDEX_v1 (SSR)
\echo ================================================================

DROP TABLE IF EXISTS INDEX_v1 CASCADE;

CREATE TABLE INDEX_v1 AS
SELECT DISTINCT
    n_person1._0 AS person1, n_person2._0 AS person2,
    e1._0 AS e1, n_comment._0 AS comment,
    e3._0 AS e3, e4._0 AS e4, n_post._0 AS post, e5._0 AS e5
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400;

CREATE INDEX idx_index_v1_person1 ON INDEX_v1 (person1);

\echo INDEX_v1 recomputed - row count:
SELECT COUNT(*) FROM INDEX_v1;

\timing off
