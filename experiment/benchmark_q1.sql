-- LSQB Q1 Benchmark: Three Execution Modes
-- Based on "Implementation Strategies for Views over Property Graphs" (SIGMOD 2024)
--
-- Q1 Pattern:
--   (person1:Person)-[e1:KNOWS]->(person2:Person)
--   (comment:Comment)-[e3:HASCREATOR]->(person1:Person)
--   (comment:Comment)-[e4:REPLYOF]->(post:Post)
--   (post:Post)-[e5:HASCREATOR]->(person2:Person)
-- WHERE person1 >= <low> AND person1 <= <high>
-- RETURN person1
--
-- Note: Original paper uses person1 >= 1159373 AND person1 <= 1159400
-- Our dataset has Person IDs 723388-727287, so we use equivalent range

\echo ================================================================
\echo Q1 Benchmark - LSQB
\echo ================================================================
\echo

-- Warm up the cache
SELECT COUNT(*) FROM n_g;
SELECT COUNT(*) FROM e_g;

\echo ================================================================
\echo Q1_base: Query over base graph G (N_g, E_g)
\echo ================================================================
\timing on

EXPLAIN ANALYZE
SELECT DISTINCT n_person1._0 AS person1
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' 
  AND n_person1._0 >= 723388 AND n_person1._0 <= 723415;

\echo
\echo Result count for Q1_base:
SELECT COUNT(DISTINCT n_person1._0) AS person1_count
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' 
  AND n_person1._0 >= 723388 AND n_person1._0 <= 723415;


\echo ================================================================
\echo Q1_mv: Query over materialized view MV (v1_nodes, v1_edges)
\echo ================================================================

EXPLAIN ANALYZE
SELECT DISTINCT n_person1.node_id AS person1
FROM v1_nodes n_person1
JOIN v1_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'KNOWS'
JOIN v1_nodes n_person2 ON e1.to_node = n_person2.node_id AND n_person2.label = 'Person'
JOIN v1_edges e3 ON e3.to_node = n_person1.node_id AND e3.label = 'HASCREATOR'
JOIN v1_nodes n_comment ON e3.from_node = n_comment.node_id AND n_comment.label = 'Comment'
JOIN v1_edges e4 ON e4.from_node = n_comment.node_id AND e4.label = 'REPLYOF'
JOIN v1_nodes n_post ON e4.to_node = n_post.node_id AND n_post.label = 'Post'
JOIN v1_edges e5 ON e5.from_node = n_post.node_id AND e5.to_node = n_person2.node_id AND e5.label = 'HASCREATOR'
WHERE n_person1.label = 'Person'
  AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723415;

\echo
\echo Result count for Q1_mv:
SELECT COUNT(DISTINCT n_person1.node_id) AS person1_count
FROM v1_nodes n_person1
JOIN v1_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'KNOWS'
JOIN v1_nodes n_person2 ON e1.to_node = n_person2.node_id AND n_person2.label = 'Person'
JOIN v1_edges e3 ON e3.to_node = n_person1.node_id AND e3.label = 'HASCREATOR'
JOIN v1_nodes n_comment ON e3.from_node = n_comment.node_id AND n_comment.label = 'Comment'
JOIN v1_edges e4 ON e4.from_node = n_comment.node_id AND e4.label = 'REPLYOF'
JOIN v1_nodes n_post ON e4.to_node = n_post.node_id AND n_post.label = 'Post'
JOIN v1_edges e5 ON e5.from_node = n_post.node_id AND e5.to_node = n_person2.node_id AND e5.label = 'HASCREATOR'
WHERE n_person1.label = 'Person'
  AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723415;


\echo ================================================================
\echo Q1_ssr: Query using SSR index only (INDEX_v1)
\echo ================================================================

-- SSR approach: The pattern match is already pre-computed in INDEX_v1
-- We just need to filter on person1 and return distinct person1 values

EXPLAIN ANALYZE
SELECT DISTINCT person1
FROM INDEX_v1
WHERE person1 >= 723388 AND person1 <= 723415;

\echo
\echo Result count for Q1_ssr:
SELECT COUNT(DISTINCT person1) AS person1_count
FROM INDEX_v1
WHERE person1 >= 723388 AND person1 <= 723415;


\echo ================================================================
\echo Summary: Execution Time Comparison
\echo ================================================================
\echo Run each query 3 times for more stable timing:
\echo

\echo --- Q1_base (run 1) ---
SELECT DISTINCT n_person1._0 AS person1
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' 
  AND n_person1._0 >= 723388 AND n_person1._0 <= 723415;

\echo --- Q1_base (run 2) ---
SELECT DISTINCT n_person1._0 AS person1
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' 
  AND n_person1._0 >= 723388 AND n_person1._0 <= 723415;

\echo --- Q1_base (run 3) ---
SELECT DISTINCT n_person1._0 AS person1
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' 
  AND n_person1._0 >= 723388 AND n_person1._0 <= 723415;

\echo --- Q1_mv (run 1) ---
SELECT DISTINCT n_person1.node_id AS person1
FROM v1_nodes n_person1
JOIN v1_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'KNOWS'
JOIN v1_nodes n_person2 ON e1.to_node = n_person2.node_id AND n_person2.label = 'Person'
JOIN v1_edges e3 ON e3.to_node = n_person1.node_id AND e3.label = 'HASCREATOR'
JOIN v1_nodes n_comment ON e3.from_node = n_comment.node_id AND n_comment.label = 'Comment'
JOIN v1_edges e4 ON e4.from_node = n_comment.node_id AND e4.label = 'REPLYOF'
JOIN v1_nodes n_post ON e4.to_node = n_post.node_id AND n_post.label = 'Post'
JOIN v1_edges e5 ON e5.from_node = n_post.node_id AND e5.to_node = n_person2.node_id AND e5.label = 'HASCREATOR'
WHERE n_person1.label = 'Person'
  AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723415;

\echo --- Q1_mv (run 2) ---
SELECT DISTINCT n_person1.node_id AS person1
FROM v1_nodes n_person1
JOIN v1_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'KNOWS'
JOIN v1_nodes n_person2 ON e1.to_node = n_person2.node_id AND n_person2.label = 'Person'
JOIN v1_edges e3 ON e3.to_node = n_person1.node_id AND e3.label = 'HASCREATOR'
JOIN v1_nodes n_comment ON e3.from_node = n_comment.node_id AND n_comment.label = 'Comment'
JOIN v1_edges e4 ON e4.from_node = n_comment.node_id AND e4.label = 'REPLYOF'
JOIN v1_nodes n_post ON e4.to_node = n_post.node_id AND n_post.label = 'Post'
JOIN v1_edges e5 ON e5.from_node = n_post.node_id AND e5.to_node = n_person2.node_id AND e5.label = 'HASCREATOR'
WHERE n_person1.label = 'Person'
  AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723415;

\echo --- Q1_mv (run 3) ---
SELECT DISTINCT n_person1.node_id AS person1
FROM v1_nodes n_person1
JOIN v1_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'KNOWS'
JOIN v1_nodes n_person2 ON e1.to_node = n_person2.node_id AND n_person2.label = 'Person'
JOIN v1_edges e3 ON e3.to_node = n_person1.node_id AND e3.label = 'HASCREATOR'
JOIN v1_nodes n_comment ON e3.from_node = n_comment.node_id AND n_comment.label = 'Comment'
JOIN v1_edges e4 ON e4.from_node = n_comment.node_id AND e4.label = 'REPLYOF'
JOIN v1_nodes n_post ON e4.to_node = n_post.node_id AND n_post.label = 'Post'
JOIN v1_edges e5 ON e5.from_node = n_post.node_id AND e5.to_node = n_person2.node_id AND e5.label = 'HASCREATOR'
WHERE n_person1.label = 'Person'
  AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723415;

\echo --- Q1_ssr (run 1) ---
SELECT DISTINCT person1 FROM INDEX_v1 WHERE person1 >= 723388 AND person1 <= 723415;

\echo --- Q1_ssr (run 2) ---
SELECT DISTINCT person1 FROM INDEX_v1 WHERE person1 >= 723388 AND person1 <= 723415;

\echo --- Q1_ssr (run 3) ---
SELECT DISTINCT person1 FROM INDEX_v1 WHERE person1 >= 723388 AND person1 <= 723415;

\timing off
\echo
\echo ================================================================
\echo Benchmark Complete
\echo ================================================================

