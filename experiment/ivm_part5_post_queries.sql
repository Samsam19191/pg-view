-- ================================================================
-- LSQB IVM Experiment - Part 5: Post-Update Queries
-- ================================================================

\timing on

\echo ================================================================
\echo POST-UPDATE QUERIES (after maintenance)
\echo ================================================================

-- ----------------------------------------
-- Q1 Post-Update
-- ----------------------------------------
\echo Q1(G) - Base graph (post-update):
SELECT COUNT(DISTINCT n_person1._0) AS result
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' AND n_person1._0 >= 723388 AND n_person1._0 <= 723415;

\echo Q1(MV) - Materialized view (post-update):
SELECT COUNT(DISTINCT n_person1.node_id) AS result
FROM v1_nodes n_person1
JOIN v1_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'KNOWS'
JOIN v1_nodes n_person2 ON e1.to_node = n_person2.node_id AND n_person2.label = 'Person'
JOIN v1_edges e3 ON e3.to_node = n_person1.node_id AND e3.label = 'HASCREATOR'
JOIN v1_nodes n_comment ON e3.from_node = n_comment.node_id AND n_comment.label = 'Comment'
JOIN v1_edges e4 ON e4.from_node = n_comment.node_id AND e4.label = 'REPLYOF'
JOIN v1_nodes n_post ON e4.to_node = n_post.node_id AND n_post.label = 'Post'
JOIN v1_edges e5 ON e5.from_node = n_post.node_id AND e5.to_node = n_person2.node_id AND e5.label = 'HASCREATOR'
WHERE n_person1.label = 'Person' AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723415;

\echo Q1(SSR) - SSR index (post-update):
SELECT COUNT(DISTINCT person1) AS result FROM INDEX_v1 
WHERE person1 >= 723388 AND person1 <= 723415;

-- ----------------------------------------
-- Q2 Post-Update
-- ----------------------------------------
\echo Q2(G) - Base graph (post-update):
SELECT COUNT(DISTINCT n_person1._0) AS result
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'ISLOCATEDIN'
JOIN n_g n_city1 ON e1._2 = n_city1._0 AND n_city1._1 = 'City'
JOIN e_g e2 ON e2._1 = n_city1._0 AND e2._3 = 'ISPARTOF'
JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
JOIN n_g n_person2 ON n_person2._1 = 'Person'
JOIN e_g e3 ON e3._1 = n_person2._0 AND e3._3 = 'ISLOCATEDIN'
JOIN n_g n_city2 ON e3._2 = n_city2._0 AND n_city2._1 = 'City'
JOIN e_g e4 ON e4._1 = n_city2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
JOIN n_g n_person3 ON n_person3._1 = 'Person'
JOIN e_g e5 ON e5._1 = n_person3._0 AND e5._3 = 'ISLOCATEDIN'
JOIN n_g n_city3 ON e5._2 = n_city3._0 AND n_city3._1 = 'City'
JOIN e_g e6 ON e6._1 = n_city3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
JOIN e_g e7 ON e7._1 = n_person1._0 AND e7._2 = n_person2._0 AND e7._3 = 'KNOWS'
JOIN e_g e9 ON e9._1 = n_person2._0 AND e9._2 = n_person3._0 AND e9._3 = 'KNOWS'
WHERE n_person1._1 = 'Person' AND n_person1._0 >= 723388 AND n_person1._0 <= 723395;

\echo Q2(MV) - Materialized view (post-update):
SELECT COUNT(DISTINCT n_person1.node_id) AS result
FROM v2_nodes n_person1
JOIN v2_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'ISLOCATEDIN'
JOIN v2_nodes n_city1 ON e1.to_node = n_city1.node_id AND n_city1.label = 'City'
JOIN v2_edges e2 ON e2.from_node = n_city1.node_id AND e2.label = 'ISPARTOF'
JOIN v2_nodes n_country ON e2.to_node = n_country.node_id AND n_country.label = 'Country'
JOIN v2_nodes n_person2 ON n_person2.label = 'Person'
JOIN v2_edges e3 ON e3.from_node = n_person2.node_id AND e3.label = 'ISLOCATEDIN'
JOIN v2_nodes n_city2 ON e3.to_node = n_city2.node_id AND n_city2.label = 'City'
JOIN v2_edges e4 ON e4.from_node = n_city2.node_id AND e4.to_node = n_country.node_id AND e4.label = 'ISPARTOF'
JOIN v2_nodes n_person3 ON n_person3.label = 'Person'
JOIN v2_edges e5 ON e5.from_node = n_person3.node_id AND e5.label = 'ISLOCATEDIN'
JOIN v2_nodes n_city3 ON e5.to_node = n_city3.node_id AND n_city3.label = 'City'
JOIN v2_edges e6 ON e6.from_node = n_city3.node_id AND e6.to_node = n_country.node_id AND e6.label = 'ISPARTOF'
JOIN v2_edges e7 ON e7.from_node = n_person1.node_id AND e7.to_node = n_person2.node_id AND e7.label = 'KNOWS'
JOIN v2_edges e9 ON e9.from_node = n_person2.node_id AND e9.to_node = n_person3.node_id AND e9.label = 'KNOWS'
WHERE n_person1.label = 'Person' AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723395;

\echo Q2(SSR) - SSR index (post-update):
SELECT COUNT(DISTINCT person1) AS result FROM INDEX_v2 
WHERE person1 >= 723388 AND person1 <= 723395;

\timing off

\echo ================================================================
\echo SANITY CHECK - Q1(G) should match Q1(MV) and Q1(SSR)
\echo ================================================================
SELECT 'Q1(G)' AS mode, COUNT(DISTINCT n_person1._0) AS result
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' AND n_person1._0 >= 723388 AND n_person1._0 <= 723415
UNION ALL
SELECT 'Q1(MV)', COUNT(DISTINCT node_id)
FROM v1_nodes n_person1
JOIN v1_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'KNOWS'
JOIN v1_nodes n_person2 ON e1.to_node = n_person2.node_id AND n_person2.label = 'Person'
JOIN v1_edges e3 ON e3.to_node = n_person1.node_id AND e3.label = 'HASCREATOR'
JOIN v1_nodes n_comment ON e3.from_node = n_comment.node_id AND n_comment.label = 'Comment'
JOIN v1_edges e4 ON e4.from_node = n_comment.node_id AND e4.label = 'REPLYOF'
JOIN v1_nodes n_post ON e4.to_node = n_post.node_id AND n_post.label = 'Post'
JOIN v1_edges e5 ON e5.from_node = n_post.node_id AND e5.to_node = n_person2.node_id AND e5.label = 'HASCREATOR'
WHERE n_person1.label = 'Person' AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723415
UNION ALL
SELECT 'Q1(SSR)', COUNT(DISTINCT person1) FROM INDEX_v1 
WHERE person1 >= 723388 AND person1 <= 723415;

\echo ================================================================
\echo SANITY CHECK - Q2(G) should match Q2(MV) and Q2(SSR)
\echo ================================================================
SELECT 'Q2(G)' AS mode, COUNT(DISTINCT n_person1._0) AS result
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'ISLOCATEDIN'
JOIN n_g n_city1 ON e1._2 = n_city1._0 AND n_city1._1 = 'City'
JOIN e_g e2 ON e2._1 = n_city1._0 AND e2._3 = 'ISPARTOF'
JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
JOIN n_g n_person2 ON n_person2._1 = 'Person'
JOIN e_g e3 ON e3._1 = n_person2._0 AND e3._3 = 'ISLOCATEDIN'
JOIN n_g n_city2 ON e3._2 = n_city2._0 AND n_city2._1 = 'City'
JOIN e_g e4 ON e4._1 = n_city2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
JOIN n_g n_person3 ON n_person3._1 = 'Person'
JOIN e_g e5 ON e5._1 = n_person3._0 AND e5._3 = 'ISLOCATEDIN'
JOIN n_g n_city3 ON e5._2 = n_city3._0 AND n_city3._1 = 'City'
JOIN e_g e6 ON e6._1 = n_city3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
JOIN e_g e7 ON e7._1 = n_person1._0 AND e7._2 = n_person2._0 AND e7._3 = 'KNOWS'
JOIN e_g e9 ON e9._1 = n_person2._0 AND e9._2 = n_person3._0 AND e9._3 = 'KNOWS'
WHERE n_person1._1 = 'Person' AND n_person1._0 >= 723388 AND n_person1._0 <= 723395
UNION ALL
SELECT 'Q2(MV)', COUNT(DISTINCT node_id)
FROM v2_nodes n_person1
JOIN v2_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'ISLOCATEDIN'
JOIN v2_nodes n_city1 ON e1.to_node = n_city1.node_id AND n_city1.label = 'City'
JOIN v2_edges e2 ON e2.from_node = n_city1.node_id AND e2.label = 'ISPARTOF'
JOIN v2_nodes n_country ON e2.to_node = n_country.node_id AND n_country.label = 'Country'
JOIN v2_nodes n_person2 ON n_person2.label = 'Person'
JOIN v2_edges e3 ON e3.from_node = n_person2.node_id AND e3.label = 'ISLOCATEDIN'
JOIN v2_nodes n_city2 ON e3.to_node = n_city2.node_id AND n_city2.label = 'City'
JOIN v2_edges e4 ON e4.from_node = n_city2.node_id AND e4.to_node = n_country.node_id AND e4.label = 'ISPARTOF'
JOIN v2_nodes n_person3 ON n_person3.label = 'Person'
JOIN v2_edges e5 ON e5.from_node = n_person3.node_id AND e5.label = 'ISLOCATEDIN'
JOIN v2_nodes n_city3 ON e5.to_node = n_city3.node_id AND n_city3.label = 'City'
JOIN v2_edges e6 ON e6.from_node = n_city3.node_id AND e6.to_node = n_country.node_id AND e6.label = 'ISPARTOF'
JOIN v2_edges e7 ON e7.from_node = n_person1.node_id AND e7.to_node = n_person2.node_id AND e7.label = 'KNOWS'
JOIN v2_edges e9 ON e9.from_node = n_person2.node_id AND e9.to_node = n_person3.node_id AND e9.label = 'KNOWS'
WHERE n_person1.label = 'Person' AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723395
UNION ALL
SELECT 'Q2(SSR)', COUNT(DISTINCT person1) FROM INDEX_v2 
WHERE person1 >= 723388 AND person1 <= 723395;
