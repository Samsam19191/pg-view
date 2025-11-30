-- LSQB SSR (Subgraph Substitution Relations) for V1 and V2
-- Based on the paper "Implementation Strategies for Views over Property Graphs" (SIGMOD 2024)
--
-- SSR stores the variable bindings for each matching pattern instance.
-- For standard views (no MAP clauses), SSR captures the tuple of node/edge IDs 
-- that satisfy the pattern match.

\echo ========================================
\echo Creating SSR for V1
\echo ========================================
\timing on

DROP TABLE IF EXISTS INDEX_v1 CASCADE;

-- V1 SSR: stores bindings for (person1, person2, e1, comment, e3, e4, post, e5)
-- Pattern:
--   (person1:Person)-[e1:KNOWS]->(person2:Person)
--   (comment:Comment)-[e3:HASCREATOR]->(person1:Person)
--   (comment:Comment)-[e4:REPLYOF]->(post:Post)
--   (post:Post)-[e5:HASCREATOR]->(person2:Person)

CREATE TABLE INDEX_v1 AS
SELECT DISTINCT
    n_person1._0 AS person1,
    n_person2._0 AS person2,
    e1._0 AS e1,
    n_comment._0 AS comment,
    e3._0 AS e3,
    e4._0 AS e4,
    n_post._0 AS post,
    e5._0 AS e5
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' AND n_person1._0 <= 1159400;

-- Create index on SSR for faster query rewriting
CREATE INDEX idx_index_v1_person1 ON INDEX_v1 (person1);

\echo V1 SSR row count:
SELECT COUNT(*) FROM INDEX_v1;

\echo V1 SSR sample rows:
SELECT * FROM INDEX_v1 LIMIT 5;


\echo ========================================
\echo Creating SSR for V2
\echo ========================================

DROP TABLE IF EXISTS INDEX_v2 CASCADE;

-- V2 SSR: stores bindings for (person1, person2, person3, city1, city2, city3, country, e1-e9)
-- Pattern:
--   (person1:Person)-[e1:ISLOCATEDIN]->(city1:City)
--   (city1:City)-[e2:ISPARTOF]->(country:Country)
--   (person2:Person)-[e3:ISLOCATEDIN]->(city2:City)
--   (city2:City)-[e4:ISPARTOF]->(country:Country)
--   (person3:Person)-[e5:ISLOCATEDIN]->(city3:City)
--   (city3:City)-[e6:ISPARTOF]->(country:Country)
--   (person1:Person)-[e7:KNOWS]->(person2:Person)
--   (person2:Person)-[e9:KNOWS]->(person3:Person)

CREATE TABLE INDEX_v2 AS
SELECT DISTINCT
    n_p1._0 AS person1,
    n_p2._0 AS person2,
    n_p3._0 AS person3,
    n_c1._0 AS city1,
    n_c2._0 AS city2,
    n_c3._0 AS city3,
    n_country._0 AS country,
    e1._0 AS e1,
    e2._0 AS e2,
    e3._0 AS e3,
    e4._0 AS e4,
    e5._0 AS e5,
    e6._0 AS e6,
    e7._0 AS e7,
    e9._0 AS e9
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
WHERE n_p1._1 = 'Person';

-- Create index on SSR for faster query rewriting  
CREATE INDEX idx_index_v2_person1 ON INDEX_v2 (person1);

\echo V2 SSR row count:
SELECT COUNT(*) FROM INDEX_v2;

\echo V2 SSR sample rows:
SELECT * FROM INDEX_v2 LIMIT 5;


\echo ========================================
\echo SSR Summary
\echo ========================================

SELECT 'INDEX_v1' as ssr_table, COUNT(*) as rows FROM INDEX_v1
UNION ALL 
SELECT 'INDEX_v2', COUNT(*) FROM INDEX_v2
ORDER BY ssr_table;

