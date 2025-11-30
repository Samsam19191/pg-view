-- LSQB Views V1 and V2 Materialization
-- Based on the paper "Implementation Strategies for Views over Property Graphs" (SIGMOD 2024)

\echo ========================================
\echo Creating V1 Materialized View
\echo ========================================

-- V1: KNOWS relationship with Comment/Post pattern
-- Pattern:
--   (person1:Person)-[e1:KNOWS]->(person2:Person)
--   (comment:Comment)-[e3:HASCREATOR]->(person1:Person)
--   (comment:Comment)-[e4:REPLYOF]->(post:Post)
--   (post:Post)-[e5:HASCREATOR]->(person2:Person)
-- WHERE person1 <= 1159400

\timing on

DROP TABLE IF EXISTS V1_nodes CASCADE;
DROP TABLE IF EXISTS V1_edges CASCADE;

-- V1 Nodes: All distinct nodes involved in the pattern
CREATE TABLE V1_nodes AS
SELECT DISTINCT node_id, label FROM (
    -- person1 nodes
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

    -- person2 nodes
    SELECT n_person2._0 AS node_id, n_person2._1 AS label
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

    -- comment nodes
    SELECT n_comment._0 AS node_id, n_comment._1 AS label
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

    -- post nodes
    SELECT n_post._0 AS node_id, n_post._1 AS label
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

-- V1 Edges: All edges involved in the pattern
CREATE TABLE V1_edges AS
SELECT DISTINCT edge_id, from_node, to_node, label FROM (
    -- e1: KNOWS edges
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

    -- e3: HASCREATOR edges (comment -> person1)
    SELECT e3._0 AS edge_id, e3._1 AS from_node, e3._2 AS to_node, e3._3 AS label
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

    -- e4: REPLYOF edges (comment -> post)
    SELECT e4._0 AS edge_id, e4._1 AS from_node, e4._2 AS to_node, e4._3 AS label
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

    -- e5: HASCREATOR edges (post -> person2)
    SELECT e5._0 AS edge_id, e5._1 AS from_node, e5._2 AS to_node, e5._3 AS label
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

\echo V1 Nodes count:
SELECT COUNT(*) FROM V1_nodes;
\echo V1 Edges count:
SELECT COUNT(*) FROM V1_edges;
\echo V1 Node label distribution:
SELECT label, COUNT(*) FROM V1_nodes GROUP BY label ORDER BY COUNT(*) DESC;
\echo V1 Edge label distribution:
SELECT label, COUNT(*) FROM V1_edges GROUP BY label ORDER BY COUNT(*) DESC;


\echo ========================================
\echo Creating V2 Materialized View
\echo ========================================

-- V2: Location-based triangle pattern
-- Pattern:
--   (person1:Person)-[e1:ISLOCATEDIN]->(city1:City)
--   (city1:City)-[e2:ISPARTOF]->(country:Country)
--   (person2:Person)-[e3:ISLOCATEDIN]->(city2:City)
--   (city2:City)-[e4:ISPARTOF]->(country:Country)
--   (person3:Person)-[e5:ISLOCATEDIN]->(city3:City)
--   (city3:City)-[e6:ISPARTOF]->(country:Country)
--   (person1:Person)-[e7:KNOWS]->(person2:Person)
--   (person2:Person)-[e9:KNOWS]->(person3:Person)
-- WHERE person1 >= 1159373 AND person1 <= 1159400 (no-op for our dataset)

DROP TABLE IF EXISTS V2_nodes CASCADE;
DROP TABLE IF EXISTS V2_edges CASCADE;

-- V2 Nodes: All distinct nodes involved in the pattern
CREATE TABLE V2_nodes AS
SELECT DISTINCT node_id, label FROM (
    -- person1
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- person2
    SELECT n_p2._0 AS node_id, n_p2._1 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- person3
    SELECT n_p3._0 AS node_id, n_p3._1 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- city1
    SELECT n_c1._0 AS node_id, n_c1._1 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- city2
    SELECT n_c2._0 AS node_id, n_c2._1 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- city3
    SELECT n_c3._0 AS node_id, n_c3._1 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- country
    SELECT n_country._0 AS node_id, n_country._1 AS label
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
    WHERE n_p1._1 = 'Person'
) AS v2_all_nodes;

-- V2 Edges
CREATE TABLE V2_edges AS
SELECT DISTINCT edge_id, from_node, to_node, label FROM (
    -- e1: person1 ISLOCATEDIN city1
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- e2: city1 ISPARTOF country
    SELECT e2._0 AS edge_id, e2._1 AS from_node, e2._2 AS to_node, e2._3 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- e3: person2 ISLOCATEDIN city2
    SELECT e3._0 AS edge_id, e3._1 AS from_node, e3._2 AS to_node, e3._3 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- e4: city2 ISPARTOF country
    SELECT e4._0 AS edge_id, e4._1 AS from_node, e4._2 AS to_node, e4._3 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- e5: person3 ISLOCATEDIN city3
    SELECT e5._0 AS edge_id, e5._1 AS from_node, e5._2 AS to_node, e5._3 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- e6: city3 ISPARTOF country
    SELECT e6._0 AS edge_id, e6._1 AS from_node, e6._2 AS to_node, e6._3 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- e7: person1 KNOWS person2
    SELECT e7._0 AS edge_id, e7._1 AS from_node, e7._2 AS to_node, e7._3 AS label
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
    WHERE n_p1._1 = 'Person'

    UNION

    -- e9: person2 KNOWS person3
    SELECT e9._0 AS edge_id, e9._1 AS from_node, e9._2 AS to_node, e9._3 AS label
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
    WHERE n_p1._1 = 'Person'
) AS v2_all_edges;

\echo V2 Nodes count:
SELECT COUNT(*) FROM V2_nodes;
\echo V2 Edges count:
SELECT COUNT(*) FROM V2_edges;
\echo V2 Node label distribution:
SELECT label, COUNT(*) FROM V2_nodes GROUP BY label ORDER BY COUNT(*) DESC;
\echo V2 Edge label distribution:
SELECT label, COUNT(*) FROM V2_edges GROUP BY label ORDER BY COUNT(*) DESC;

\echo ========================================
\echo Summary
\echo ========================================
SELECT 'V1_nodes' as table_name, COUNT(*) as rows FROM V1_nodes
UNION ALL SELECT 'V1_edges', COUNT(*) FROM V1_edges
UNION ALL SELECT 'V2_nodes', COUNT(*) FROM V2_nodes
UNION ALL SELECT 'V2_edges', COUNT(*) FROM V2_edges
ORDER BY table_name;

