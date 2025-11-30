-- ================================================================
-- LSQB Q2 Benchmark: Base Graph vs Materialized View vs SSR
-- ================================================================
-- 
-- Q2 Definition (from docs/workload.md):
--   MATCH 
--     (person1:Person)-[e1:ISLOCATEDIN]->(city1:City),
--     (city1:City)-[e2:ISPARTOF]->(country:Country),
--     (person2:Person)-[e3:ISLOCATEDIN]->(city2:City),
--     (city2:City)-[e4:ISPARTOF]->(country:Country),
--     (person3:Person)-[e5:ISLOCATEDIN]->(city3:City),
--     (city3:City)-[e6:ISPARTOF]->(country:Country),
--     (person1:Person)-[e7:KNOWS]->(person2:Person),
--     (person2:Person)-[e9:KNOWS]->(person3:Person)
--   FROM v2 
--   WHERE person1 >= 723388 AND person1 <= 723395
--   RETURN (person1)
--
-- This is a triangle pattern where three persons are in cities 
-- that are all part of the same country, with KNOWS edges between them.
-- ================================================================

\timing on

\echo ================================================================
\echo Q2(G) - Query over base graph tables (N_g, E_g)
\echo ================================================================
\echo Running 3 iterations...

-- Q2 on base graph: 8-way join pattern
-- Pattern: person1 -ISLOCATEDIN-> city1 -ISPARTOF-> country
--          person2 -ISLOCATEDIN-> city2 -ISPARTOF-> country (same)
--          person3 -ISLOCATEDIN-> city3 -ISPARTOF-> country (same)
--          person1 -KNOWS-> person2 -KNOWS-> person3

SELECT COUNT(DISTINCT n_person1._0) AS result
FROM n_g n_person1
-- person1 -> city1 via ISLOCATEDIN
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'ISLOCATEDIN'
JOIN n_g n_city1 ON e1._2 = n_city1._0 AND n_city1._1 = 'City'
-- city1 -> country via ISPARTOF
JOIN e_g e2 ON e2._1 = n_city1._0 AND e2._3 = 'ISPARTOF'
JOIN n_g n_country ON e2._2 = n_country._0 AND n_country._1 = 'Country'
-- person2 -> city2 via ISLOCATEDIN
JOIN n_g n_person2 ON n_person2._1 = 'Person'
JOIN e_g e3 ON e3._1 = n_person2._0 AND e3._3 = 'ISLOCATEDIN'
JOIN n_g n_city2 ON e3._2 = n_city2._0 AND n_city2._1 = 'City'
-- city2 -> same country via ISPARTOF
JOIN e_g e4 ON e4._1 = n_city2._0 AND e4._2 = n_country._0 AND e4._3 = 'ISPARTOF'
-- person3 -> city3 via ISLOCATEDIN
JOIN n_g n_person3 ON n_person3._1 = 'Person'
JOIN e_g e5 ON e5._1 = n_person3._0 AND e5._3 = 'ISLOCATEDIN'
JOIN n_g n_city3 ON e5._2 = n_city3._0 AND n_city3._1 = 'City'
-- city3 -> same country via ISPARTOF
JOIN e_g e6 ON e6._1 = n_city3._0 AND e6._2 = n_country._0 AND e6._3 = 'ISPARTOF'
-- person1 -> person2 via KNOWS
JOIN e_g e7 ON e7._1 = n_person1._0 AND e7._2 = n_person2._0 AND e7._3 = 'KNOWS'
-- person2 -> person3 via KNOWS
JOIN e_g e9 ON e9._1 = n_person2._0 AND e9._2 = n_person3._0 AND e9._3 = 'KNOWS'
WHERE n_person1._1 = 'Person' 
  AND n_person1._0 >= 723388 AND n_person1._0 <= 723395;

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
WHERE n_person1._1 = 'Person' 
  AND n_person1._0 >= 723388 AND n_person1._0 <= 723395;

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
WHERE n_person1._1 = 'Person' 
  AND n_person1._0 >= 723388 AND n_person1._0 <= 723395;


\echo ================================================================
\echo Q2(MV) - Query over materialized V2 tables (v2_nodes, v2_edges)
\echo ================================================================
\echo Running 3 iterations...

SELECT COUNT(DISTINCT n_person1.node_id) AS result
FROM v2_nodes n_person1
-- person1 -> city1 via ISLOCATEDIN
JOIN v2_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'ISLOCATEDIN'
JOIN v2_nodes n_city1 ON e1.to_node = n_city1.node_id AND n_city1.label = 'City'
-- city1 -> country via ISPARTOF
JOIN v2_edges e2 ON e2.from_node = n_city1.node_id AND e2.label = 'ISPARTOF'
JOIN v2_nodes n_country ON e2.to_node = n_country.node_id AND n_country.label = 'Country'
-- person2 -> city2 via ISLOCATEDIN
JOIN v2_nodes n_person2 ON n_person2.label = 'Person'
JOIN v2_edges e3 ON e3.from_node = n_person2.node_id AND e3.label = 'ISLOCATEDIN'
JOIN v2_nodes n_city2 ON e3.to_node = n_city2.node_id AND n_city2.label = 'City'
-- city2 -> same country via ISPARTOF
JOIN v2_edges e4 ON e4.from_node = n_city2.node_id AND e4.to_node = n_country.node_id AND e4.label = 'ISPARTOF'
-- person3 -> city3 via ISLOCATEDIN
JOIN v2_nodes n_person3 ON n_person3.label = 'Person'
JOIN v2_edges e5 ON e5.from_node = n_person3.node_id AND e5.label = 'ISLOCATEDIN'
JOIN v2_nodes n_city3 ON e5.to_node = n_city3.node_id AND n_city3.label = 'City'
-- city3 -> same country via ISPARTOF
JOIN v2_edges e6 ON e6.from_node = n_city3.node_id AND e6.to_node = n_country.node_id AND e6.label = 'ISPARTOF'
-- person1 -> person2 via KNOWS
JOIN v2_edges e7 ON e7.from_node = n_person1.node_id AND e7.to_node = n_person2.node_id AND e7.label = 'KNOWS'
-- person2 -> person3 via KNOWS
JOIN v2_edges e9 ON e9.from_node = n_person2.node_id AND e9.to_node = n_person3.node_id AND e9.label = 'KNOWS'
WHERE n_person1.label = 'Person' 
  AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723395;

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
WHERE n_person1.label = 'Person' 
  AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723395;

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
WHERE n_person1.label = 'Person' 
  AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723395;


\echo ================================================================
\echo Q2(SSR) - Query using SSR INDEX_v2 only (no joins)
\echo ================================================================
\echo Running 3 iterations...

SELECT COUNT(DISTINCT person1) AS result
FROM INDEX_v2
WHERE person1 >= 723388 AND person1 <= 723395;

SELECT COUNT(DISTINCT person1) AS result
FROM INDEX_v2
WHERE person1 >= 723388 AND person1 <= 723395;

SELECT COUNT(DISTINCT person1) AS result
FROM INDEX_v2
WHERE person1 >= 723388 AND person1 <= 723395;


\echo ================================================================
\echo SANITY CHECK - All three approaches should return the same count
\echo ================================================================

\echo Q2(G) result:
SELECT COUNT(DISTINCT n_person1._0) AS "Q2(G)"
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
WHERE n_person1._1 = 'Person' 
  AND n_person1._0 >= 723388 AND n_person1._0 <= 723395;

\echo Q2(MV) result:
SELECT COUNT(DISTINCT n_person1.node_id) AS "Q2(MV)"
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
WHERE n_person1.label = 'Person' 
  AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723395;

\echo Q2(SSR) result:
SELECT COUNT(DISTINCT person1) AS "Q2(SSR)"
FROM INDEX_v2
WHERE person1 >= 723388 AND person1 <= 723395;

\timing off

\echo ================================================================
\echo BENCHMARK COMPLETE
\echo ================================================================
