-- ================================================================
-- PROV SSR Tables for V3 Transformation View
-- ================================================================
-- SSR (Substitution Subgraph Relations) stores the variable bindings
-- for each matching pattern instance in the transformation view.
--
-- For V3, we need SSR tables for:
-- 1. INDEX_v3_r1: Rule 1 pattern (DERBY zoom-in to REVISION)
-- 2. INDEX_v3_r2: Rule 2 pattern (MULTIDERBY2 shortcut)
-- ================================================================

\timing on

\echo ================================================================
\echo Creating SSR for V3 Rule 1 (REVISION zoom-in)
\echo ================================================================

DROP TABLE IF EXISTS INDEX_v3_r1 CASCADE;

-- INDEX_v3_r1: Bindings for Rule 1
-- Pattern: (e2:E)-[d:DERBY]->(e1:E) WHERE e1 < 1000
-- Output: (a:REVISION)-[u:USED]->(e1:E), (e2:E)-[g:GENBY]->(a:REVISION)
-- Variables: e1, e2, d, a (synthetic), u (synthetic), g (synthetic)

CREATE TABLE INDEX_v3_r1 AS
SELECT DISTINCT
    e1._0 AS e1,
    e2._0 AS e2,
    d._0 AS d,
    10000000 + d._0 AS a,           -- SK("rev", d)
    20000000 + d._0 AS u,           -- SK("used", d)
    30000000 + d._0 AS g            -- SK("genby", d)
FROM e_g d
JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
WHERE d._3 = 'DERBY' AND e1._0 < 1000;

CREATE INDEX idx_index_v3_r1_e1 ON INDEX_v3_r1 (e1);
CREATE INDEX idx_index_v3_r1_a ON INDEX_v3_r1 (a);

\echo INDEX_v3_r1 row count:
SELECT COUNT(*) FROM INDEX_v3_r1;

\echo Sample INDEX_v3_r1 rows:
SELECT * FROM INDEX_v3_r1 LIMIT 5;


\echo ================================================================
\echo Creating SSR for V3 Rule 2 (MULTIDERBY2 shortcut)
\echo ================================================================

DROP TABLE IF EXISTS INDEX_v3_r2 CASCADE;

-- INDEX_v3_r2: Bindings for Rule 2
-- Pattern: (e2:E)-[d1:DERBY]->(e1:E), (e3:E)-[d2:DERBY]->(e2:E) WHERE e1 < 1000
-- Output: (e3:E)-[u:MULTIDERBY2]->(e1:E)
-- Variables: e1, e2, e3, d1, d2, u (synthetic)

CREATE TABLE INDEX_v3_r2 AS
SELECT DISTINCT
    e1._0 AS e1,
    e2._0 AS e2,
    e3._0 AS e3,
    d1._0 AS d1,
    d2._0 AS d2,
    40000000 + (e1._0 * 10000 + e3._0 % 10000) AS u  -- SK("multi", e1, e3)
FROM e_g d1
JOIN n_g e1 ON d1._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d1._1 = e2._0 AND e2._1 = 'E'
JOIN e_g d2 ON d2._2 = e2._0 AND d2._3 = 'DERBY'
JOIN n_g e3 ON d2._1 = e3._0 AND e3._1 = 'E'
WHERE d1._3 = 'DERBY' AND e1._0 < 1000;

CREATE INDEX idx_index_v3_r2_e1 ON INDEX_v3_r2 (e1);
CREATE INDEX idx_index_v3_r2_e3 ON INDEX_v3_r2 (e3);

\echo INDEX_v3_r2 row count:
SELECT COUNT(*) FROM INDEX_v3_r2;

\echo Sample INDEX_v3_r2 rows:
SELECT * FROM INDEX_v3_r2 LIMIT 5;


\echo ================================================================
\echo Creating Combined SSR for Q3 pattern
\echo ================================================================

DROP TABLE IF EXISTS INDEX_v3_q3 CASCADE;

-- INDEX_v3_q3: Combined SSR for Q3
-- Q3: MATCH (a:REVISION)-[u:USED]->(e1:E), (e2:E)-[g:GENBY]->(a:REVISION), (e2:E)-[e3:DERBY]->(e1:E)
-- This is essentially Rule 1 pattern with the original DERBY edge
-- But since V3 transforms DERBY to USED/GENBY, we need the SSR that captures this

CREATE TABLE INDEX_v3_q3 AS
SELECT DISTINCT
    e1._0 AS e1,
    e2._0 AS e2,
    10000000 + d._0 AS a,           -- REVISION node
    20000000 + d._0 AS u,           -- USED edge
    30000000 + d._0 AS g,           -- GENBY edge
    d._0 AS original_derby          -- Original DERBY edge (for reference)
FROM e_g d
JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
WHERE d._3 = 'DERBY' AND e1._0 < 1000;

CREATE INDEX idx_index_v3_q3_e1 ON INDEX_v3_q3 (e1);
CREATE INDEX idx_index_v3_q3_a ON INDEX_v3_q3 (a);

\echo INDEX_v3_q3 row count:
SELECT COUNT(*) FROM INDEX_v3_q3;


\echo ================================================================
\echo Creating SSR for Q4 pattern
\echo ================================================================

DROP TABLE IF EXISTS INDEX_v3_q4 CASCADE;

-- INDEX_v3_q4: SSR for Q4
-- Q4: MATCH (e2:E)-[u:MULTIDERBY2]->(e1:E) WHERE e1 < 1000
-- This directly uses the Rule 2 pattern

CREATE TABLE INDEX_v3_q4 AS
SELECT DISTINCT
    e1,
    e2 AS e2_src,  -- Source of MULTIDERBY2 (was e3 in rule)
    u
FROM INDEX_v3_r2;

CREATE INDEX idx_index_v3_q4_e1 ON INDEX_v3_q4 (e1);

\echo INDEX_v3_q4 row count:
SELECT COUNT(*) FROM INDEX_v3_q4;


\timing off

\echo ================================================================
\echo SSR Summary
\echo ================================================================

SELECT 'INDEX_v3_r1' AS table_name, COUNT(*) AS row_count FROM INDEX_v3_r1
UNION ALL
SELECT 'INDEX_v3_r2', COUNT(*) FROM INDEX_v3_r2
UNION ALL
SELECT 'INDEX_v3_q3', COUNT(*) FROM INDEX_v3_q3
UNION ALL
SELECT 'INDEX_v3_q4', COUNT(*) FROM INDEX_v3_q4;
