-- ================================================================
-- PROV Transformation View V3: Materialization
-- ================================================================
-- Based on the paper's V3 definition:
--
-- CREATE VIEW v3 ON g WITH DEFAULT MAP (
--   MATCH (e2:E)-[d:DERBY]->(e1:E)
--   WHERE e1 < 1000
--   CONSTRUCT (a:REVISION)-[u:USED]->(e1:E), (e2:E)-[g:GENBY]->(a:REVISION)
--   SET a = SK("rev", d), u = SK("used", d), g = SK("genby", d)
--     UNION
--   MATCH (e2:E)-[d1:DERBY]->(e1:E), (e3:E)-[d2:DERBY]->(e2:E)
--   WHERE e1 < 1000
--   CONSTRUCT (e3:E)-[u:MULTIDERBY2]->(e1:E)
--   SET u = SK("multi", e1, e3)
-- )
--
-- This transformation view:
-- 1. Rule 1: Introduces REVISION nodes between DERBY edges (zoom-in)
--    - For each DERBY edge d: e2 -> e1
--    - Creates: REVISION(a) -USED-> e1, e2 -GENBY-> REVISION(a)
--    - Skolem function creates synthetic node from edge d
--
-- 2. Rule 2: Creates MULTIDERBY2 shortcut edges (zoom-out)
--    - For chains e3 -DERBY-> e2 -DERBY-> e1
--    - Creates: e3 -MULTIDERBY2-> e1 (skipping e2)
--
-- 3. DEFAULT MAP: Copies all other nodes/edges unchanged
-- ================================================================

\timing on

\echo ================================================================
\echo Creating PROV Transformation View V3
\echo ================================================================

DROP TABLE IF EXISTS v3_nodes CASCADE;
DROP TABLE IF EXISTS v3_edges CASCADE;

-- ================================================================
-- V3 NODES
-- ================================================================
\echo Creating V3_nodes...

CREATE TABLE v3_nodes AS
SELECT DISTINCT node_id, label FROM (
    -- DEFAULT MAP: Copy all E nodes (Entity) that appear in patterns
    SELECT n._0 AS node_id, n._1 AS label
    FROM n_g n
    WHERE n._1 = 'E'
    
    UNION
    
    -- Rule 1: Create synthetic REVISION nodes from DERBY edges
    -- SK("rev", d) creates a new REVISION node for each qualifying DERBY edge
    -- We use the edge_id as the Skolem key, offset to avoid collision
    SELECT 
        10000000 + d._0 AS node_id,  -- Skolem: offset edge_id for REVISION nodes
        'REVISION' AS label
    FROM e_g d
    JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
    JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
    WHERE d._3 = 'DERBY' AND e1._0 < 1000

) AS v3_all_nodes;

\echo V3_nodes created.

-- ================================================================
-- V3 EDGES
-- ================================================================
\echo Creating V3_edges...

CREATE TABLE v3_edges AS
SELECT DISTINCT edge_id, from_node, to_node, label FROM (
    -- DEFAULT MAP: Copy DERBY edges that are NOT transformed (e1 >= 1000)
    SELECT d._0 AS edge_id, d._1 AS from_node, d._2 AS to_node, d._3 AS label
    FROM e_g d
    JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
    JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
    WHERE d._3 = 'DERBY' AND e1._0 >= 1000
    
    UNION
    
    -- Rule 1a: Create USED edges (REVISION -> e1)
    -- SK("used", d) creates edge from synthetic REVISION to e1
    SELECT 
        20000000 + d._0 AS edge_id,      -- Skolem for USED edge
        10000000 + d._0 AS from_node,    -- From: REVISION node (SK("rev", d))
        e1._0 AS to_node,                -- To: e1 (Entity)
        'USED' AS label
    FROM e_g d
    JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
    JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
    WHERE d._3 = 'DERBY' AND e1._0 < 1000
    
    UNION
    
    -- Rule 1b: Create GENBY edges (e2 -> REVISION)
    -- SK("genby", d) creates edge from e2 to synthetic REVISION
    SELECT 
        30000000 + d._0 AS edge_id,      -- Skolem for GENBY edge
        e2._0 AS from_node,              -- From: e2 (Entity)
        10000000 + d._0 AS to_node,      -- To: REVISION node (SK("rev", d))
        'GENBY' AS label
    FROM e_g d
    JOIN n_g e1 ON d._2 = e1._0 AND e1._1 = 'E'
    JOIN n_g e2 ON d._1 = e2._0 AND e2._1 = 'E'
    WHERE d._3 = 'DERBY' AND e1._0 < 1000
    
    UNION
    
    -- Rule 2: Create MULTIDERBY2 edges (e3 -> e1, skipping e2)
    -- For chains: e3 -DERBY-> e2 -DERBY-> e1 where e1 < 1000
    -- SK("multi", e1, e3) creates a shortcut edge
    SELECT DISTINCT
        40000000 + (e1._0 * 10000 + e3._0 % 10000) AS edge_id,  -- Skolem for MULTIDERBY2
        e3._0 AS from_node,              -- From: e3 (Entity)
        e1._0 AS to_node,                -- To: e1 (Entity)
        'MULTIDERBY2' AS label
    FROM e_g d1
    JOIN n_g e1 ON d1._2 = e1._0 AND e1._1 = 'E'
    JOIN n_g e2 ON d1._1 = e2._0 AND e2._1 = 'E'
    JOIN e_g d2 ON d2._2 = e2._0 AND d2._3 = 'DERBY'
    JOIN n_g e3 ON d2._1 = e3._0 AND e3._1 = 'E'
    WHERE d1._3 = 'DERBY' AND e1._0 < 1000

) AS v3_all_edges;

\echo V3_edges created.

-- ================================================================
-- Create Indexes on V3 tables
-- ================================================================
\echo Creating indexes on V3 tables...

CREATE INDEX idx_v3_nodes_id ON v3_nodes(node_id);
CREATE INDEX idx_v3_nodes_label ON v3_nodes(label);
CREATE INDEX idx_v3_edges_id ON v3_edges(edge_id);
CREATE INDEX idx_v3_edges_from ON v3_edges(from_node);
CREATE INDEX idx_v3_edges_to ON v3_edges(to_node);
CREATE INDEX idx_v3_edges_label ON v3_edges(label);
CREATE INDEX idx_v3_edges_from_label ON v3_edges(from_node, label);
CREATE INDEX idx_v3_edges_to_label ON v3_edges(to_node, label);

ANALYZE v3_nodes;
ANALYZE v3_edges;

\timing off

\echo ================================================================
\echo V3 Transformation View Summary
\echo ================================================================

SELECT 'v3_nodes' AS table_name, COUNT(*) AS row_count FROM v3_nodes
UNION ALL
SELECT 'v3_edges', COUNT(*) FROM v3_edges;

\echo V3 Node labels:
SELECT label, COUNT(*) AS cnt FROM v3_nodes GROUP BY label ORDER BY cnt DESC;

\echo V3 Edge labels:
SELECT label, COUNT(*) AS cnt FROM v3_edges GROUP BY label ORDER BY cnt DESC;

\echo Sample V3 nodes (REVISION - synthetic):
SELECT * FROM v3_nodes WHERE label = 'REVISION' LIMIT 5;

\echo Sample V3 edges (MULTIDERBY2 - shortcut):
SELECT * FROM v3_edges WHERE label = 'MULTIDERBY2' LIMIT 5;

\echo Sample V3 edges (USED - from transformation):
SELECT * FROM v3_edges WHERE label = 'USED' LIMIT 5;

\echo Sample V3 edges (GENBY - from transformation):
SELECT * FROM v3_edges WHERE label = 'GENBY' LIMIT 5;
