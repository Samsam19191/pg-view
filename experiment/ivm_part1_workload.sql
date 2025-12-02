-- ================================================================
-- LSQB IVM Experiment - Part 1: Update Workload Definition
-- ================================================================
-- This script defines a deterministic, reproducible update workload
-- that will affect V1 and V2 patterns.
--
-- Update workload:
--   - 500 new KNOWS edges (affects V1 and V2)
--   - 300 new HASCREATOR edges (affects V1)
--   - 200 new ISLOCATEDIN edges (affects V2)
-- ================================================================

-- ========================================
-- STEP 1: Create staging tables for updates
-- ========================================

DROP TABLE IF EXISTS delta_edges CASCADE;

CREATE TABLE delta_edges (
    _0 INT,      -- edge_id
    _1 INT,      -- from_node
    _2 INT,      -- to_node
    _3 VARCHAR(1024)  -- label
);

-- ========================================
-- STEP 2: Generate KNOWS edges (500 edges)
-- Connects Person nodes in range [723388, 723888]
-- These affect both V1 (KNOWS in pattern) and V2 (KNOWS chain)
-- ========================================

INSERT INTO delta_edges
SELECT 
    6183839 + row_number() OVER () AS _0,  -- new edge IDs starting after max
    723388 + (i % 500) AS _1,               -- from: person in range
    723388 + ((i + 50) % 500) AS _2,        -- to: different person
    'KNOWS' AS _3
FROM generate_series(0, 499) AS i
WHERE 723388 + (i % 500) != 723388 + ((i + 50) % 500);  -- no self-loops

-- ========================================
-- STEP 3: Generate HASCREATOR edges (300 edges)
-- Connects Comment nodes to Person nodes
-- These affect V1 pattern
-- ========================================

INSERT INTO delta_edges
SELECT 
    6184339 + row_number() OVER () AS _0,  -- edge IDs continue
    1575 + (i * 2000) AS _1,                -- from: comment nodes (spread across range)
    723388 + (i % 300) AS _2,               -- to: person nodes
    'HASCREATOR' AS _3
FROM generate_series(0, 299) AS i
WHERE 1575 + (i * 2000) <= 699402;  -- within comment range

-- ========================================
-- STEP 4: Generate ISLOCATEDIN edges (200 edges)
-- Connects Person nodes to City nodes
-- These affect V2 pattern
-- ========================================

INSERT INTO delta_edges
SELECT 
    6184639 + row_number() OVER () AS _0,  -- edge IDs continue
    723388 + (i * 10) AS _1,                -- from: person nodes
    721934 + (i % 200) AS _2,               -- to: city nodes
    'ISLOCATEDIN' AS _3
FROM generate_series(0, 199) AS i
WHERE 723388 + (i * 10) <= 727287;  -- within person range

-- ========================================
-- Verify the delta
-- ========================================

\echo ========================================
\echo Delta edges summary:
\echo ========================================
SELECT _3 AS label, COUNT(*) AS cnt FROM delta_edges GROUP BY _3 ORDER BY _3;
SELECT COUNT(*) AS total_delta_edges FROM delta_edges;
