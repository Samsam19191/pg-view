-- ================================================================
-- PROV Dataset: Schema Creation and Data Loading
-- ================================================================
-- Based on the SIGMOD 2024 paper "Implementation Strategies for 
-- Views over Property Graphs" by Han & Ives
--
-- PROV Schema (Wikipedia Talk edits):
--   Node labels:
--     R (Revision) - Activity
--     U (User) - Agent  
--     E (Entity) - Article version
--
--   Edge labels:
--     DERBY - isDerivedFrom (E -> E)
--     USED - used (R -> E)
--     GENBY - isGeneratedBy (E -> R)
--     ASSOC - wasAssociatedWith (R -> U)
-- ================================================================

-- Create database if needed (run separately):
-- CREATE DATABASE pgview_prov;

\echo ================================================================
\echo Creating PROV Schema
\echo ================================================================

-- Drop existing tables
DROP TABLE IF EXISTS ep_g CASCADE;
DROP TABLE IF EXISTS np_g CASCADE;
DROP TABLE IF EXISTS e_g CASCADE;
DROP TABLE IF EXISTS n_g CASCADE;

-- Node table: N_g(node_id, label)
CREATE TABLE n_g (
    _0 INT,           -- node_id
    _1 VARCHAR(1024)  -- label (R, U, E)
);

-- Edge table: E_g(edge_id, from, to, label)
CREATE TABLE e_g (
    _0 INT,           -- edge_id
    _1 INT,           -- from_node
    _2 INT,           -- to_node
    _3 VARCHAR(1024)  -- label (DERBY, USED, GENBY, ASSOC)
);

-- Node properties (empty for PROV)
CREATE TABLE np_g (
    _0 INT,
    _1 VARCHAR(1024),
    _2 VARCHAR(1024)
);

-- Edge properties (empty for PROV)
CREATE TABLE ep_g (
    _0 INT,
    _1 VARCHAR(1024),
    _2 VARCHAR(1024)
);

\echo ================================================================
\echo Loading PROV Data from CSV
\echo ================================================================

\timing on

-- Load nodes
\echo Loading nodes...
\copy n_g(_0, _1) FROM 'dataset/targets/prov/node.csv' WITH (FORMAT csv, HEADER true);

-- Load edges
\echo Loading edges...
\copy e_g(_0, _1, _2, _3) FROM 'dataset/targets/prov/edge.csv' WITH (FORMAT csv, HEADER true);

\timing off

\echo ================================================================
\echo Creating Indexes
\echo ================================================================

\timing on

CREATE INDEX idx_n_g_id ON n_g(_0);
CREATE INDEX idx_n_g_label ON n_g(_1);
CREATE INDEX idx_e_g_id ON e_g(_0);
CREATE INDEX idx_e_g_from ON e_g(_1);
CREATE INDEX idx_e_g_to ON e_g(_2);
CREATE INDEX idx_e_g_label ON e_g(_3);
CREATE INDEX idx_e_g_from_label ON e_g(_1, _3);
CREATE INDEX idx_e_g_to_label ON e_g(_2, _3);

ANALYZE n_g;
ANALYZE e_g;

\timing off

\echo ================================================================
\echo PROV Dataset Summary
\echo ================================================================

SELECT 'n_g' AS table_name, COUNT(*) AS row_count FROM n_g
UNION ALL
SELECT 'e_g', COUNT(*) FROM e_g;

\echo Node labels:
SELECT _1 AS label, COUNT(*) AS cnt FROM n_g GROUP BY _1 ORDER BY cnt DESC;

\echo Edge labels:
SELECT _3 AS label, COUNT(*) AS cnt FROM e_g GROUP BY _3 ORDER BY cnt DESC;

\echo Sample nodes:
SELECT * FROM n_g LIMIT 10;

\echo Sample edges:
SELECT * FROM e_g LIMIT 10;
