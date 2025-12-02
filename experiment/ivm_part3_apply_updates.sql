-- ================================================================
-- LSQB IVM Experiment - Part 3: Apply Updates
-- ================================================================
-- Insert delta edges into the base graph E_g
-- ================================================================

\timing on

\echo ================================================================
\echo APPLYING UPDATES TO BASE GRAPH
\echo ================================================================

\echo Before update - E_g count:
SELECT COUNT(*) AS e_g_before FROM e_g;

\echo Inserting delta edges into E_g...
INSERT INTO e_g (_0, _1, _2, _3)
SELECT _0, _1, _2, _3 FROM delta_edges;

\echo After update - E_g count:
SELECT COUNT(*) AS e_g_after FROM e_g;

\echo Delta applied:
SELECT 
    (SELECT COUNT(*) FROM e_g) - 6183839 AS edges_added;

\timing off
