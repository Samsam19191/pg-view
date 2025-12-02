-- ================================================================
-- LSQB IVM Experiment - Part 6: Rollback (cleanup)
-- ================================================================
-- Removes the delta edges to restore original state
-- ================================================================

\echo ================================================================
\echo ROLLING BACK UPDATES
\echo ================================================================

\echo Before rollback - E_g count:
SELECT COUNT(*) AS e_g_before FROM e_g;

-- Remove delta edges
DELETE FROM e_g WHERE _0 >= 6183839;

\echo After rollback - E_g count:
SELECT COUNT(*) AS e_g_after FROM e_g;

-- Drop delta staging table
DROP TABLE IF EXISTS delta_edges;

\echo Rollback complete.
