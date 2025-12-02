-- ================================================================
-- Workload Mix Experiment - Setup and Helper Functions
-- ================================================================
-- Creates tables and functions for workload mix experiments
-- ================================================================

-- Results table to capture all timings
DROP TABLE IF EXISTS workload_results CASCADE;
CREATE TABLE workload_results (
    experiment_id VARCHAR(50),
    operation_num INT,
    operation_type VARCHAR(20),  -- 'query' or 'update'
    query_mode VARCHAR(10),      -- 'G', 'MV', 'SSR'
    time_ms NUMERIC,
    result_count INT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Delta edges staging for updates
DROP TABLE IF EXISTS update_batch CASCADE;
CREATE TABLE update_batch (
    batch_id INT,
    _0 INT,      -- edge_id
    _1 INT,      -- from_node
    _2 INT,      -- to_node
    _3 VARCHAR(1024)  -- label
);

-- Pre-generate 100 update batches (10 edges each)
-- Each batch inserts edges that could affect V1 pattern
\echo Generating update batches...

DO $$
DECLARE
    base_edge_id INT := 7000000;
    batch INT;
    i INT;
BEGIN
    FOR batch IN 0..99 LOOP
        FOR i IN 0..9 LOOP
            -- Alternate between KNOWS, HASCREATOR, REPLYOF edges
            IF i % 3 = 0 THEN
                -- KNOWS edge between persons
                INSERT INTO update_batch VALUES (
                    batch,
                    base_edge_id + batch * 10 + i,
                    723388 + (batch % 100),      -- from: person
                    723388 + ((batch + 50) % 100), -- to: person
                    'KNOWS'
                );
            ELSIF i % 3 = 1 THEN
                -- HASCREATOR edge: comment -> person
                INSERT INTO update_batch VALUES (
                    batch,
                    base_edge_id + batch * 10 + i,
                    1575 + (batch * 1000),       -- from: comment
                    723388 + (batch % 100),       -- to: person
                    'HASCREATOR'
                );
            ELSE
                -- ISLOCATEDIN edge: person -> city
                INSERT INTO update_batch VALUES (
                    batch,
                    base_edge_id + batch * 10 + i,
                    723388 + (batch % 200),      -- from: person
                    721934 + (batch % 100),       -- to: city
                    'ISLOCATEDIN'
                );
            END IF;
        END LOOP;
    END LOOP;
END $$;

\echo Update batches created:
SELECT batch_id, COUNT(*) as edges, 
       string_agg(DISTINCT _3, ', ') as edge_types 
FROM update_batch 
GROUP BY batch_id 
ORDER BY batch_id 
LIMIT 5;

SELECT COUNT(*) as total_batches FROM (SELECT DISTINCT batch_id FROM update_batch) t;
SELECT COUNT(*) as total_edges FROM update_batch;
