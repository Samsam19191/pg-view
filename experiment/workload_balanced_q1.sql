-- ================================================================
-- Workload Mix: BALANCED (50% reads, 50% updates) - Q1
-- ================================================================
-- 100 operations: 50 queries, 50 updates (alternating)
-- ================================================================

\timing on

\echo ================================================================
\echo BALANCED WORKLOAD (50/50) - Q1
\echo ================================================================

DELETE FROM workload_results WHERE experiment_id = 'balanced_q1';

\echo Start time:
SELECT NOW() as experiment_start;

-- Baseline
INSERT INTO workload_results 
SELECT 'balanced_q1', 0, 'baseline', 'G', 
       EXTRACT(EPOCH FROM clock_timestamp()) * 1000, 
       COUNT(DISTINCT n_person1._0)
FROM n_g n_person1
JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
WHERE n_person1._1 = 'Person' AND n_person1._0 >= 723388 AND n_person1._0 <= 723415;

INSERT INTO workload_results 
SELECT 'balanced_q1', 0, 'baseline', 'MV', 
       EXTRACT(EPOCH FROM clock_timestamp()) * 1000, 
       COUNT(DISTINCT n_person1.node_id)
FROM v1_nodes n_person1
JOIN v1_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'KNOWS'
JOIN v1_nodes n_person2 ON e1.to_node = n_person2.node_id AND n_person2.label = 'Person'
JOIN v1_edges e3 ON e3.to_node = n_person1.node_id AND e3.label = 'HASCREATOR'
JOIN v1_nodes n_comment ON e3.from_node = n_comment.node_id AND n_comment.label = 'Comment'
JOIN v1_edges e4 ON e4.from_node = n_comment.node_id AND e4.label = 'REPLYOF'
JOIN v1_nodes n_post ON e4.to_node = n_post.node_id AND n_post.label = 'Post'
JOIN v1_edges e5 ON e5.from_node = n_post.node_id AND e5.to_node = n_person2.node_id AND e5.label = 'HASCREATOR'
WHERE n_person1.label = 'Person' AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723415;

INSERT INTO workload_results 
SELECT 'balanced_q1', 0, 'baseline', 'SSR', 
       EXTRACT(EPOCH FROM clock_timestamp()) * 1000, 
       COUNT(DISTINCT person1)
FROM INDEX_v1 WHERE person1 >= 723388 AND person1 <= 723415;

-- Workload: alternating query/update
DO $$
DECLARE
    op_num INT;
    start_ts TIMESTAMP;
    end_ts TIMESTAMP;
    q_result INT;
BEGIN
    FOR op_num IN 1..100 LOOP
        IF op_num % 2 = 0 THEN
            -- UPDATE OPERATION (even numbers)
            INSERT INTO e_g (_0, _1, _2, _3)
            SELECT _0, _1, _2, _3 FROM update_batch WHERE batch_id = (op_num / 2) - 1;
            
            INSERT INTO workload_results VALUES 
                ('balanced_q1', op_num, 'update', 'BASE', 0, 10);
        ELSE
            -- QUERY OPERATION (odd numbers)
            start_ts := clock_timestamp();
            SELECT COUNT(DISTINCT n_person1._0) INTO q_result
            FROM n_g n_person1
            JOIN e_g e1 ON e1._1 = n_person1._0 AND e1._3 = 'KNOWS'
            JOIN n_g n_person2 ON e1._2 = n_person2._0 AND n_person2._1 = 'Person'
            JOIN e_g e3 ON e3._2 = n_person1._0 AND e3._3 = 'HASCREATOR'
            JOIN n_g n_comment ON e3._1 = n_comment._0 AND n_comment._1 = 'Comment'
            JOIN e_g e4 ON e4._1 = n_comment._0 AND e4._3 = 'REPLYOF'
            JOIN n_g n_post ON e4._2 = n_post._0 AND n_post._1 = 'Post'
            JOIN e_g e5 ON e5._1 = n_post._0 AND e5._2 = n_person2._0 AND e5._3 = 'HASCREATOR'
            WHERE n_person1._1 = 'Person' AND n_person1._0 >= 723388 AND n_person1._0 <= 723415;
            end_ts := clock_timestamp();
            INSERT INTO workload_results VALUES 
                ('balanced_q1', op_num, 'query', 'G', 
                 EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000, q_result);
            
            start_ts := clock_timestamp();
            SELECT COUNT(DISTINCT n_person1.node_id) INTO q_result
            FROM v1_nodes n_person1
            JOIN v1_edges e1 ON e1.from_node = n_person1.node_id AND e1.label = 'KNOWS'
            JOIN v1_nodes n_person2 ON e1.to_node = n_person2.node_id AND n_person2.label = 'Person'
            JOIN v1_edges e3 ON e3.to_node = n_person1.node_id AND e3.label = 'HASCREATOR'
            JOIN v1_nodes n_comment ON e3.from_node = n_comment.node_id AND n_comment.label = 'Comment'
            JOIN v1_edges e4 ON e4.from_node = n_comment.node_id AND e4.label = 'REPLYOF'
            JOIN v1_nodes n_post ON e4.to_node = n_post.node_id AND n_post.label = 'Post'
            JOIN v1_edges e5 ON e5.from_node = n_post.node_id AND e5.to_node = n_person2.node_id AND e5.label = 'HASCREATOR'
            WHERE n_person1.label = 'Person' AND n_person1.node_id >= 723388 AND n_person1.node_id <= 723415;
            end_ts := clock_timestamp();
            INSERT INTO workload_results VALUES 
                ('balanced_q1', op_num, 'query', 'MV', 
                 EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000, q_result);
            
            start_ts := clock_timestamp();
            SELECT COUNT(DISTINCT person1) INTO q_result
            FROM INDEX_v1 WHERE person1 >= 723388 AND person1 <= 723415;
            end_ts := clock_timestamp();
            INSERT INTO workload_results VALUES 
                ('balanced_q1', op_num, 'query', 'SSR', 
                 EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000, q_result);
        END IF;
        
        IF op_num % 20 = 0 THEN
            RAISE NOTICE 'Completed % operations', op_num;
        END IF;
    END LOOP;
END $$;

\echo End time:
SELECT NOW() as experiment_end;

\echo ================================================================
\echo BALANCED Q1 RESULTS SUMMARY
\echo ================================================================

SELECT query_mode, 
       COUNT(*) as num_queries,
       ROUND(AVG(time_ms)::numeric, 3) as avg_ms,
       ROUND(MIN(time_ms)::numeric, 3) as min_ms,
       ROUND(MAX(time_ms)::numeric, 3) as max_ms
FROM workload_results 
WHERE experiment_id = 'balanced_q1' AND operation_type = 'query'
GROUP BY query_mode ORDER BY query_mode;

SELECT COUNT(*) as num_updates 
FROM workload_results WHERE experiment_id = 'balanced_q1' AND operation_type = 'update';

DELETE FROM e_g WHERE _0 >= 7000000;
\echo Cleanup complete.

\timing off
