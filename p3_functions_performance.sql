-- \c performance 
--

/*****/ 

BEGIN; 

/*****/ 


-- # # # # # # # # # # # # 
SET ROLE performance_dbo; 
-- # # # # # # # # # # # # 


/******* CONTENTS: ********************* 

--  -------------------  -- 
--  core record-keeping  -- 
--  -------------------  -- 

  Procedure - metric_exec.prc_insert_postgres_database 
  Procedure - metric_exec.prc_insert_postgres_table 
  Procedure - metric_exec.prc_insert_postgres_index 
  Procedure - metric_exec.prc_insert_postgres_function 
  Procedure - metric_exec.prc_insert_postgres_query 
  
  Function - metric_exec.fcn_get_postgres_database_info 
  Function - metric_exec.fcn_get_postgres_table_info 
  Function - metric_exec.fcn_get_postgres_index_info 
  Function - metric_exec.fcn_get_postgres_function_info 
  Function - metric_exec.fcn_get_postgres_query_info 


--  ---------  -- 
--  reporting  -- 
--  ---------  -- 
  
  Function - report_exec.fcn_get_postgres_database_info 
  Function - report_exec.fcn_get_postgres_table_info 
  Function - report_exec.fcn_get_postgres_index_info 
  Function - report_exec.fcn_get_postgres_function_info 
  Function - report_exec.fcn_get_postgres_query_info 


--  ----------------  -- 
--  d.b. maintenance  -- 
--  ----------------  -- 

  Procedure - purge_exec.prc_wipe_old_metric_records 


***************************************/ 


--
-- DROP SCHEMA IF EXISTS metric_exec, report_exec, purge_exec CASCADE; 
--

 
  --
  -- create "metric_exec" schema for general, non-modifying or only-safely-&-routinely-modifying functions relating to the "metric" schema 
  -- create "report_exec" schema for friendly-format data-extract functions from "metric"-schema tables 
  -- 
  -- create "purge_exec" schema for controlled deletions of older records after they become less relevant 
  --


--
--

CREATE SCHEMA metric_exec; 
CREATE SCHEMA report_exec; 
--
CREATE SCHEMA purge_exec;
--

--
--


GRANT USAGE ON SCHEMA metric_exec TO performance_exec_metric; 
GRANT USAGE ON SCHEMA report_exec TO performance_exec_report; 
--
GRANT USAGE ON SCHEMA purge_exec TO performance_exec_purge; 
--


ALTER DEFAULT PRIVILEGES IN SCHEMA metric_exec GRANT EXECUTE ON FUNCTIONS TO performance_exec_metric; 
ALTER DEFAULT PRIVILEGES IN SCHEMA report_exec GRANT EXECUTE ON FUNCTIONS TO performance_exec_report; 
--
ALTER DEFAULT PRIVILEGES IN SCHEMA purge_exec GRANT EXECUTE ON FUNCTIONS TO performance_exec_purge;
--

--
--

  --
  -- create functions in "metric_exec" schema 
  -- 
 
--
--

--
--

/*** INSERT postgres_database ***/ 

/***********************************************
 
    PROCEDURE:  metric_exec.prc_insert_postgres_database 
    
    PARAMETER(S):  p_input_row_json_array   json[]   REQUIRED 
    
    
    DESCRIPTION: 
    -- 
    --  Attempts to process an input array of record attributes (in json format) 
    --   intended for upload into the "postgres_database" table. 
    --
    
    
    EXAMPLE: 
    
    
     CALL metric_exec.prc_insert_postgres_database ( 
               --
                  p_input_row_json_array  =>  ARRAY[ 
                    --
                      '{"domain_code_name":"performance","measured_timestamp":"2022-07-04 13:14:01.111","database_size":"333","sessions":"777"}'::jsonb 
                    , '{"domain_code_name":"example","measured_timestamp":"2022-07-04 13:15:02.222","numbackends":"1","conflicts":"4","deadlocks":"13"}'::jsonb 
                    --
                    ]::jsonb[]::json[] 
               -- 
               ); 


    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE PROCEDURE metric_exec.prc_insert_postgres_database ( p_input_row_json_array json[] ) 
-- 
AS $main_def$ 
<<main_block>> 
DECLARE 
  --
  v_raise_message text := ''; 
  --
  v_row_count int; -- for internal use with: -- GET DIAGNOSTICS v_row_count = ROW_COUNT; 
  --
  --
  v_input_array_cardinality int := cardinality( p_input_row_json_array )::int; 
  v_loop_current_iteration int := 1; 
  --
  --
BEGIN 
    
    v_raise_message := utility.fcn_console_message('START :: metric_exec.prc_insert_postgres_database');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    CREATE TEMPORARY TABLE tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input  
    (
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , domain_code_name text NULL 
    , measured_timestamp timestamp NULL 
    --
    , domain_pin bigint NULL 
    --
    -- 
    , numbackends int NULL 
    , xact_commit bigint NULL 
    , xact_rollback bigint NULL 
    , blks_read bigint NULL 
    , blks_hit bigint NULL 
    , tup_returned bigint NULL 
    , tup_fetched bigint NULL 
    , tup_inserted bigint NULL 
    , tup_updated bigint NULL 
    , tup_deleted bigint NULL 
    , conflicts bigint NULL 
    , temp_files bigint NULL 
    , temp_bytes bigint NULL 
    , deadlocks bigint NULL 
    , checksum_failures bigint NULL 
    , checksum_last_failure timestamp NULL 
    , blk_read_time numeric(32,16) NULL 
    , blk_write_time numeric(32,16) NULL 
    , session_time numeric(32,16) NULL 
    , active_time numeric(32,16) NULL 
    , idle_in_transaction_time numeric(32,16) NULL 
    , sessions bigint NULL 
    , sessions_abandoned bigint NULL 
    , sessions_fatal bigint NULL 
    , sessions_killed bigint NULL 
    , stats_reset timestamp NULL 
    --
    , database_size bigint NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_i_p_d_unpacked_input PRIMARY KEY ( tmp_pin ) 
    -- 
    ) ON COMMIT DROP; 

--
--

    --
    --  Check input parameters ... 
    --
    
--
--

IF p_input_row_json_array IS NULL THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_database');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

    v_raise_message := utility.fcn_console_message('Unpack the array-format input-parameter "p_input_row_json_array" into a table-format.');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    WHILE ( v_loop_current_iteration <= v_input_array_cardinality ) 
    LOOP 
    --

      INSERT INTO tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input 
      (
        domain_code_name 
      , measured_timestamp 
      --
      -- 
      , numbackends 
      , xact_commit 
      , xact_rollback 
      , blks_read 
      , blks_hit 
      , tup_returned 
      , tup_fetched 
      , tup_inserted 
      , tup_updated 
      , tup_deleted 
      , conflicts 
      , temp_files 
      , temp_bytes 
      , deadlocks 
      , checksum_failures 
      , checksum_last_failure 
      , blk_read_time 
      , blk_write_time 
      , session_time 
      , active_time 
      , idle_in_transaction_time 
      , sessions 
      , sessions_abandoned 
      , sessions_fatal 
      , sessions_killed 
      , stats_reset 
      --
      , database_size 
      --
      -- 
      )
      
        SELECT  A.domain_code_name 
        ,       A.measured_timestamp 
        --      
        --      
        ,       A.numbackends 
        ,       A.xact_commit 
        ,       A.xact_rollback 
        ,       A.blks_read 
        ,       A.blks_hit 
        ,       A.tup_returned 
        ,       A.tup_fetched 
        ,       A.tup_inserted 
        ,       A.tup_updated 
        ,       A.tup_deleted 
        ,       A.conflicts 
        ,       A.temp_files 
        ,       A.temp_bytes 
        ,       A.deadlocks 
        ,       A.checksum_failures 
        ,       A.checksum_last_failure 
        ,       A.blk_read_time 
        ,       A.blk_write_time 
        ,       A.session_time 
        ,       A.active_time 
        ,       A.idle_in_transaction_time 
        ,       A.sessions 
        ,       A.sessions_abandoned 
        ,       A.sessions_fatal 
        ,       A.sessions_killed 
        ,       A.stats_reset 
        -- 
        ,       A.database_size 
        --
        --
        FROM  json_populate_record( null::tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input 
                                  , p_input_row_json_array[v_loop_current_iteration] )  AS  A  
        --
        ;
        
    --
    --
                
        v_loop_current_iteration := v_loop_current_iteration + 1; 
    
    --
    --
                
    --
    END LOOP; 
    
--
--
      
    v_row_count := v_input_array_cardinality; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

IF v_row_count = 0 THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_database');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_code_name IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "domain_code_name" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "domain_code_name" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.measured_timestamp IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "measured_timestamp" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "measured_timestamp" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--

    --
    --  Validate request ... 
    --
    
--
--

    v_raise_message := utility.fcn_console_message('Map all relevant "domain_code_name" values to their associated "domain" record (by "pin" value).');  
    RAISE NOTICE '%' , v_raise_message; 
    
      UPDATE  tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input  AS  UU 
      SET     domain_pin = X.pin 
      FROM    metric.domain  AS  X  
      WHERE   lower( UU.domain_code_name ) = lower( X.code_name ) 
      --
      ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
        
--
--
        
IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_pin IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "domain_code_name" coordinate-value does not match any rows in the "domain" table.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'Each provided "domain_code_name" coordinate-value must match a row in the "domain" table.';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input  AS  X 
                INNER JOIN  metric.postgres_database  AS  E  ON  X.domain_pin = E.domain_pin 
                                                             AND X.measured_timestamp = E.measured_timestamp ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference and "measured_timestamp" coordinate-value already exists in the "postgres_database" table.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'A new measurement record can not be inserted with an existing "domain" reference and "measured_timestamp" coordinate-value.';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input  AS  X 
                GROUP BY  X.domain_pin 
                ,         X.measured_timestamp 
                HAVING  COUNT(*) > 1 ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference and "measured_timestamp" coordinate-value appears more than once in the staged insert-proposal row-set.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'The provided "p_input_row_json_array" parameter-value must not contain any duplicate combinations of a "domain" reference and "measured_timestamp" coordinate-value.';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         
    
--
--
        
--
--

--
--

    --
    --  Perform request ... 
    --
    
--
--
    
    v_raise_message := utility.fcn_console_message('Insert new records into "metric"."postgres_database":');  
    RAISE NOTICE '%' , v_raise_message; 
    
      INSERT INTO metric.postgres_database 
      (
        domain_pin 
      --
      , measured_timestamp 
      --
      -- 
      , numbackends 
      , xact_commit 
      , xact_rollback 
      , blks_read 
      , blks_hit 
      , tup_returned 
      , tup_fetched 
      , tup_inserted 
      , tup_updated 
      , tup_deleted 
      , conflicts 
      , temp_files 
      , temp_bytes 
      , deadlocks 
      , checksum_failures 
      , checksum_last_failure 
      , blk_read_time 
      , blk_write_time 
      , session_time 
      , active_time 
      , idle_in_transaction_time 
      , sessions 
      , sessions_abandoned 
      , sessions_fatal 
      , sessions_killed 
      , stats_reset 
      --
      , database_size 
      -- 
      --
      )  
    
        SELECT  N.domain_pin 
        --
        ,       N.measured_timestamp 
        -- 
        -- 
        ,       N.numbackends 
        ,       N.xact_commit 
        ,       N.xact_rollback 
        ,       N.blks_read 
        ,       N.blks_hit 
        ,       N.tup_returned 
        ,       N.tup_fetched 
        ,       N.tup_inserted 
        ,       N.tup_updated 
        ,       N.tup_deleted 
        ,       N.conflicts 
        ,       N.temp_files 
        ,       N.temp_bytes 
        ,       N.deadlocks 
        ,       N.checksum_failures 
        ,       N.checksum_last_failure 
        ,       N.blk_read_time 
        ,       N.blk_write_time 
        ,       N.session_time 
        ,       N.active_time 
        ,       N.idle_in_transaction_time 
        ,       N.sessions 
        ,       N.sessions_abandoned 
        ,       N.sessions_fatal 
        ,       N.sessions_killed 
        ,       N.stats_reset 
        -- 
        ,       N.database_size 
        -- 
        -- 
        FROM  tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input  AS  N  
        --
        LEFT  JOIN  metric.postgres_database  AS  E  ON  N.domain_pin = E.domain_pin 
                                                     AND N.measured_timestamp = E.measured_timestamp 
        --
        WHERE  E.pin IS NULL 
        --
        ORDER BY  N.tmp_pin  ASC             
        --
        ;  

    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
                
--
--

--
--

    --
    --  Drop temporary tables & finish/exit routine ... 
    --
    
-- 
-- 

    DROP TABLE tmp_internal_prc_i_p_d_input_row_json_array_unpacked_input; 
    
--
--

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_database');  
    RAISE NOTICE '%' , v_raise_message; 
    
-- 
-- 
END main_block;
$main_def$ LANGUAGE plpgsql 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** INSERT postgres_table ***/ 

/***********************************************
 
    PROCEDURE: metric_exec.prc_insert_postgres_table 
    
    PARAMETER(S):  p_input_row_json_array   json[]   REQUIRED 
    
    
    DESCRIPTION: 
    -- 
    --  Attempts to process an input array of record attributes (in json format) 
    --   intended for upload into the "postgres_table" table. 
    --
    
    
    EXAMPLE: 
    
    
     CALL metric_exec.prc_insert_postgres_table ( 
               --
                  p_input_row_json_array  =>  ARRAY[ 
                    --
                      '{"domain_code_name":"performance","measured_timestamp":"2022-07-04 13:14:01.111","schemaname":"private","relname":"test_table","vacuum_count":"333","table_size":"777"}'::jsonb 
                    , '{"domain_code_name":"example","measured_timestamp":"2022-07-04 13:15:02.222","schemaname":"private","relname":"test_table","autovacuum_count":"333","indexes_size":"777"}'::jsonb 
                    --
                    ]::jsonb[]::json[] 
               -- 
               ); 
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE PROCEDURE metric_exec.prc_insert_postgres_table ( p_input_row_json_array json[] ) 
-- 
AS $main_def$ 
<<main_block>> 
DECLARE 
  --
  v_raise_message text := ''; 
  --
  v_row_count int; -- for internal use with: -- GET DIAGNOSTICS v_row_count = ROW_COUNT; 
  --
  --
  v_input_array_cardinality int := cardinality( p_input_row_json_array )::int; 
  v_loop_current_iteration int := 1; 
  --
  --
BEGIN 
    
    v_raise_message := utility.fcn_console_message('START :: metric_exec.prc_insert_postgres_table');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    CREATE TEMPORARY TABLE tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  
    (
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , domain_code_name text NULL 
    , measured_timestamp timestamp NULL 
    --
    , domain_pin bigint NULL 
    --
    -- 
    , schemaname text NOT NULL 
    , relname text NOT NULL 
    , seq_scan bigint NULL 
    , seq_tup_read bigint NULL 
    , idx_scan bigint NULL 
    , idx_tup_fetch bigint NULL 
    , n_tup_ins bigint NULL 
    , n_tup_upd bigint NULL 
    , n_tup_del bigint NULL 
    , n_tup_hot_upd bigint NULL 
    , n_live_tup bigint NULL 
    , n_dead_tup bigint NULL 
    , n_mod_since_analyze bigint NULL 
    , n_ins_since_vacuum bigint NULL 
    , last_vacuum timestamp NULL 
    , last_autovacuum timestamp NULL 
    , last_analyze timestamp NULL 
    , last_autoanalyze timestamp NULL 
    , vacuum_count bigint NULL 
    , autovacuum_count bigint NULL 
    , analyze_count bigint NULL 
    , autoanalyze_count bigint NULL 
    --
    , heap_blks_read bigint NULL 
    , heap_blks_hit bigint NULL 
    , idx_blks_read bigint NULL 
    , idx_blks_hit bigint NULL 
    , toast_blks_read bigint NULL 
    , toast_blks_hit bigint NULL 
    , tidx_blks_read bigint NULL 
    , tidx_blks_hit bigint NULL 
    -- 
    , total_relation_size bigint NULL 
    , table_size bigint NULL 
    , indexes_size bigint NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_i_p_t_unpacked_input PRIMARY KEY ( tmp_pin ) 
    -- 
    ) ON COMMIT DROP; 

--
--

    --
    --  Check input parameters ... 
    --
    
--
--

IF p_input_row_json_array IS NULL THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_table');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

    v_raise_message := utility.fcn_console_message('Unpack the array-format input-parameter "p_input_row_json_array" into a table-format.');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    WHILE ( v_loop_current_iteration <= v_input_array_cardinality ) 
    LOOP 
    --

      INSERT INTO tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input 
      (
        domain_code_name 
      , measured_timestamp 
      --
      -- 
      , schemaname 
      , relname 
      , seq_scan 
      , seq_tup_read 
      , idx_scan 
      , idx_tup_fetch 
      , n_tup_ins 
      , n_tup_upd 
      , n_tup_del 
      , n_tup_hot_upd 
      , n_live_tup 
      , n_dead_tup 
      , n_mod_since_analyze 
      , n_ins_since_vacuum 
      , last_vacuum 
      , last_autovacuum 
      , last_analyze 
      , last_autoanalyze 
      , vacuum_count 
      , autovacuum_count 
      , analyze_count 
      , autoanalyze_count 
      --
      , heap_blks_read 
      , heap_blks_hit 
      , idx_blks_read 
      , idx_blks_hit 
      , toast_blks_read 
      , toast_blks_hit 
      , tidx_blks_read 
      , tidx_blks_hit 
      -- 
      , total_relation_size 
      , table_size 
      , indexes_size 
      --
      -- 
      )
      
        SELECT  A.domain_code_name 
        ,       A.measured_timestamp 
        --      
        --      
        ,       A.schemaname 
        ,       A.relname 
        ,       A.seq_scan 
        ,       A.seq_tup_read 
        ,       A.idx_scan 
        ,       A.idx_tup_fetch 
        ,       A.n_tup_ins 
        ,       A.n_tup_upd 
        ,       A.n_tup_del 
        ,       A.n_tup_hot_upd 
        ,       A.n_live_tup 
        ,       A.n_dead_tup 
        ,       A.n_mod_since_analyze 
        ,       A.n_ins_since_vacuum 
        ,       A.last_vacuum 
        ,       A.last_autovacuum 
        ,       A.last_analyze 
        ,       A.last_autoanalyze 
        ,       A.vacuum_count 
        ,       A.autovacuum_count 
        ,       A.analyze_count 
        ,       A.autoanalyze_count 
        -- 
        ,       A.heap_blks_read 
        ,       A.heap_blks_hit 
        ,       A.idx_blks_read 
        ,       A.idx_blks_hit 
        ,       A.toast_blks_read 
        ,       A.toast_blks_hit 
        ,       A.tidx_blks_read 
        ,       A.tidx_blks_hit 
        -- 
        ,       A.total_relation_size 
        ,       A.table_size 
        ,       A.indexes_size 
        --
        --
        FROM  json_populate_record( null::tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  
                                  , p_input_row_json_array[v_loop_current_iteration] )  AS  A  
        --
        ;
        
    --
    --
                
        v_loop_current_iteration := v_loop_current_iteration + 1; 
    
    --
    --
                
    --
    END LOOP; 
    
--
--
      
    v_row_count := v_input_array_cardinality; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

IF v_row_count = 0 THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_table');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_code_name IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "domain_code_name" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "domain_code_name" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.measured_timestamp IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "measured_timestamp" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "measured_timestamp" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.schemaname IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "schemaname" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "schemaname" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.relname IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "relname" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "relname" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--

    --
    --  Validate request ... 
    --
    
--
--

    v_raise_message := utility.fcn_console_message('Map all relevant "domain_code_name" values to their associated "domain" record (by "pin" value).');  
    RAISE NOTICE '%' , v_raise_message; 
    
      UPDATE  tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  AS  UU 
      SET     domain_pin = X.pin 
      FROM    metric.domain  AS  X  
      WHERE   lower( UU.domain_code_name ) = lower( X.code_name ) 
      --
      ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
        
--
--
        
IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_pin IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "domain_code_name" coordinate-value does not match any rows in the "domain" table.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'Each provided "domain_code_name" coordinate-value must match a row in the "domain" table.';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  AS  X 
                INNER JOIN  metric.postgres_table  AS  E  ON  X.domain_pin = E.domain_pin 
                                                          AND X.measured_timestamp = E.measured_timestamp ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference and "measured_timestamp" coordinate-value already exists in the "postgres_table" table.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'A new measurement record can not be inserted with an existing "domain" reference and "measured_timestamp" coordinate-value.';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  AS  X 
                GROUP BY  X.domain_pin 
                ,         X.measured_timestamp 
                --
                ,         X.schemaname 
                ,         X.relname                      
                --
                HAVING  COUNT(*) > 1 ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference, "measured_timestamp" coordinate-value, "schemaname", and "relname" appears more than once in the staged insert-proposal row-set.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'The provided "p_input_row_json_array" parameter-value must not contain any duplicate combinations of a "domain" reference, "measured_timestamp" coordinate-value, "schemaname", and "relname".';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         
    
--
--
        
--
--

--
--

    --
    --  Perform request ... 
    --
    
--
--
    
    v_raise_message := utility.fcn_console_message('Insert new records into "metric"."postgres_table":');  
    RAISE NOTICE '%' , v_raise_message; 
    
      INSERT INTO metric.postgres_table 
      (
        domain_pin 
      --
      , measured_timestamp 
      --
      -- 
      , schemaname 
      , relname 
      , seq_scan 
      , seq_tup_read 
      , idx_scan 
      , idx_tup_fetch 
      , n_tup_ins 
      , n_tup_upd 
      , n_tup_del 
      , n_tup_hot_upd 
      , n_live_tup 
      , n_dead_tup 
      , n_mod_since_analyze 
      , n_ins_since_vacuum 
      , last_vacuum 
      , last_autovacuum 
      , last_analyze 
      , last_autoanalyze 
      , vacuum_count 
      , autovacuum_count 
      , analyze_count 
      , autoanalyze_count 
      --
      , heap_blks_read 
      , heap_blks_hit 
      , idx_blks_read 
      , idx_blks_hit 
      , toast_blks_read 
      , toast_blks_hit 
      , tidx_blks_read 
      , tidx_blks_hit 
      -- 
      , total_relation_size 
      , table_size 
      , indexes_size 
      -- 
      --
      )  
    
        SELECT  N.domain_pin 
        --
        ,       N.measured_timestamp 
        -- 
        -- 
        ,       N.schemaname 
        ,       N.relname 
        ,       N.seq_scan 
        ,       N.seq_tup_read 
        ,       N.idx_scan 
        ,       N.idx_tup_fetch 
        ,       N.n_tup_ins 
        ,       N.n_tup_upd 
        ,       N.n_tup_del 
        ,       N.n_tup_hot_upd 
        ,       N.n_live_tup 
        ,       N.n_dead_tup 
        ,       N.n_mod_since_analyze 
        ,       N.n_ins_since_vacuum 
        ,       N.last_vacuum 
        ,       N.last_autovacuum 
        ,       N.last_analyze 
        ,       N.last_autoanalyze 
        ,       N.vacuum_count 
        ,       N.autovacuum_count 
        ,       N.analyze_count 
        ,       N.autoanalyze_count 
        --      
        ,       N.heap_blks_read 
        ,       N.heap_blks_hit 
        ,       N.idx_blks_read 
        ,       N.idx_blks_hit 
        ,       N.toast_blks_read 
        ,       N.toast_blks_hit 
        ,       N.tidx_blks_read 
        ,       N.tidx_blks_hit 
        --       
        ,       N.total_relation_size 
        ,       N.table_size 
        ,       N.indexes_size 
        -- 
        -- 
        FROM  tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input  AS  N  
        --
        LEFT  JOIN  metric.postgres_table  AS  E  ON  N.domain_pin = E.domain_pin 
                                                  AND N.measured_timestamp = E.measured_timestamp 
        --
        WHERE  E.pin IS NULL 
        --
        ORDER BY  N.tmp_pin  ASC             
        --
        ;  

    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
                
--
--

--
--

    --
    --  Drop temporary tables & finish/exit routine ... 
    --
    
-- 
-- 

    DROP TABLE tmp_internal_prc_i_p_t_input_row_json_array_unpacked_input; 
    
--
--

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_table');  
    RAISE NOTICE '%' , v_raise_message; 
    
-- 
-- 
END main_block; 
$main_def$ LANGUAGE plpgsql 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** INSERT postgres_index ***/ 

/***********************************************
 
    PROCEDURE: metric_exec.prc_insert_postgres_index 
    
    PARAMETER(S):  p_input_row_json_array   json[]   REQUIRED 
    
    
    DESCRIPTION: 
    -- 
    --  Attempts to process an input array of record attributes (in json format) 
    --   intended for upload into the "postgres_index" table. 
    --
    
    
    EXAMPLE: 
    
    
     CALL metric_exec.prc_insert_postgres_index ( 
               --
                  p_input_row_json_array  =>  ARRAY[ 
                    --
                      '{"domain_code_name":"performance","measured_timestamp":"2022-07-04 13:14:01.111","schemaname":"private","relname":"test_table","indexrelname":"ix_test_table_1","idx_tup_read":"777"}'::jsonb 
                    , '{"domain_code_name":"example","measured_timestamp":"2022-07-04 13:15:02.222","schemaname":"private","relname":"test_table","indexrelname":"ix_test_table_2","idx_blks_read":"4","idx_blks_hit":"13"}'::jsonb 
                    --
                    ]::jsonb[]::json[] 
               -- 
               ); 
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE PROCEDURE metric_exec.prc_insert_postgres_index ( p_input_row_json_array json[] ) 
-- 
AS $main_def$ 
<<main_block>> 
DECLARE 
  --
  v_raise_message text := ''; 
  --
  v_row_count int; -- for internal use with: -- GET DIAGNOSTICS v_row_count = ROW_COUNT; 
  --
  --
  v_input_array_cardinality int := cardinality( p_input_row_json_array )::int; 
  v_loop_current_iteration int := 1; 
  --
  --
BEGIN 
    
    v_raise_message := utility.fcn_console_message('START :: metric_exec.prc_insert_postgres_index');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    CREATE TEMPORARY TABLE tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  
    (
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , domain_code_name text NULL 
    , measured_timestamp timestamp NULL 
    --
    , domain_pin bigint NULL 
    --
    -- 
    , schemaname text NOT NULL 
    , relname text NOT NULL 
    , indexrelname text NOT NULL 
    , idx_scan bigint NULL 
    , idx_tup_read bigint NULL 
    , idx_tup_fetch bigint NULL 
    --
    , idx_blks_read bigint NULL 
    , idx_blks_hit bigint NULL 
    --
    , total_relation_size bigint NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_i_p_i_unpacked_input PRIMARY KEY ( tmp_pin ) 
    -- 
    ) ON COMMIT DROP; 

--
--

    --
    --  Check input parameters ... 
    --
    
--
--

IF p_input_row_json_array IS NULL THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_index');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

    v_raise_message := utility.fcn_console_message('Unpack the array-format input-parameter "p_input_row_json_array" into a table-format.');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    WHILE ( v_loop_current_iteration <= v_input_array_cardinality ) 
    LOOP 
    --

      INSERT INTO tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input 
      (
        domain_code_name 
      , measured_timestamp 
      --
      -- 
      , schemaname 
      , relname 
      , indexrelname 
      , idx_scan 
      , idx_tup_read 
      , idx_tup_fetch 
      --
      , idx_blks_read 
      , idx_blks_hit 
      --
      , total_relation_size 
      --
      -- 
      )
      
        SELECT  A.domain_code_name 
        ,       A.measured_timestamp 
        --      
        --      
        ,       A.schemaname 
        ,       A.relname 
        ,       A.indexrelname 
        ,       A.idx_scan 
        ,       A.idx_tup_read 
        ,       A.idx_tup_fetch 
        --
        ,       A.idx_blks_read 
        ,       A.idx_blks_hit 
        --
        ,       A.total_relation_size 
        --
        --
        FROM  json_populate_record( null::tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input 
                                  , p_input_row_json_array[v_loop_current_iteration] )  AS  A  
        --
        ;
        
    --
    --
                
        v_loop_current_iteration := v_loop_current_iteration + 1; 
    
    --
    --
                
    --
    END LOOP; 
    
--
--
      
    v_row_count := v_input_array_cardinality; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

IF v_row_count = 0 THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_index');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_code_name IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "domain_code_name" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "domain_code_name" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.measured_timestamp IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "measured_timestamp" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "measured_timestamp" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.schemaname IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "schemaname" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "schemaname" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.relname IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "relname" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "relname" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.indexrelname IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "indexrelname" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "indexrelname" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--

    --
    --  Validate request ... 
    --
    
--
--

    v_raise_message := utility.fcn_console_message('Map all relevant "domain_code_name" values to their associated "domain" record (by "pin" value).');  
    RAISE NOTICE '%' , v_raise_message; 
    
      UPDATE  tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  UU 
      SET     domain_pin = X.pin 
      FROM    metric.domain  AS  X  
      WHERE   lower( UU.domain_code_name ) = lower( X.code_name ) 
      --
      ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
        
--
--
        
IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_pin IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "domain_code_name" coordinate-value does not match any rows in the "domain" table.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'Each provided "domain_code_name" coordinate-value must match a row in the "domain" table.';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  X 
                INNER JOIN  metric.postgres_index  AS  E  ON  X.domain_pin = E.domain_pin 
                                                          AND X.measured_timestamp = E.measured_timestamp ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference and "measured_timestamp" coordinate-value already exists in the "postgres_index" table.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'A new measurement record can not be inserted with an existing "domain" reference and "measured_timestamp" coordinate-value.';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  X 
                GROUP BY  X.domain_pin 
                ,         X.measured_timestamp 
                --
                ,         X.schemaname      
                ,         X.relname          
                ,         X.indexrelname      
                --
                HAVING  COUNT(*) > 1 ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference, "measured_timestamp" coordinate-value, "schemaname", "relname", and "indexrelname" appears more than once in the staged insert-proposal row-set.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'The provided "p_input_row_json_array" parameter-value must not contain any duplicate combinations of a "domain" reference, "measured_timestamp" coordinate-value, "schemaname", "relname", and "indexrelname".';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         
    
--
--
        
--
--

--
--

    --
    --  Perform request ... 
    --
    
--
--
    
    v_raise_message := utility.fcn_console_message('Insert new records into "metric"."postgres_index":');  
    RAISE NOTICE '%' , v_raise_message; 
    
      INSERT INTO metric.postgres_index 
      (
        domain_pin 
      --
      , measured_timestamp 
      --
      -- 
      , schemaname 
      , relname 
      , indexrelname 
      , idx_scan 
      , idx_tup_read 
      , idx_tup_fetch 
      --
      , idx_blks_read 
      , idx_blks_hit 
      --
      , total_relation_size 
      -- 
      --
      )  
    
        SELECT  N.domain_pin 
        --
        ,       N.measured_timestamp 
        -- 
        -- 
        ,       N.schemaname 
        ,       N.relname 
        ,       N.indexrelname 
        ,       N.idx_scan 
        ,       N.idx_tup_read 
        ,       N.idx_tup_fetch 
        --      
        ,       N.idx_blks_read 
        ,       N.idx_blks_hit 
        --      
        ,       N.total_relation_size  
        -- 
        -- 
        FROM  tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input  AS  N  
        --
        LEFT  JOIN  metric.postgres_index  AS  E  ON  N.domain_pin = E.domain_pin 
                                                  AND N.measured_timestamp = E.measured_timestamp 
        --
        WHERE  E.pin IS NULL 
        --
        ORDER BY  N.tmp_pin  ASC             
        -- 
        ;  

    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
                
--
--

--
--

    --
    --  Drop temporary tables & finish/exit routine ... 
    --
    
-- 
-- 

    DROP TABLE tmp_internal_prc_i_p_i_input_row_json_array_unpacked_input; 
    
--
--

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_index');  
    RAISE NOTICE '%' , v_raise_message; 
    
-- 
-- 
END main_block; 
$main_def$ LANGUAGE plpgsql 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** INSERT postgres_function ***/ 

/***********************************************
 
    PROCEDURE: metric_exec.prc_insert_postgres_function 
    
    PARAMETER(S):  p_input_row_json_array   json[]   REQUIRED 
    
    
    DESCRIPTION: 
    -- 
    --  Attempts to process an input array of record attributes (in json format) 
    --   intended for upload into the "postgres_function" table. 
    --
    
    
    EXAMPLE: 
    
    
     CALL metric_exec.prc_insert_postgres_function ( 
               --
                  p_input_row_json_array  =>  ARRAY[ 
                    --
                      '{"domain_code_name":"performance","measured_timestamp":"2022-07-04 13:14:01.111","schemaname":"private","funcname":"test_function","calls":"333","total_time":"777.7777"}'::jsonb 
                    , '{"domain_code_name":"example","measured_timestamp":"2022-07-04 13:15:02.222","schemaname":"private","funcname":"test_function","self_time":"13"}'::jsonb 
                    --
                    ]::jsonb[]::json[] 
               -- 
               ); 


    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE PROCEDURE metric_exec.prc_insert_postgres_function ( p_input_row_json_array json[] ) 
-- 
AS $main_def$ 
<<main_block>> 
DECLARE 
  --
  v_raise_message text := ''; 
  --
  v_row_count int; -- for internal use with: -- GET DIAGNOSTICS v_row_count = ROW_COUNT; 
  --
  --
  v_input_array_cardinality int := cardinality( p_input_row_json_array )::int; 
  v_loop_current_iteration int := 1; 
  --
  --
BEGIN 
    
    v_raise_message := utility.fcn_console_message('START :: metric_exec.prc_insert_postgres_function');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    CREATE TEMPORARY TABLE tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  
    (
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , domain_code_name text NULL 
    , measured_timestamp timestamp NULL 
    --
    , domain_pin bigint NULL 
    -- 
    -- 
    , schemaname text NOT NULL 
    , funcname text NOT NULL 
    , calls bigint NULL 
    , total_time numeric(32,16) NULL 
    , self_time numeric(32,16) NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_i_p_f_unpacked_input PRIMARY KEY ( tmp_pin ) 
    -- 
    ) ON COMMIT DROP; 

--
--

    --
    --  Check input parameters ... 
    --
    
--
--

IF p_input_row_json_array IS NULL THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_function');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

    v_raise_message := utility.fcn_console_message('Unpack the array-format input-parameter "p_input_row_json_array" into a table-format.');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    WHILE ( v_loop_current_iteration <= v_input_array_cardinality ) 
    LOOP 
    --

      INSERT INTO tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input 
      (
        domain_code_name 
      , measured_timestamp 
      --
      -- 
      , schemaname 
      , funcname 
      , calls 
      , total_time 
      , self_time 
      --
      -- 
      )
      
        SELECT  A.domain_code_name 
        ,       A.measured_timestamp 
        --      
        --      
        ,       A.schemaname 
        ,       A.funcname 
        ,       A.calls 
        ,       A.total_time 
        ,       A.self_time 
        --
        --
        FROM  json_populate_record( null::tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input 
                                  , p_input_row_json_array[v_loop_current_iteration] )  AS  A  
        --
        ;
        
    --
    --
                
        v_loop_current_iteration := v_loop_current_iteration + 1; 
    
    --
    --
                
    --
    END LOOP; 
    
--
--
      
    v_row_count := v_input_array_cardinality; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

IF v_row_count = 0 THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_function');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_code_name IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "domain_code_name" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "domain_code_name" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.measured_timestamp IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "measured_timestamp" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "measured_timestamp" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.schemaname IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "schemaname" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "schemaname" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.funcname IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "funcname" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "funcname" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--

    --
    --  Validate request ... 
    --
    
--
--

    v_raise_message := utility.fcn_console_message('Map all relevant "domain_code_name" values to their associated "domain" record (by "pin" value).');  
    RAISE NOTICE '%' , v_raise_message; 
    
      UPDATE  tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  AS  UU 
      SET     domain_pin = X.pin 
      FROM    metric.domain  AS  X  
      WHERE   lower( UU.domain_code_name ) = lower( X.code_name ) 
      --
      ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
        
--
--
        
IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_pin IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "domain_code_name" coordinate-value does not match any rows in the "domain" table.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'Each provided "domain_code_name" coordinate-value must match a row in the "domain" table.';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  AS  X 
                INNER JOIN  metric.postgres_function  AS  E  ON  X.domain_pin = E.domain_pin 
                                                             AND X.measured_timestamp = E.measured_timestamp ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference and "measured_timestamp" coordinate-value already exists in the "postgres_function" table.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'A new measurement record can not be inserted with an existing "domain" reference and "measured_timestamp" coordinate-value.';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  AS  X 
                GROUP BY  X.domain_pin 
                ,         X.measured_timestamp 
                --
                ,         X.schemaname 
                ,         X.funcname 
                --
                HAVING  COUNT(*) > 1 ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference, "measured_timestamp" coordinate-value, "schemaname", and "funcname" appears more than once in the staged insert-proposal row-set.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'The provided "p_input_row_json_array" parameter-value must not contain any duplicate combinations of a "domain" reference, "measured_timestamp" coordinate-value, "schemaname", and "funcname".';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         
    
--
--
        
--
--

--
--

    --
    --  Perform request ... 
    --
    
--
--
    
    v_raise_message := utility.fcn_console_message('Insert new records into "metric"."postgres_function":');  
    RAISE NOTICE '%' , v_raise_message; 
    
      INSERT INTO metric.postgres_function 
      (
        domain_pin 
      --
      , measured_timestamp 
      --
      -- 
      , schemaname 
      , funcname 
      , calls 
      , total_time 
      , self_time 
      -- 
      --
      )  
    
        SELECT  N.domain_pin 
        --
        ,       N.measured_timestamp 
        -- 
        -- 
        ,       N.schemaname 
        ,       N.funcname 
        ,       N.calls 
        ,       N.total_time 
        ,       N.self_time 
        -- 
        -- 
        FROM  tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input  AS  N  
        --
        LEFT  JOIN  metric.postgres_function  AS  E  ON  N.domain_pin = E.domain_pin 
                                                     AND N.measured_timestamp = E.measured_timestamp 
        --
        WHERE  E.pin IS NULL 
        --
        ORDER BY  N.tmp_pin  ASC             
        --
        ;  

    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
                
--
--

--
--

    --
    --  Drop temporary tables & finish/exit routine ... 
    --
    
-- 
-- 

    DROP TABLE tmp_internal_prc_i_p_f_input_row_json_array_unpacked_input; 
    
--
--

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_function');  
    RAISE NOTICE '%' , v_raise_message; 
    
-- 
-- 
END main_block; 
$main_def$ LANGUAGE plpgsql 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** INSERT postgres_query ***/ 

/***********************************************
 
    PROCEDURE: metric_exec.prc_insert_postgres_query 
    
    PARAMETER(S):  p_input_row_json_array   json[]   REQUIRED 
    
    
    DESCRIPTION: 
    -- 
    --  Attempts to process an input array of record attributes (in json format) 
    --   intended for upload into the "postgres_query" table. 
    --
    
    
    EXAMPLE: 
    
    
     CALL metric_exec.prc_insert_postgres_query ( 
               --
                  p_input_row_json_array  =>  ARRAY[ 
                    --
                      '{"domain_code_name":"performance","measured_timestamp":"2022-07-04 13:14:01.111","toplevel":"true","queryid":"777","rolname":"postgres","calls":"777"}'::jsonb 
                    , '{"domain_code_name":"example","measured_timestamp":"2022-07-04 13:15:02.222","toplevel":"true","queryid":"-1","rolname":"postgres","mean_exec_time":"333.333"}'::jsonb 
                    --
                    ]::jsonb[]::json[] 
               -- 
               ); 
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE PROCEDURE metric_exec.prc_insert_postgres_query ( p_input_row_json_array json[] ) 
-- 
AS $main_def$ 
<<main_block>> 
DECLARE 
  --
  v_raise_message text := ''; 
  --
  v_row_count int; -- for internal use with: -- GET DIAGNOSTICS v_row_count = ROW_COUNT; 
  --
  --
  v_input_array_cardinality int := cardinality( p_input_row_json_array )::int; 
  v_loop_current_iteration int := 1; 
  --
  --
BEGIN 
    
    v_raise_message := utility.fcn_console_message('START :: metric_exec.prc_insert_postgres_query');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    CREATE TEMPORARY TABLE tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  
    (
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , domain_code_name text NULL 
    , measured_timestamp timestamp NULL 
    --
    , domain_pin bigint NULL 
    -- 
    -- 
    , toplevel bool NOT NULL 
    , queryid bigint NOT NULL 
    , query text NULL 
    , plans bigint NULL 
    , total_plan_time numeric(32,16) NULL 
    , min_plan_time numeric(32,16) NULL 
    , max_plan_time numeric(32,16) NULL 
    , mean_plan_time numeric(32,16) NULL 
    , stddev_plan_time numeric(32,16) NULL 
    , calls bigint NULL 
    , total_exec_time numeric(32,16) NULL 
    , min_exec_time numeric(32,16) NULL 
    , max_exec_time numeric(32,16) NULL 
    , mean_exec_time numeric(32,16) NULL 
    , stddev_exec_time numeric(32,16) NULL 
    , rows bigint NULL 
    , shared_blks_hit bigint NULL 
    , shared_blks_read bigint NULL 
    , shared_blks_dirtied bigint NULL 
    , shared_blks_written bigint NULL 
    , local_blks_hit bigint NULL 
    , local_blks_read bigint NULL 
    , local_blks_dirtied bigint NULL 
    , local_blks_written bigint NULL 
    , temp_blks_read bigint NULL 
    , temp_blks_written bigint NULL 
    , blk_read_time numeric(32,16) NULL 
    , blk_write_time numeric(32,16) NULL 
    , wal_records bigint NULL 
    , wal_fpi bigint NULL 
    , wal_bytes numeric(32,16) NULL 
    --
    , rolname text NOT NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_i_p_q_unpacked_input PRIMARY KEY ( tmp_pin ) 
    -- 
    ) ON COMMIT DROP; 

--
--

    --
    --  Check input parameters ... 
    --
    
--
--

IF p_input_row_json_array IS NULL THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_query');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

    v_raise_message := utility.fcn_console_message('Unpack the array-format input-parameter "p_input_row_json_array" into a table-format.');  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    WHILE ( v_loop_current_iteration <= v_input_array_cardinality ) 
    LOOP 
    --

      INSERT INTO tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input 
      (
        domain_code_name 
      , measured_timestamp 
      --
      -- 
      , toplevel 
      , queryid 
      , query 
      , plans 
      , total_plan_time 
      , min_plan_time 
      , max_plan_time 
      , mean_plan_time 
      , stddev_plan_time 
      , calls 
      , total_exec_time
      , min_exec_time 
      , max_exec_time 
      , mean_exec_time 
      , stddev_exec_time 
      , rows 
      , shared_blks_hit 
      , shared_blks_read 
      , shared_blks_dirtied 
      , shared_blks_written 
      , local_blks_hit 
      , local_blks_read 
      , local_blks_dirtied 
      , local_blks_written 
      , temp_blks_read 
      , temp_blks_written 
      , blk_read_time 
      , blk_write_time 
      , wal_records 
      , wal_fpi 
      , wal_bytes 
      --
      , rolname 
      --
      -- 
      )
      
        SELECT  A.domain_code_name 
        ,       A.measured_timestamp 
        --      
        --      
        ,     coalesce( A.toplevel , true )  AS  toplevel 
        ,       A.queryid 
        ,       A.query 
        ,       A.plans 
        ,       A.total_plan_time 
        ,       A.min_plan_time 
        ,       A.max_plan_time 
        ,       A.mean_plan_time 
        ,       A.stddev_plan_time 
        ,       A.calls 
        ,       A.total_exec_time
        ,       A.min_exec_time 
        ,       A.max_exec_time 
        ,       A.mean_exec_time 
        ,       A.stddev_exec_time 
        ,       A.rows 
        ,       A.shared_blks_hit 
        ,       A.shared_blks_read 
        ,       A.shared_blks_dirtied 
        ,       A.shared_blks_written 
        ,       A.local_blks_hit 
        ,       A.local_blks_read 
        ,       A.local_blks_dirtied 
        ,       A.local_blks_written 
        ,       A.temp_blks_read 
        ,       A.temp_blks_written 
        ,       A.blk_read_time 
        ,       A.blk_write_time 
        ,       A.wal_records 
        ,       A.wal_fpi 
        ,       A.wal_bytes 
        --      
        ,       A.rolname 
        --
        --
        FROM  json_populate_record( null::tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input 
                                  , p_input_row_json_array[v_loop_current_iteration] )  AS  A  
        --
        ;
        
    --
    --
                
        v_loop_current_iteration := v_loop_current_iteration + 1; 
    
    --
    --
                
    --
    END LOOP; 
    
--
--
      
    v_row_count := v_input_array_cardinality; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

IF v_row_count = 0 THEN 

  v_raise_message := utility.fcn_console_message('No record information was provided for insertion.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := utility.fcn_console_message('Exiting procedure without any action performed.');
  RAISE NOTICE '%' , v_raise_message; 

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_query');  
    RAISE NOTICE '%' , v_raise_message; 
    
  RETURN; 

END IF; 

--
--

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_code_name IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "domain_code_name" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "domain_code_name" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.measured_timestamp IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "measured_timestamp" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "measured_timestamp" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.queryid IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "queryid" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "queryid" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.toplevel IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "toplevel" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "toplevel" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.rolname IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "p_input_row_json_array" entry has NULL "rolname" coordinate-value.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'A non-null "rolname" coordinate-value must be provided for every entry in "p_input_row_json_array".';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--

    --
    --  Validate request ... 
    --
    
--
--

    v_raise_message := utility.fcn_console_message('Map all relevant "domain_code_name" values to their associated "domain" record (by "pin" value).');  
    RAISE NOTICE '%' , v_raise_message; 
    
      UPDATE  tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  UU 
      SET     domain_pin = X.pin 
      FROM    metric.domain  AS  X  
      WHERE   lower( UU.domain_code_name ) = lower( X.code_name ) 
      --
      ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
        
--
--
        
IF ( EXISTS ( SELECT  null 
              FROM    tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  X 
              WHERE   X.domain_pin IS NULL ) ) THEN 

  v_raise_message := utility.fcn_console_message('At least one provided "domain_code_name" coordinate-value does not match any rows in the "domain" table.');
  RAISE NOTICE '%' , v_raise_message; 
  
  v_raise_message := 'Each provided "domain_code_name" coordinate-value must match a row in the "domain" table.';
  RAISE EXCEPTION '%' , v_raise_message; 
  
END IF; 

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  X 
                INNER JOIN  metric.postgres_query  AS  E  ON  X.domain_pin = E.domain_pin 
                                                          AND X.measured_timestamp = E.measured_timestamp ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference and "measured_timestamp" coordinate-value already exists in the "postgres_query" table.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'A new measurement record can not be inserted with an existing "domain" reference and "measured_timestamp" coordinate-value.';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         

--
--
        
    IF EXISTS ( SELECT  null 
                FROM    tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  X 
                GROUP BY  X.domain_pin 
                ,         X.measured_timestamp 
                --
                ,         X.queryid 
                ,         X.toplevel 
                ,         X.rolname 
                --
                HAVING  COUNT(*) > 1 ) 
    THEN 

        v_raise_message := utility.fcn_console_message('At least one combination of a "domain" reference, "measured_timestamp" coordinate-value, "queryid", "toplevel", and "rolname" appears more than once in the staged insert-proposal row-set.');
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'The provided "p_input_row_json_array" parameter-value must not contain any duplicate combinations of a "domain" reference, "measured_timestamp" coordinate-value, "queryid", "toplevel", and "rolname".';
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF;         
    
--
--
        
--
--

--
--

    --
    --  Perform request ... 
    --
    
--
--
    
    v_raise_message := utility.fcn_console_message('Insert new records into "metric"."postgres_query":');  
    RAISE NOTICE '%' , v_raise_message; 
    
      INSERT INTO metric.postgres_query 
      (
        domain_pin 
      --
      , measured_timestamp 
      --
      -- 
      , toplevel 
      , queryid 
      , query 
      , plans 
      , total_plan_time 
      , min_plan_time 
      , max_plan_time 
      , mean_plan_time 
      , stddev_plan_time 
      , calls 
      , total_exec_time
      , min_exec_time 
      , max_exec_time 
      , mean_exec_time 
      , stddev_exec_time 
      , rows 
      , shared_blks_hit 
      , shared_blks_read 
      , shared_blks_dirtied 
      , shared_blks_written 
      , local_blks_hit 
      , local_blks_read 
      , local_blks_dirtied 
      , local_blks_written 
      , temp_blks_read 
      , temp_blks_written 
      , blk_read_time 
      , blk_write_time 
      , wal_records 
      , wal_fpi 
      , wal_bytes 
      --
      , rolname 
      -- 
      --
      )  
    
        SELECT  N.domain_pin 
        --
        ,       N.measured_timestamp 
        -- 
        -- 
        ,       N.toplevel 
        ,       N.queryid 
        ,       N.query 
        ,       N.plans 
        ,       N.total_plan_time 
        ,       N.min_plan_time 
        ,       N.max_plan_time 
        ,       N.mean_plan_time 
        ,       N.stddev_plan_time 
        ,       N.calls 
        ,       N.total_exec_time
        ,       N.min_exec_time 
        ,       N.max_exec_time 
        ,       N.mean_exec_time 
        ,       N.stddev_exec_time 
        ,       N.rows 
        ,       N.shared_blks_hit 
        ,       N.shared_blks_read 
        ,       N.shared_blks_dirtied 
        ,       N.shared_blks_written 
        ,       N.local_blks_hit 
        ,       N.local_blks_read 
        ,       N.local_blks_dirtied 
        ,       N.local_blks_written 
        ,       N.temp_blks_read 
        ,       N.temp_blks_written 
        ,       N.blk_read_time 
        ,       N.blk_write_time 
        ,       N.wal_records 
        ,       N.wal_fpi 
        ,       N.wal_bytes 
        --
        ,       N.rolname 
        -- 
        -- 
        FROM  tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input  AS  N  
        --
        LEFT  JOIN  metric.postgres_query  AS  E  ON  N.domain_pin = E.domain_pin 
                                                  AND N.measured_timestamp = E.measured_timestamp 
        --
        WHERE  E.pin IS NULL 
        --
        ORDER BY  N.tmp_pin  ASC             
        -- 
        ;  

    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
                
--
--

--
--

    --
    --  Drop temporary tables & finish/exit routine ... 
    --
    
-- 
-- 

    DROP TABLE tmp_internal_prc_i_p_q_input_row_json_array_unpacked_input; 
    
--
--

    v_raise_message := utility.fcn_console_message('END :: metric_exec.prc_insert_postgres_query');  
    RAISE NOTICE '%' , v_raise_message; 
    
-- 
-- 
END main_block; 
$main_def$ LANGUAGE plpgsql 
               SECURITY DEFINER 
               SET search_path = utility, pg_temp;

--
--
--
--    

--
--
--
--

--
--

/*** SELECT postgres_database info ***/ 

/***********************************************
 
    FUNCTION: metric_exec.fcn_get_postgres_database_info 
    
    PARAMETER(S):  p_latest_measured_timestamp   timestamp   OPTIONAL  
                   p_domain_code_name            text        OPTIONAL  
                       
    
    DESCRIPTION: 
    --  
    --  Returns information from the latest "metric"."postgres_database" record 
    --   with "measured_timestamp" not later than the provided "p_latest_measured_timestamp" value, if non-null, 
    --    and restricted to the "domain"-row reference matching the provided "p_domain_code_name" value, if non-null. 
    -- 
    --      Only "domain" rows with "is_active" = TRUE are returned (others are assumed to be not relevant for reporting). 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  metric_exec.fcn_get_postgres_database_info   
              (   p_latest_measured_timestamp  =>  null  -- '2022-07-04 16:15:00.000'::timestamp 
              -- 
              ,   p_domain_code_name  =>  null  -- 'performance' 
              -- 
              )  
                 AS  X 
     --
     ORDER BY  X.display_order  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION metric_exec.fcn_get_postgres_database_info ( p_latest_measured_timestamp timestamp DEFAULT null 
                                                           -- 
                                                           , p_domain_code_name text DEFAULT null ) 
-- 
RETURNS TABLE ( 
  display_order int 
--
, domain_class_code_name text 
, domain_class_display_name text 
--
, domain_code_name text 
, domain_display_name text 
--
--
, measured_timestamp timestamp 
--
--
, numbackends int 
, xact_commit bigint 
, xact_rollback bigint 
, blks_read bigint 
, blks_hit bigint 
, tup_returned bigint 
, tup_fetched bigint 
, tup_inserted bigint 
, tup_updated bigint 
, tup_deleted bigint 
, conflicts bigint 
, temp_files bigint 
, temp_bytes bigint 
, deadlocks bigint 
, checksum_failures bigint 
, checksum_last_failure timestamp 
, blk_read_time numeric(32,16) 
, blk_write_time numeric(32,16) 
, session_time numeric(32,16) 
, active_time numeric(32,16) 
, idle_in_transaction_time numeric(32,16) 
, sessions bigint 
, sessions_abandoned bigint 
, sessions_fatal bigint 
, sessions_killed bigint 
, stats_reset timestamp 
-- 
, database_size bigint 
--
-- 
) 
--
--
AS $main_def$ 
    
    
    WITH cte_relevant_domain AS 
    ( 
      SELECT  D.pin  AS  domain_pin 
      -- 
      ,       C.code_name  AS  domain_class_code_name 
      ,       C.display_name  AS  domain_class_display_name 
      -- 
      ,       D.code_name  AS  domain_code_name 
      ,       D.display_name  AS  domain_display_name 
      --
      ,       D.display_order_rank  
      -- 
      FROM  metric.domain  AS  D  
      INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
      -- 
      WHERE  ( p_domain_code_name IS NULL 
             OR lower( D.code_name ) = lower( p_domain_code_name ) ) 
      -- 
      AND  D.is_active = true  
      -- 
    ) 
    
    , cte_latest_timestamp_by_domain AS 
    ( 
      SELECT  Y.domain_pin 
      -- 
      ,   MAX( X.measured_timestamp )  AS  latest_measured_timestamp    
      -- 
      FROM  cte_relevant_domain  AS  Y 
      INNER JOIN  metric.postgres_database  AS  X  ON  Y.domain_pin = X.domain_pin 
      -- 
      WHERE  ( p_latest_measured_timestamp IS NULL 
             OR X.measured_timestamp <= p_latest_measured_timestamp ) 
      -- 
      GROUP BY  Y.domain_pin  
      -- 
    ) 
    
        SELECT  ROW_NUMBER() OVER( ORDER BY RD.display_order_rank )::int  AS  display_order 
        --
        ,       RD.domain_class_code_name 
        ,       RD.domain_class_display_name 
        -- 
        ,       RD.domain_code_name 
        ,       RD.domain_display_name 
        -- 
        -- 
        ,       M.measured_timestamp 
        -- 
        -- 
        ,       M.numbackends 
        ,       M.xact_commit 
        ,       M.xact_rollback 
        ,       M.blks_read 
        ,       M.blks_hit 
        ,       M.tup_returned 
        ,       M.tup_fetched 
        ,       M.tup_inserted 
        ,       M.tup_updated 
        ,       M.tup_deleted 
        ,       M.conflicts 
        ,       M.temp_files 
        ,       M.temp_bytes 
        ,       M.deadlocks 
        ,       M.checksum_failures 
        ,       M.checksum_last_failure 
        ,       M.blk_read_time 
        ,       M.blk_write_time 
        ,       M.session_time 
        ,       M.active_time 
        ,       M.idle_in_transaction_time 
        ,       M.sessions 
        ,       M.sessions_abandoned 
        ,       M.sessions_fatal 
        ,       M.sessions_killed 
        ,       M.stats_reset 
        -- 
        ,       M.database_size 
        --
        -- 
        FROM   cte_latest_timestamp_by_domain  AS  LT 
        --
        INNER JOIN  metric.postgres_database  AS  M  ON  LT.domain_pin = M.domain_pin 
                                                     AND LT.latest_measured_timestamp = M.measured_timestamp 
        --
        INNER JOIN  cte_relevant_domain  AS  RD  ON  M.domain_pin = RD.domain_pin  
        --
        ;  
        
        
$main_def$ LANGUAGE sql 
           STABLE 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** SELECT postgres_table info ***/ 

/***********************************************
 
    FUNCTION: metric_exec.fcn_get_postgres_table_info 
    
    PARAMETER(S):  p_latest_measured_timestamp              timestamp   OPTIONAL  
                   p_domain_code_name                       text        OPTIONAL  
                   p_include_schema_level_summary_records   boolean     OPTIONAL  
                       
    
    DESCRIPTION: 
    --  
    --  Returns information from the latest "metric"."postgres_table" record 
    --   with "measured_timestamp" not later than the provided "p_latest_measured_timestamp" value, if non-null, 
    --    and restricted to the "domain"-row reference matching the provided "p_domain_code_name" value, if non-null. 
    -- 
    --      Only "domain" rows with "is_active" = TRUE are returned (others are assumed to be not relevant for reporting). 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  metric_exec.fcn_get_postgres_table_info   
              (   p_latest_measured_timestamp  =>  null  -- '2022-07-04 16:15:00.000'::timestamp 
              -- 
              ,   p_domain_code_name  =>  null  -- 'performance' 
              -- 
              ,   p_include_schema_level_summary_records  =>  true  
              -- 
              )  
                 AS  X 
     --
     ORDER BY  X.display_order  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION metric_exec.fcn_get_postgres_table_info ( p_latest_measured_timestamp timestamp DEFAULT null 
                                                        -- 
                                                        , p_domain_code_name text DEFAULT null 
                                                        -- 
                                                        , p_include_schema_level_summary_records boolean DEFAULT false ) 
-- 
RETURNS TABLE ( 
  display_order int 
--
, domain_class_code_name text 
, domain_class_display_name text 
--
, domain_code_name text 
, domain_display_name text 
--
--
, measured_timestamp timestamp 
--
--
, schemaname text 
, relname text 
, seq_scan bigint 
, seq_tup_read bigint 
, idx_scan bigint 
, idx_tup_fetch bigint 
, n_tup_ins bigint 
, n_tup_upd bigint 
, n_tup_del bigint 
, n_tup_hot_upd bigint 
, n_live_tup bigint 
, n_dead_tup bigint 
, n_mod_since_analyze bigint 
, n_ins_since_vacuum bigint 
, last_vacuum timestamp 
, last_autovacuum timestamp 
, last_analyze timestamp 
, last_autoanalyze timestamp 
, vacuum_count bigint 
, autovacuum_count bigint 
, analyze_count bigint 
, autoanalyze_count bigint 
--
, heap_blks_read bigint 
, heap_blks_hit bigint 
, idx_blks_read bigint 
, idx_blks_hit bigint 
, toast_blks_read bigint 
, toast_blks_hit bigint 
, tidx_blks_read bigint 
, tidx_blks_hit bigint 
-- 
, total_relation_size bigint 
, table_size bigint 
, indexes_size bigint 
--
-- 
) 
-- 
--
AS $main_def$ 
    
    
    WITH cte_relevant_domain AS 
    ( 
      SELECT  D.pin  AS  domain_pin 
      -- 
      ,       C.code_name  AS  domain_class_code_name 
      ,       C.display_name  AS  domain_class_display_name 
      -- 
      ,       D.code_name  AS  domain_code_name 
      ,       D.display_name  AS  domain_display_name 
      --
      ,       D.display_order_rank  
      -- 
      FROM  metric.domain  AS  D  
      INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
      -- 
      WHERE  ( p_domain_code_name IS NULL 
             OR lower( D.code_name ) = lower( p_domain_code_name ) ) 
      -- 
      AND  D.is_active = true  
      -- 
    ) 
    
    , cte_latest_timestamp_by_domain AS 
    ( 
      SELECT  Y.domain_pin 
      -- 
      ,   MAX( X.measured_timestamp )  AS  latest_measured_timestamp    
      -- 
      FROM  cte_relevant_domain  AS  Y 
      INNER JOIN  metric.postgres_table  AS  X  ON  Y.domain_pin = X.domain_pin 
      -- 
      WHERE  ( p_latest_measured_timestamp IS NULL 
             OR X.measured_timestamp <= p_latest_measured_timestamp ) 
      -- 
      GROUP BY  Y.domain_pin  
      -- 
    ) 
    
        SELECT  ROW_NUMBER() OVER( ORDER BY RD.display_order_rank , M.schemaname , M.relname )::int  AS  display_order 
        --
        ,       RD.domain_class_code_name 
        ,       RD.domain_class_display_name 
        -- 
        ,       RD.domain_code_name 
        ,       RD.domain_display_name 
        -- 
        -- 
        ,       M.measured_timestamp 
        -- 
        -- 
        ,       M.schemaname 
        ,       M.relname 
        ,       M.seq_scan 
        ,       M.seq_tup_read 
        ,       M.idx_scan 
        ,       M.idx_tup_fetch 
        ,       M.n_tup_ins 
        ,       M.n_tup_upd 
        ,       M.n_tup_del 
        ,       M.n_tup_hot_upd 
        ,       M.n_live_tup 
        ,       M.n_dead_tup 
        ,       M.n_mod_since_analyze 
        ,       M.n_ins_since_vacuum 
        ,       M.last_vacuum 
        ,       M.last_autovacuum 
        ,       M.last_analyze 
        ,       M.last_autoanalyze 
        ,       M.vacuum_count 
        ,       M.autovacuum_count 
        ,       M.analyze_count 
        ,       M.autoanalyze_count 
        -- 
        ,       M.heap_blks_read 
        ,       M.heap_blks_hit 
        ,       M.idx_blks_read 
        ,       M.idx_blks_hit 
        ,       M.toast_blks_read 
        ,       M.toast_blks_hit 
        ,       M.tidx_blks_read 
        ,       M.tidx_blks_hit 
        -- 
        ,       M.total_relation_size 
        ,       M.table_size 
        ,       M.indexes_size 
        --
        -- 
        FROM   cte_latest_timestamp_by_domain  AS  LT 
        --
        INNER JOIN  metric.postgres_table  AS  M  ON  LT.domain_pin = M.domain_pin 
                                                  AND LT.latest_measured_timestamp = M.measured_timestamp 
        --
        INNER JOIN  cte_relevant_domain  AS  RD  ON  M.domain_pin = RD.domain_pin  
        --
        
    UNION ALL 
    
        SELECT  - ROW_NUMBER() OVER( ORDER BY RD.display_order_rank DESC , M.schemaname DESC )::int  AS  display_order 
        --
        ,       RD.domain_class_code_name 
        ,       RD.domain_class_display_name 
        -- 
        ,       RD.domain_code_name 
        ,       RD.domain_display_name 
        -- 
        -- 
        ,       M.measured_timestamp 
        -- 
        -- 
        ,       M.schemaname 
        ,       'ALL TABLES IN SCHEMA'::text  AS  relname 
        ,  SUM( M.seq_scan            )::bigint  AS  seq_scan           
        ,  SUM( M.seq_tup_read        )::bigint  AS  seq_tup_read       
        ,  SUM( M.idx_scan            )::bigint  AS  idx_scan           
        ,  SUM( M.idx_tup_fetch       )::bigint  AS  idx_tup_fetch      
        ,  SUM( M.n_tup_ins           )::bigint  AS  n_tup_ins          
        ,  SUM( M.n_tup_upd           )::bigint  AS  n_tup_upd          
        ,  SUM( M.n_tup_del           )::bigint  AS  n_tup_del          
        ,  SUM( M.n_tup_hot_upd       )::bigint  AS  n_tup_hot_upd      
        ,  SUM( M.n_live_tup          )::bigint  AS  n_live_tup         
        ,  SUM( M.n_dead_tup          )::bigint  AS  n_dead_tup         
        ,  SUM( M.n_mod_since_analyze )::bigint  AS  n_mod_since_analyze
        ,  SUM( M.n_ins_since_vacuum  )::bigint  AS  n_ins_since_vacuum 
        ,       null::timestamp  AS  last_vacuum 
        ,       null::timestamp  AS  last_autovacuum 
        ,       null::timestamp  AS  last_analyze 
        ,       null::timestamp  AS  last_autoanalyze 
        ,  SUM( M.vacuum_count      )::bigint  AS  vacuum_count     
        ,  SUM( M.autovacuum_count  )::bigint  AS  autovacuum_count 
        ,  SUM( M.analyze_count     )::bigint  AS  analyze_count    
        ,  SUM( M.autoanalyze_count )::bigint  AS  autoanalyze_count
        -- 
        ,  SUM( M.heap_blks_read  )::bigint  AS  heap_blks_read      
        ,  SUM( M.heap_blks_hit   )::bigint  AS  heap_blks_hit       
        ,  SUM( M.idx_blks_read   )::bigint  AS  idx_blks_read       
        ,  SUM( M.idx_blks_hit    )::bigint  AS  idx_blks_hit        
        ,  SUM( M.toast_blks_read )::bigint  AS  toast_blks_read     
        ,  SUM( M.toast_blks_hit  )::bigint  AS  toast_blks_hit      
        ,  SUM( M.tidx_blks_read  )::bigint  AS  tidx_blks_read      
        ,  SUM( M.tidx_blks_hit   )::bigint  AS  tidx_blks_hit       
        -- 
        ,  SUM( M.total_relation_size )::bigint  AS  total_relation_size  
        ,  SUM( M.table_size          )::bigint  AS  table_size           
        ,  SUM( M.indexes_size        )::bigint  AS  indexes_size         
        --
        -- 
        FROM   cte_latest_timestamp_by_domain  AS  LT 
        --
        INNER JOIN  metric.postgres_table  AS  M  ON  LT.domain_pin = M.domain_pin 
                                                  AND LT.latest_measured_timestamp = M.measured_timestamp 
        --
        INNER JOIN  cte_relevant_domain  AS  RD  ON  M.domain_pin = RD.domain_pin  
        --
        WHERE  p_include_schema_level_summary_records = true 
        --
        GROUP BY  RD.display_order_rank
        ,         RD.domain_class_code_name 
        ,         RD.domain_class_display_name 
        ,         RD.domain_code_name 
        ,         RD.domain_display_name 
        ,         M.measured_timestamp 
        ,         M.schemaname 
        -- 
        ;  
        
        
$main_def$ LANGUAGE sql 
           STABLE 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** SELECT postgres_index info ***/ 

/***********************************************
 
    FUNCTION: metric_exec.fcn_get_postgres_index_info 
    
    PARAMETER(S):  p_latest_measured_timestamp              timestamp   OPTIONAL  
                   p_domain_code_name                       text        OPTIONAL  
                   p_include_schema_level_summary_records   boolean     OPTIONAL 

    
    DESCRIPTION: 
    --  
    --  Returns information from the latest "metric"."postgres_index" record 
    --   with "measured_timestamp" not later than the provided "p_latest_measured_timestamp" value, if non-null, 
    --    and restricted to the "domain"-row reference matching the provided "p_domain_code_name" value, if non-null. 
    -- 
    --      Only "domain" rows with "is_active" = TRUE are returned (others are assumed to be not relevant for reporting). 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  metric_exec.fcn_get_postgres_index_info   
              (   p_latest_measured_timestamp  =>  null  -- '2022-07-04 16:15:00.000'::timestamp 
              -- 
              ,   p_domain_code_name  =>  null  -- 'performance' 
              -- 
              ,   p_include_schema_level_summary_records  =>  true  
              -- 
              )  
                 AS  X 
     --
     ORDER BY  X.display_order  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION metric_exec.fcn_get_postgres_index_info ( p_latest_measured_timestamp timestamp DEFAULT null 
                                                        -- 
                                                        , p_domain_code_name text DEFAULT null 
                                                        -- 
                                                        , p_include_schema_level_summary_records boolean DEFAULT false ) 
-- 
RETURNS TABLE ( 
  display_order int 
--
, domain_class_code_name text 
, domain_class_display_name text 
--
, domain_code_name text 
, domain_display_name text 
--
--
, measured_timestamp timestamp 
--
--
, schemaname text 
, relname text 
, indexrelname text 
, idx_scan bigint 
, idx_tup_read bigint 
, idx_tup_fetch bigint 
--
, idx_blks_read bigint 
, idx_blks_hit bigint 
--
, total_relation_size bigint 
--
-- 
) 
-- 
--
AS $main_def$ 
    
    
    WITH cte_relevant_domain AS 
    ( 
      SELECT  D.pin  AS  domain_pin 
      -- 
      ,       C.code_name  AS  domain_class_code_name 
      ,       C.display_name  AS  domain_class_display_name 
      -- 
      ,       D.code_name  AS  domain_code_name 
      ,       D.display_name  AS  domain_display_name 
      --
      ,       D.display_order_rank  
      -- 
      FROM  metric.domain  AS  D  
      INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
      -- 
      WHERE  ( p_domain_code_name IS NULL 
             OR lower( D.code_name ) = lower( p_domain_code_name ) ) 
      -- 
      AND  D.is_active = true  
      -- 
    ) 
    
    , cte_latest_timestamp_by_domain AS 
    ( 
      SELECT  Y.domain_pin 
      -- 
      ,   MAX( X.measured_timestamp )  AS  latest_measured_timestamp    
      -- 
      FROM  cte_relevant_domain  AS  Y 
      INNER JOIN  metric.postgres_index  AS  X  ON  Y.domain_pin = X.domain_pin 
      -- 
      WHERE  ( p_latest_measured_timestamp IS NULL 
             OR X.measured_timestamp <= p_latest_measured_timestamp ) 
      -- 
      GROUP BY  Y.domain_pin  
      -- 
    ) 
    
        SELECT  ROW_NUMBER() OVER( ORDER BY RD.display_order_rank , M.schemaname , M.relname , M.indexrelname )::int  AS  display_order 
        --
        ,       RD.domain_class_code_name 
        ,       RD.domain_class_display_name 
        -- 
        ,       RD.domain_code_name 
        ,       RD.domain_display_name 
        -- 
        -- 
        ,       M.measured_timestamp 
        -- 
        -- 
        ,       M.schemaname 
        ,       M.relname 
        ,       M.indexrelname 
        ,       M.idx_scan 
        ,       M.idx_tup_read 
        ,       M.idx_tup_fetch 
        -- 
        ,       M.idx_blks_read 
        ,       M.idx_blks_hit 
        -- 
        ,       M.total_relation_size  
        --
        -- 
        FROM   cte_latest_timestamp_by_domain  AS  LT 
        --
        INNER JOIN  metric.postgres_index  AS  M  ON  LT.domain_pin = M.domain_pin 
                                                  AND LT.latest_measured_timestamp = M.measured_timestamp 
        --
        INNER JOIN  cte_relevant_domain  AS  RD  ON  M.domain_pin = RD.domain_pin  
        --
        
    UNION ALL 
    
        SELECT  - ROW_NUMBER() OVER( ORDER BY RD.display_order_rank DESC , M.schemaname DESC )::int  AS  display_order 
        --
        ,       RD.domain_class_code_name 
        ,       RD.domain_class_display_name 
        -- 
        ,       RD.domain_code_name 
        ,       RD.domain_display_name 
        -- 
        -- 
        ,       M.measured_timestamp 
        -- 
        -- 
        ,       M.schemaname 
        ,       ''::text  AS  relname 
        ,       'ALL INDEXES IN SCHEMA'::text  AS  indexrelname 
        ,  SUM( M.idx_scan      )::bigint  AS  idx_scan     
        ,  SUM( M.idx_tup_read  )::bigint  AS  idx_tup_read 
        ,  SUM( M.idx_tup_fetch )::bigint  AS  idx_tup_fetch
        --                                     
        ,  SUM( M.idx_blks_read )::bigint  AS  idx_blks_read 
        ,  SUM( M.idx_blks_hit  )::bigint  AS  idx_blks_hit 
        -- 
        ,  SUM( M.total_relation_size )::bigint  AS  total_relation_size 
        --
        -- 
        FROM   cte_latest_timestamp_by_domain  AS  LT 
        --
        INNER JOIN  metric.postgres_index  AS  M  ON  LT.domain_pin = M.domain_pin 
                                                  AND LT.latest_measured_timestamp = M.measured_timestamp 
        --
        INNER JOIN  cte_relevant_domain  AS  RD  ON  M.domain_pin = RD.domain_pin  
        --
        WHERE  p_include_schema_level_summary_records = true 
        --
        GROUP BY  RD.display_order_rank
        ,         RD.domain_class_code_name 
        ,         RD.domain_class_display_name 
        ,         RD.domain_code_name 
        ,         RD.domain_display_name 
        ,         M.measured_timestamp 
        ,         M.schemaname 
        -- 
        ;  
        
        
$main_def$ LANGUAGE sql 
           STABLE 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** SELECT postgres_function info ***/ 

/***********************************************
 
    FUNCTION: metric_exec.fcn_get_postgres_function_info 
    
    PARAMETER(S):  p_latest_measured_timestamp              timestamp   OPTIONAL  
                   p_domain_code_name                       text        OPTIONAL  
                   p_include_schema_level_summary_records   boolean     OPTIONAL  
                       
    
    DESCRIPTION: 
    --  
    --  Returns information from the latest "metric"."postgres_function" record 
    --   with "measured_timestamp" not later than the provided "p_latest_measured_timestamp" value, if non-null, 
    --    and restricted to the "domain"-row reference matching the provided "p_domain_code_name" value, if non-null. 
    -- 
    --      Only "domain" rows with "is_active" = TRUE are returned (others are assumed to be not relevant for reporting). 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  metric_exec.fcn_get_postgres_function_info   
              (   p_latest_measured_timestamp  =>  null  -- '2022-07-04 16:15:00.000'::timestamp 
              -- 
              ,   p_domain_code_name  =>  null  -- 'performance' 
              -- 
              ,   p_include_schema_level_summary_records  =>  true  
              -- 
              )  
                 AS  X 
     --
     ORDER BY  X.display_order  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION metric_exec.fcn_get_postgres_function_info ( p_latest_measured_timestamp timestamp DEFAULT null 
                                                           -- 
                                                           , p_domain_code_name text DEFAULT null 
                                                           -- 
                                                           , p_include_schema_level_summary_records boolean DEFAULT false ) 
-- 
RETURNS TABLE ( 
  display_order int 
--
, domain_class_code_name text 
, domain_class_display_name text 
--
, domain_code_name text 
, domain_display_name text 
--
--
, measured_timestamp timestamp 
--
--
, schemaname text 
, funcname text 
, calls bigint 
, total_time numeric(32,16) 
, self_time numeric(32,16) 
--
-- 
) 
-- 
--
AS $main_def$          
    
    
    WITH cte_relevant_domain AS 
    ( 
      SELECT  D.pin  AS  domain_pin 
      -- 
      ,       C.code_name  AS  domain_class_code_name 
      ,       C.display_name  AS  domain_class_display_name 
      -- 
      ,       D.code_name  AS  domain_code_name 
      ,       D.display_name  AS  domain_display_name 
      --
      ,       D.display_order_rank  
      -- 
      FROM  metric.domain  AS  D  
      INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
      -- 
      WHERE  ( p_domain_code_name IS NULL 
             OR lower( D.code_name ) = lower( p_domain_code_name ) ) 
      -- 
      AND  D.is_active = true  
      -- 
    ) 
    
    , cte_latest_timestamp_by_domain AS 
    ( 
      SELECT  Y.domain_pin 
      -- 
      ,   MAX( X.measured_timestamp )  AS  latest_measured_timestamp    
      -- 
      FROM  cte_relevant_domain  AS  Y 
      INNER JOIN  metric.postgres_function  AS  X  ON  Y.domain_pin = X.domain_pin 
      -- 
      WHERE  ( p_latest_measured_timestamp IS NULL 
             OR X.measured_timestamp <= p_latest_measured_timestamp ) 
      -- 
      GROUP BY  Y.domain_pin  
      -- 
    ) 
    
        SELECT  ROW_NUMBER() OVER( ORDER BY RD.display_order_rank , M.schemaname , M.funcname )::int  AS  display_order 
        --
        ,       RD.domain_class_code_name 
        ,       RD.domain_class_display_name 
        -- 
        ,       RD.domain_code_name 
        ,       RD.domain_display_name 
        -- 
        -- 
        ,       M.measured_timestamp 
        -- 
        -- 
        ,       M.schemaname 
        ,       M.funcname 
        ,       M.calls 
        ,       M.total_time 
        ,       M.self_time 
        --
        -- 
        FROM   cte_latest_timestamp_by_domain  AS  LT 
        --
        INNER JOIN  metric.postgres_function  AS  M  ON  LT.domain_pin = M.domain_pin 
                                                     AND LT.latest_measured_timestamp = M.measured_timestamp 
        --
        INNER JOIN  cte_relevant_domain  AS  RD  ON  M.domain_pin = RD.domain_pin  
        --
        
    UNION ALL 
    
        SELECT  - ROW_NUMBER() OVER( ORDER BY RD.display_order_rank DESC , M.schemaname DESC )::int  AS  display_order 
        --
        ,       RD.domain_class_code_name 
        ,       RD.domain_class_display_name 
        -- 
        ,       RD.domain_code_name 
        ,       RD.domain_display_name 
        -- 
        -- 
        ,       M.measured_timestamp 
        -- 
        -- 
        ,       M.schemaname 
        ,       'ALL (TRACKED) FUNCTIONS IN SCHEMA'::text  AS  funcname 
        ,  SUM( M.calls      )::bigint  AS  calls     
        ,  SUM( M.total_time )::numeric(32,16)  AS  total_time
        ,  SUM( M.self_time  )::numeric(32,16)  AS  self_time 
        --
        -- 
        FROM   cte_latest_timestamp_by_domain  AS  LT 
        --
        INNER JOIN  metric.postgres_function  AS  M  ON  LT.domain_pin = M.domain_pin 
                                                     AND LT.latest_measured_timestamp = M.measured_timestamp 
        --
        INNER JOIN  cte_relevant_domain  AS  RD  ON  M.domain_pin = RD.domain_pin  
        --
        WHERE  p_include_schema_level_summary_records = true 
        --
        GROUP BY  RD.display_order_rank
        ,         RD.domain_class_code_name 
        ,         RD.domain_class_display_name 
        ,         RD.domain_code_name 
        ,         RD.domain_display_name 
        ,         M.measured_timestamp 
        ,         M.schemaname 
        -- 
        ;  
        
        
$main_def$ LANGUAGE sql 
           STABLE 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** SELECT postgres_query info ***/ 

/***********************************************
 
    FUNCTION: metric_exec.fcn_get_postgres_query_info 
    
    PARAMETER(S):  p_latest_measured_timestamp   timestamp   OPTIONAL  
                   p_domain_code_name            text        OPTIONAL  
                       
    
    DESCRIPTION: 
    --  
    --  Returns information from the latest "metric"."postgres_query" record 
    --   with "measured_timestamp" not later than the provided "p_latest_measured_timestamp" value, if non-null, 
    --    and restricted to the "domain"-row reference matching the provided "p_domain_code_name" value, if non-null. 
    -- 
    --      Only "domain" rows with "is_active" = TRUE are returned (others are assumed to be not relevant for reporting). 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  metric_exec.fcn_get_postgres_query_info   
              (   p_latest_measured_timestamp  =>  null  -- '2022-07-04 16:15:00.000'::timestamp 
              -- 
              ,   p_domain_code_name  =>  null  -- 'performance' 
              -- 
              )  
                 AS  X 
     --
     ORDER BY  X.display_order  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION metric_exec.fcn_get_postgres_query_info ( p_latest_measured_timestamp timestamp DEFAULT null 
                                                        -- 
                                                        , p_domain_code_name text DEFAULT null ) 
-- 
RETURNS TABLE ( 
  display_order int 
--
, domain_class_code_name text 
, domain_class_display_name text 
--
, domain_code_name text 
, domain_display_name text 
--
--
, measured_timestamp timestamp 
--
--
, toplevel bool 
, queryid bigint 
, query text 
, plans bigint 
, total_plan_time numeric(32,16) 
, min_plan_time numeric(32,16) 
, max_plan_time numeric(32,16) 
, mean_plan_time numeric(32,16) 
, stddev_plan_time numeric(32,16) 
, calls bigint 
, total_exec_time numeric(32,16) 
, min_exec_time numeric(32,16) 
, max_exec_time numeric(32,16) 
, mean_exec_time numeric(32,16) 
, stddev_exec_time numeric(32,16) 
, rows bigint 
, shared_blks_hit bigint 
, shared_blks_read bigint 
, shared_blks_dirtied bigint 
, shared_blks_written bigint 
, local_blks_hit bigint 
, local_blks_read bigint 
, local_blks_dirtied bigint 
, local_blks_written bigint 
, temp_blks_read bigint 
, temp_blks_written bigint 
, blk_read_time numeric(32,16) 
, blk_write_time numeric(32,16) 
, wal_records bigint 
, wal_fpi bigint 
, wal_bytes numeric(32,16) 
--
, rolname text 
--
-- 
) 
--
--
AS $main_def$          
    
    
    WITH cte_relevant_domain AS 
    ( 
      SELECT  D.pin  AS  domain_pin 
      -- 
      ,       C.code_name  AS  domain_class_code_name 
      ,       C.display_name  AS  domain_class_display_name 
      -- 
      ,       D.code_name  AS  domain_code_name 
      ,       D.display_name  AS  domain_display_name 
      --
      ,       D.display_order_rank  
      -- 
      FROM  metric.domain  AS  D  
      INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
      -- 
      WHERE  ( p_domain_code_name IS NULL 
             OR lower( D.code_name ) = lower( p_domain_code_name ) ) 
      -- 
      AND  D.is_active = true  
      -- 
    ) 
    
    , cte_latest_timestamp_by_domain AS 
    ( 
      SELECT  Y.domain_pin 
      -- 
      ,   MAX( X.measured_timestamp )  AS  latest_measured_timestamp    
      -- 
      FROM  cte_relevant_domain  AS  Y 
      INNER JOIN  metric.postgres_query  AS  X  ON  Y.domain_pin = X.domain_pin 
      -- 
      WHERE  ( p_latest_measured_timestamp IS NULL 
             OR X.measured_timestamp <= p_latest_measured_timestamp ) 
      -- 
      GROUP BY  Y.domain_pin  
      -- 
    ) 
    
        SELECT  ROW_NUMBER() OVER( ORDER BY RD.display_order_rank , M.queryid , M.toplevel , M.rolname )::int  AS  display_order 
        --
        ,       RD.domain_class_code_name 
        ,       RD.domain_class_display_name 
        -- 
        ,       RD.domain_code_name 
        ,       RD.domain_display_name 
        -- 
        -- 
        ,       M.measured_timestamp 
        -- 
        -- 
        ,       M.toplevel 
        ,       M.queryid 
        ,       M.query 
        ,       M.plans 
        ,       M.total_plan_time 
        ,       M.min_plan_time 
        ,       M.max_plan_time 
        ,       M.mean_plan_time 
        ,       M.stddev_plan_time 
        ,       M.calls 
        ,       M.total_exec_time 
        ,       M.min_exec_time 
        ,       M.max_exec_time 
        ,       M.mean_exec_time 
        ,       M.stddev_exec_time 
        ,       M.rows 
        ,       M.shared_blks_hit 
        ,       M.shared_blks_read 
        ,       M.shared_blks_dirtied 
        ,       M.shared_blks_written 
        ,       M.local_blks_hit 
        ,       M.local_blks_read  
        ,       M.local_blks_dirtied 
        ,       M.local_blks_written 
        ,       M.temp_blks_read 
        ,       M.temp_blks_written 
        ,       M.blk_read_time 
        ,       M.blk_write_time 
        ,       M.wal_records 
        ,       M.wal_fpi 
        ,       M.wal_bytes 
        --
        ,       M.rolname 
        --
        -- 
        FROM   cte_latest_timestamp_by_domain  AS  LT 
        --
        INNER JOIN  metric.postgres_query  AS  M  ON  LT.domain_pin = M.domain_pin 
                                                  AND LT.latest_measured_timestamp = M.measured_timestamp 
        --
        INNER JOIN  cte_relevant_domain  AS  RD  ON  M.domain_pin = RD.domain_pin  
        --
        ;  
        
        
$main_def$ LANGUAGE sql 
           STABLE 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

  --
  -- create functions in "report_exec" schema 
  -- 

--
--

/*** SELECT postgres_database info ***/ 

/***********************************************
 
    FUNCTION: report_exec.fcn_get_postgres_database_info 
    
    PARAMETER(S):  p_as_of_timestamp    timestamp   OPTIONAL  
                   p_domain_code_name   text        OPTIONAL  
                       
    
    DESCRIPTION: 
    --  
    --  Returns a list of databases with performance metrics. 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  report_exec.fcn_get_postgres_database_info   
               (   p_as_of_timestamp  =>  null  -- '2022-08-24 13:37:00.000'::timestamp 
               -- 
               ,   p_domain_code_name  =>  null  -- 'performance' 
               -- 
               )  
                  AS  X 
     --
     ORDER BY  X.line_number  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION report_exec.fcn_get_postgres_database_info ( p_as_of_timestamp timestamp DEFAULT null 
                                                           -- 
                                                           , p_domain_code_name text DEFAULT null ) 
-- 
RETURNS TABLE ( 
  line_number int 
--
, domain_code_name text 
, domain_display_name text 
, measured_timestamp timestamp 
--
, database_size text 
, size_change_1_day text 
, size_change_1_week text 
, size_change_1_month text 
--
, recent_deadlocks bigint 
, deadlocks_in_past_week bigint 
, deadlocks_in_past_month bigint 
--
) 
--
--
AS $main_def$          
        
        
    SELECT  ROW_NUMBER() OVER( ORDER BY C.display_order )::int  AS  line_number 
    -- 
    ,  C.domain_code_name 
    ,  C.domain_display_name 
    ,  C.measured_timestamp 
    --
    ,  pg_size_pretty ( C.database_size )  AS  database_size 
    ,  pg_size_pretty ( C.database_size - D.database_size )  AS  size_change_1_day 
    ,  pg_size_pretty ( C.database_size - W.database_size )  AS  size_change_1_week 
    ,  pg_size_pretty ( C.database_size - M.database_size )  AS  size_change_1_month 
    --
    ,  CASE WHEN X.stats_reset_since_week_back = false 
            AND  D.measured_timestamp IS NOT NULL   
            THEN C.deadlocks - coalesce( D.deadlocks , 0 ) 
            WHEN X.stats_reset_since_day_back = true 
            OR   ( D.measured_timestamp IS NULL 
                 AND W.measured_timestamp IS NULL 
                 AND M.measured_timestamp IS NULL 
                 ) 
            THEN C.deadlocks  
            ELSE null 
       END  AS  recent_deadlocks  
    ,  CASE WHEN X.stats_reset_since_week_back = false 
            AND  W.measured_timestamp IS NOT NULL   
            THEN C.deadlocks - coalesce( W.deadlocks , 0 ) 
            ELSE null 
       END  AS  deadlocks_in_past_week 
    ,  CASE WHEN X.stats_reset_since_month_back = false 
            AND  M.measured_timestamp IS NOT NULL   
            THEN C.deadlocks - coalesce( M.deadlocks , 0 ) 
            ELSE null 
       END  AS  deadlocks_in_past_month 
    --
    FROM  metric_exec.fcn_get_postgres_database_info ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) 
                                                     , p_domain_code_name )  AS  C 
    -- 
    LEFT  JOIN  metric_exec.fcn_get_postgres_database_info 
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 1 ) )::timestamp 
                  , p_domain_code_name ) 
                  AS  D  ON  C.domain_code_name = D.domain_code_name 
                         AND D.measured_timestamp BETWEEN C.measured_timestamp - make_interval( hours => 32 ) 
                                                      AND C.measured_timestamp - make_interval( hours => 16 ) 
    LEFT  JOIN  metric_exec.fcn_get_postgres_database_info                                                  
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 7 ) )::timestamp 
                  , p_domain_code_name ) 
                  AS  W  ON  C.domain_code_name = W.domain_code_name                                        
                         AND W.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 9 ) 
                                                      AND C.measured_timestamp - make_interval( days => 5 ) 
    LEFT  JOIN  metric_exec.fcn_get_postgres_database_info                                                  
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 30 ) )::timestamp 
                  , p_domain_code_name ) 
                  AS  M  ON  C.domain_code_name = M.domain_code_name                                        
                         AND M.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 33 ) 
                                                      AND C.measured_timestamp - make_interval( days => 27 ) 
    --
    LEFT JOIN LATERAL ( SELECT  CASE WHEN D.measured_timestamp < C.measured_timestamp 
                                     AND  ( C.stats_reset > D.stats_reset 
                                          OR ( C.stats_reset IS NOT NULL AND D.stats_reset IS NULL ) ) 
                                     THEN true 
                                     ELSE false 
                                END  AS  stats_reset_since_day_back 
                        --
                        ,       CASE WHEN W.measured_timestamp < C.measured_timestamp 
                                     AND  ( C.stats_reset > W.stats_reset 
                                          OR ( C.stats_reset IS NOT NULL AND W.stats_reset IS NULL ) ) 
                                     THEN true 
                                     ELSE false 
                                END  AS  stats_reset_since_week_back 
                        --
                        ,       CASE WHEN M.measured_timestamp < C.measured_timestamp 
                                     AND  ( C.stats_reset > M.stats_reset 
                                          OR ( C.stats_reset IS NOT NULL AND M.stats_reset IS NULL ) ) 
                                     THEN true 
                                     ELSE false 
                                END  AS  stats_reset_since_month_back 
                        --
                      )  AS  X  ON  true 
    --
    WHERE  C.measured_timestamp > coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 7 ) 
    --
    ;
            
            
$main_def$ LANGUAGE sql 
           STABLE 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** SELECT postgres_table info ***/ 

/***********************************************
 
    FUNCTION: report_exec.fcn_get_postgres_table_info 
    
    PARAMETER(S):  p_as_of_timestamp    timestamp   OPTIONAL  
                   p_domain_code_name   text        OPTIONAL  
                       
    
    DESCRIPTION: 
    --  
    --  Returns a list of tables with performance metrics. 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  report_exec.fcn_get_postgres_table_info   
               (   p_as_of_timestamp  =>  null  -- '2022-08-24 13:37:00.000'::timestamp 
               -- 
               ,   p_domain_code_name  =>  null  -- 'performance' 
               -- 
               )  
                  AS  X 
     --
     ORDER BY  X.line_number  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION report_exec.fcn_get_postgres_table_info ( p_as_of_timestamp timestamp DEFAULT null 
                                                        -- 
                                                        , p_domain_code_name text DEFAULT null ) 
-- 
RETURNS TABLE ( 
  line_number int 
--
, domain_code_name text 
, domain_display_name text 
, measured_timestamp timestamp 
--
, schema_name text 
, table_name text 
--
, total_size text 
, indexes_size text 
--
, estimated_row_count bigint 
, dead_tuples bigint 
, last_vacuum timestamp 
, last_autovacuum timestamp 
, last_analyze timestamp 
, last_autoanalyze timestamp 
--
, size_change_1_day text 
, size_change_1_week text 
, size_change_1_month text 
--
, dead_tuples_in_past_day bigint 
, dead_tuples_in_past_week bigint 
, dead_tuples_in_past_month bigint 
--
, index_scan_ratio numeric(5,2) 
--
) 
-- 
--
AS $main_def$          
        
        
    SELECT  ROW_NUMBER() OVER( ORDER BY  C.total_relation_size  DESC 
                               ,         C.n_live_tup           DESC 
                               ,         C.display_order  )::int  AS  line_number 
    -- 
    ,  C.domain_code_name 
    ,  C.domain_display_name 
    ,  C.measured_timestamp 
    --
    ,  C.schemaname  AS  schema_name 
    ,  C.relname  AS  table_name 
    --
    ,  pg_size_pretty ( C.total_relation_size )  AS  total_size 
    ,  pg_size_pretty ( C.indexes_size )  AS  indexes_size 
    --
    ,  C.n_live_tup  AS  estimated_row_count 
    ,  C.n_dead_tup  AS  dead_tuples 
    ,  C.last_vacuum 
    ,  C.last_autovacuum 
    ,  C.last_analyze 
    ,  C.last_autoanalyze 
    --
    ,  pg_size_pretty ( C.total_relation_size - D.total_relation_size )  AS  size_change_1_day 
    ,  pg_size_pretty ( C.total_relation_size - W.total_relation_size )  AS  size_change_1_week 
    ,  pg_size_pretty ( C.total_relation_size - M.total_relation_size )  AS  size_change_1_month 
    --
    ,  CASE WHEN D.measured_timestamp IS NOT NULL   
            THEN C.n_dead_tup - coalesce( D.n_dead_tup , 0 ) 
            ELSE null 
       END  AS  dead_tuples_in_past_day 
    ,  CASE WHEN W.measured_timestamp IS NOT NULL   
            THEN C.n_dead_tup - coalesce( W.n_dead_tup , 0 ) 
            ELSE null 
       END  AS  dead_tuples_in_past_week 
    ,  CASE WHEN M.measured_timestamp IS NOT NULL   
            THEN C.n_dead_tup - coalesce( M.n_dead_tup , 0 ) 
            ELSE null 
       END  AS  dead_tuples_in_past_month 
    --
    ,  CASE WHEN coalesce( C.idx_scan , 0 ) + coalesce( C.seq_scan , 0 ) > 0 
            THEN ( ( coalesce( C.idx_scan , 0 ) )::real 
                 / ( coalesce( C.idx_scan , 0 ) + coalesce( C.seq_scan , 0 ) )::real )::numeric(5,2) 
            ELSE null 
       END  AS  index_scan_ratio 
    --
    FROM  metric_exec.fcn_get_postgres_table_info ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) 
                                                  , p_domain_code_name 
                                                  , true )  AS  C 
    -- 
    LEFT  JOIN  metric_exec.fcn_get_postgres_table_info 
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 1 ) )::timestamp 
                  , p_domain_code_name 
                  , true )  
                  AS  D  ON  C.domain_code_name = D.domain_code_name 
                         AND C.schemaname = D.schemaname 
                         AND C.relname = D.relname 
                         AND D.measured_timestamp BETWEEN C.measured_timestamp - make_interval( hours => 32 ) 
                                                      AND C.measured_timestamp - make_interval( hours => 16 ) 
    LEFT  JOIN  metric_exec.fcn_get_postgres_table_info                                                  
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 7 ) )::timestamp 
                  , p_domain_code_name 
                  , true )  
                  AS  W  ON  C.domain_code_name = W.domain_code_name                               
                         AND C.schemaname = W.schemaname 
                         AND C.relname = W.relname          
                         AND W.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 9 ) 
                                                      AND C.measured_timestamp - make_interval( days => 5 ) 
    LEFT  JOIN  metric_exec.fcn_get_postgres_table_info                                        
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 30 ) )::timestamp 
                  , p_domain_code_name 
                  , true )  
                  AS  M  ON  C.domain_code_name = M.domain_code_name 
                         AND C.schemaname = M.schemaname 
                         AND C.relname = M.relname 
                         AND M.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 33 ) 
                                                      AND C.measured_timestamp - make_interval( days => 27 ) 
    --
    WHERE  C.measured_timestamp > coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 7 ) 
    --
    AND  C.schemaname NOT IN ( 'information_schema' , 'pg_catalog' , 'pg_toast' 
                             , '_timescaledb_cache' , '_timescaledb_catalog' , '_timescaledb_config' ) -- !! keep :: '_timescaledb_internal' !!  
    --
    ;
            
            
$main_def$ LANGUAGE sql 
           STABLE 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** SELECT postgres_index info ***/ 

/***********************************************
 
    FUNCTION: report_exec.fcn_get_postgres_index_info 
    
    PARAMETER(S):  p_as_of_timestamp    timestamp   OPTIONAL  
                   p_domain_code_name   text        OPTIONAL  
                       
    
    DESCRIPTION: 
    --  
    --  Returns a list of indexes with performance metrics. 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  report_exec.fcn_get_postgres_index_info   
               (   p_as_of_timestamp  =>  null  -- '2022-08-24 13:37:00.000'::timestamp 
               -- 
               ,   p_domain_code_name  =>  null  -- 'performance' 
               -- 
               )  
                  AS  X 
     --
     ORDER BY  X.line_number  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION report_exec.fcn_get_postgres_index_info ( p_as_of_timestamp timestamp DEFAULT null 
                                                        -- 
                                                        , p_domain_code_name text DEFAULT null ) 
-- 
RETURNS TABLE ( 
  line_number int 
--
, domain_code_name text 
, domain_display_name text 
, measured_timestamp timestamp 
--
, schema_name text 
, table_name text 
, index_name text 
--
, index_size text 
--
, scans bigint 
, mean_tuples_read numeric(33,1) 
, mean_tuples_fetched numeric(33,1) 
--
, size_change_1_day text 
, size_change_1_week text 
, size_change_1_month text 
--
, scans_change_1_day bigint 
, scans_change_1_week bigint 
, scans_change_1_month bigint 
--
, mean_tuples_change_1_day numeric(33,3) 
, mean_tuples_change_1_week numeric(33,3) 
, mean_tuples_change_1_month numeric(33,3) 
--
) 
--
--
AS $main_def$          
        
        
    SELECT  ROW_NUMBER() OVER( ORDER BY  coalesce(C.idx_tup_read,0) + coalesce(C.idx_tup_fetch,0)  DESC 
                               ,         coalesce(C.idx_scan,0)  DESC 
                               ,         C.total_relation_size  DESC 
                               ,         C.display_order  )::int  AS  line_number 
    -- 
    ,  C.domain_code_name 
    ,  C.domain_display_name 
    ,  C.measured_timestamp 
    --
    ,  C.schemaname  AS  schema_name 
    ,  C.relname  AS  table_name 
    ,  C.indexrelname  AS  index_name 
    --
    ,  pg_size_pretty ( C.total_relation_size )  AS  index_size 
    --
    ,  C.idx_scan  AS  scans 
    ,  X.mean_tuples_read::numeric(33,1)  AS  mean_tuples_read  
    ,  X.mean_tuples_fetched::numeric(33,1)  AS  mean_tuples_fetched  
    --
    ,  pg_size_pretty ( C.total_relation_size - D.total_relation_size )  AS  size_change_1_day 
    ,  pg_size_pretty ( C.total_relation_size - W.total_relation_size )  AS  size_change_1_week 
    ,  pg_size_pretty ( C.total_relation_size - M.total_relation_size )  AS  size_change_1_month 
    --
    ,  C.idx_scan - D.idx_scan  AS  scans_change_1_day 
    ,  C.idx_scan - W.idx_scan  AS  scans_change_1_week 
    ,  C.idx_scan - M.idx_scan  AS  scans_change_1_month 
    --
    ,  ( ( X.mean_tuples_read - X.mean_tuples_read_back_1_day 
         + X.mean_tuples_fetched - X.mean_tuples_fetched_back_1_day )::real 
       / 2::real )::numeric(33,3)  AS  mean_tuples_change_1_day 
    ,  ( ( X.mean_tuples_read - X.mean_tuples_read_back_1_week 
         + X.mean_tuples_fetched - X.mean_tuples_fetched_back_1_week )::real 
       / 2::real )::numeric(33,3)  AS  mean_tuples_change_1_week 
    ,  ( ( X.mean_tuples_read - X.mean_tuples_read_back_1_month 
         + X.mean_tuples_fetched - X.mean_tuples_fetched_back_1_month )::real 
       / 2::real )::numeric(33,3)  AS  mean_tuples_change_1_month 
    --
    FROM  metric_exec.fcn_get_postgres_index_info ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) 
                                                  , p_domain_code_name 
                                                  , true )  AS  C 
    -- 
    LEFT  JOIN  metric_exec.fcn_get_postgres_index_info 
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 1 ) )::timestamp 
                  , p_domain_code_name 
                  , true ) 
                  AS  D  ON  C.domain_code_name = D.domain_code_name 
                         AND C.schemaname = D.schemaname 
                         AND C.relname = D.relname 
                         AND C.indexrelname = D.indexrelname 
                         AND D.measured_timestamp BETWEEN C.measured_timestamp - make_interval( hours => 32 ) 
                                                      AND C.measured_timestamp - make_interval( hours => 16 ) 
    LEFT  JOIN  metric_exec.fcn_get_postgres_index_info                                                  
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 7 ) )::timestamp 
                  , p_domain_code_name 
                  , true ) 
                  AS  W  ON  C.domain_code_name = W.domain_code_name                               
                         AND C.schemaname = W.schemaname 
                         AND C.relname = W.relname 
                         AND C.indexrelname = W.indexrelname 
                         AND W.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 9 ) 
                                                      AND C.measured_timestamp - make_interval( days => 5 ) 
    LEFT  JOIN  metric_exec.fcn_get_postgres_index_info                                        
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 30 ) )::timestamp 
                  , p_domain_code_name 
                  , true ) 
                  AS  M  ON  C.domain_code_name = M.domain_code_name 
                         AND C.schemaname = M.schemaname 
                         AND C.relname = M.relname 
                         AND C.indexrelname = M.indexrelname 
                         AND M.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 33 ) 
                                                      AND C.measured_timestamp - make_interval( days => 27 ) 
    --
    LEFT JOIN LATERAL ( SELECT  CASE WHEN C.idx_scan > 0 AND C.idx_tup_read >= 0 
                                     THEN ( C.idx_tup_read::real / C.idx_scan::real )::numeric(33,3)  
                                      ELSE null 
                                END  AS  mean_tuples_read  
                        ,       CASE WHEN C.idx_scan > 0 AND C.idx_tup_fetch >= 0 
                                     THEN ( C.idx_tup_fetch::real / C.idx_scan::real )::numeric(33,3)  
                                      ELSE null 
                                END  AS  mean_tuples_fetched  
                        --
                        ,       CASE WHEN D.idx_scan > 0 AND D.idx_tup_read >= 0 
                                     THEN ( D.idx_tup_read::real / D.idx_scan::real )::numeric(33,3)  
                                      ELSE null 
                                END  AS  mean_tuples_read_back_1_day   
                        ,       CASE WHEN D.idx_scan > 0 AND D.idx_tup_fetch >= 0 
                                     THEN ( D.idx_tup_fetch::real / D.idx_scan::real )::numeric(33,3)  
                                      ELSE null 
                                END  AS  mean_tuples_fetched_back_1_day   
                        --
                        ,       CASE WHEN W.idx_scan > 0 AND W.idx_tup_read >= 0 
                                     THEN ( W.idx_tup_read::real / W.idx_scan::real )::numeric(33,3)  
                                      ELSE null 
                                END  AS  mean_tuples_read_back_1_week 
                        ,       CASE WHEN W.idx_scan > 0 AND W.idx_tup_fetch >= 0 
                                     THEN ( W.idx_tup_fetch::real / W.idx_scan::real )::numeric(33,3)  
                                      ELSE null 
                                END  AS  mean_tuples_fetched_back_1_week 
                        --
                        ,       CASE WHEN M.idx_scan > 0 AND M.idx_tup_read >= 0 
                                     THEN ( M.idx_tup_read::real / M.idx_scan::real )::numeric(33,3)  
                                      ELSE null 
                                END  AS  mean_tuples_read_back_1_month  
                        ,       CASE WHEN M.idx_scan > 0 AND M.idx_tup_fetch >= 0 
                                     THEN ( M.idx_tup_fetch::real / M.idx_scan::real )::numeric(33,3)  
                                      ELSE null 
                                END  AS  mean_tuples_fetched_back_1_month 
                        --
                      )  AS  X  ON  true 
    --
    WHERE  C.measured_timestamp > coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 7 ) 
    -- 
    AND  C.schemaname NOT IN ( 'information_schema' , 'pg_catalog' , 'pg_toast' 
                             , '_timescaledb_cache' , '_timescaledb_catalog' , '_timescaledb_config' ) -- !! keep :: '_timescaledb_internal' !!  
    --
    ;
            
            
$main_def$ LANGUAGE sql 
           STABLE 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** SELECT postgres_function info ***/ 

/***********************************************
 
    FUNCTION: report_exec.fcn_get_postgres_function_info 
    
    PARAMETER(S):  p_as_of_timestamp    timestamp   OPTIONAL  
                   p_domain_code_name   text        OPTIONAL  
                       
    
    DESCRIPTION: 
    --  
    --  Returns a list of functions with performance metrics. 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  report_exec.fcn_get_postgres_function_info   
               (   p_as_of_timestamp  =>  null  -- '2022-08-24 13:37:00.000'::timestamp 
               -- 
               ,   p_domain_code_name  =>  null  -- 'performance' 
               -- 
               )  
                  AS  X 
     --
     ORDER BY  X.line_number  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION report_exec.fcn_get_postgres_function_info ( p_as_of_timestamp timestamp DEFAULT null 
                                                           -- 
                                                           , p_domain_code_name text DEFAULT null ) 
-- 
RETURNS TABLE ( 
  line_number int 
--
, domain_code_name text 
, domain_display_name text 
, measured_timestamp timestamp 
--
, schema_name text 
, function_name text 
--
, calls bigint 
, self_seconds_per_call numeric(23,3)  
, total_seconds_per_call numeric(23,3)  
--
, calls_change_1_day bigint 
, calls_change_1_week bigint 
, calls_change_1_month bigint 
--
, mean_seconds_change_1_day numeric(23,3) 
, mean_seconds_change_1_week numeric(23,3) 
, mean_seconds_change_1_month numeric(23,3) 
--
) 
-- 
--
AS $main_def$          
        
        
    SELECT  ROW_NUMBER() OVER( ORDER BY  C.self_time  DESC 
                               ,         C.total_time  DESC 
                               ,         C.calls 
                               ,         C.display_order  )::int  AS  line_number 
    -- 
    ,  C.domain_code_name 
    ,  C.domain_display_name 
    ,  C.measured_timestamp 
    --
    ,  C.schemaname  AS  schema_name 
    ,  C.funcname  AS  function_name 
    --
    ,  C.calls 
    ,  ( X.self_time_per_call / 1000::real )::numeric(23,3)  AS  self_seconds_per_call 
    ,  ( X.total_time_per_call / 1000::real )::numeric(23,3)  AS  total_seconds_per_call 
    --
    ,  C.calls - D.calls  AS  calls_change_1_day 
    ,  C.calls - W.calls  AS  calls_change_1_week 
    ,  C.calls - M.calls  AS  calls_change_1_month 
    --
    ,  ( GREATEST ( X.self_time_per_call - X.self_time_per_call_back_1_day 
                  , X.total_time_per_call - X.total_time_per_call_back_1_day )::real 
       / 1000::real )::numeric(23,3)  AS  mean_seconds_change_1_day 
    ,  ( GREATEST ( X.self_time_per_call - X.self_time_per_call_back_1_week 
                  , X.total_time_per_call - X.total_time_per_call_back_1_week )::real 
       / 1000::real )::numeric(23,3)  AS  mean_seconds_change_1_week 
    ,  ( GREATEST ( X.self_time_per_call - X.self_time_per_call_back_1_month 
                  , X.total_time_per_call - X.total_time_per_call_back_1_month )::real 
       / 1000::real )::numeric(23,3)  AS  mean_seconds_change_1_month 
    --
    FROM  metric_exec.fcn_get_postgres_function_info ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) 
                                                     , p_domain_code_name 
                                                     , true )  AS  C 
    -- 
    LEFT  JOIN  metric_exec.fcn_get_postgres_function_info 
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 1 ) )::timestamp 
                  , p_domain_code_name 
                  , true )  
                  AS  D  ON  C.domain_code_name = D.domain_code_name 
                         AND C.schemaname = D.schemaname 
                         AND C.funcname = D.funcname 
                         AND D.measured_timestamp BETWEEN C.measured_timestamp - make_interval( hours => 32 ) 
                                                      AND C.measured_timestamp - make_interval( hours => 16 ) 
    LEFT  JOIN  metric_exec.fcn_get_postgres_function_info                                                  
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 7 ) )::timestamp 
                  , p_domain_code_name 
                  , true )  
                  AS  W  ON  C.domain_code_name = W.domain_code_name                               
                         AND C.schemaname = W.schemaname 
                         AND C.funcname = W.funcname          
                         AND W.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 9 ) 
                                                      AND C.measured_timestamp - make_interval( days => 5 ) 
    LEFT  JOIN  metric_exec.fcn_get_postgres_function_info                                        
                  ( ( coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 30 ) )::timestamp 
                  , p_domain_code_name 
                  , true )  
                  AS  M  ON  C.domain_code_name = M.domain_code_name 
                         AND C.schemaname = M.schemaname 
                         AND C.funcname = M.funcname 
                         AND M.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 33 ) 
                                                      AND C.measured_timestamp - make_interval( days => 27 ) 
    --
    LEFT JOIN LATERAL ( SELECT  CASE WHEN C.calls > 0 AND C.self_time >= 0.0::numeric(32,16)  
                                     THEN C.self_time::real / C.calls::real  
                                      ELSE null 
                                END  AS  self_time_per_call 
                        ,       CASE WHEN C.calls > 0 AND C.total_time >= 0.0::numeric(32,16)  
                                     THEN C.total_time::real / C.calls::real  
                                      ELSE null 
                                END  AS  total_time_per_call 
                        --
                        ,       CASE WHEN D.calls > 0 AND D.self_time >= 0.0::numeric(32,16)  
                                     THEN D.self_time::real / D.calls::real  
                                      ELSE null 
                                END  AS  self_time_per_call_back_1_day 
                        ,       CASE WHEN D.calls > 0 AND D.total_time >= 0.0::numeric(32,16)  
                                     THEN D.total_time::real / D.calls::real  
                                      ELSE null 
                                END  AS  total_time_per_call_back_1_day 
                        --
                        ,       CASE WHEN W.calls > 0 AND W.self_time >= 0.0::numeric(32,16)  
                                     THEN W.self_time::real / W.calls::real  
                                      ELSE null 
                                END  AS  self_time_per_call_back_1_week 
                        ,       CASE WHEN W.calls > 0 AND W.total_time >= 0.0::numeric(32,16)  
                                     THEN W.total_time::real / W.calls::real  
                                      ELSE null 
                                END  AS  total_time_per_call_back_1_week 
                        --
                        ,       CASE WHEN M.calls > 0 AND M.self_time >= 0.0::numeric(32,16)  
                                     THEN M.self_time::real / M.calls::real  
                                      ELSE null 
                                END  AS  self_time_per_call_back_1_month 
                        ,       CASE WHEN M.calls > 0 AND M.total_time >= 0.0::numeric(32,16)  
                                     THEN M.total_time::real / M.calls::real  
                                      ELSE null 
                                END  AS  total_time_per_call_back_1_month 
                        --
                      )  AS  X  ON  true 
    --
    WHERE  C.measured_timestamp > coalesce( p_as_of_timestamp , current_timestamp::timestamp ) - make_interval( days => 7 ) 
    --
    ;
            
            
$main_def$ LANGUAGE sql 
               STABLE 
               SECURITY DEFINER 
               SET search_path = utility, pg_temp;

--
--
--
--    

--
--

/*** SELECT postgres_query info ***/ 

/***********************************************
 
    FUNCTION: report_exec.fcn_get_postgres_query_info 
    
    PARAMETER(S):  p_as_of_timestamp    timestamp   OPTIONAL  
                   p_domain_code_name   text        OPTIONAL  
                       
    
    DESCRIPTION: 
    --  
    --  Returns a list of queries with performance metrics. 
    -- 
    
    
    EXAMPLE: 
    
    
     SELECT  X.* 
     --
     FROM  report_exec.fcn_get_postgres_query_info   
               (   p_as_of_timestamp  =>  null  -- '2022-08-24 13:37:00.000'::timestamp 
               -- 
               ,   p_domain_code_name  =>  null  -- 'performance' 
               -- 
               )  
                  AS  X 
     --
     ORDER BY  X.line_number  ASC 
     -- 
     ;     
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE FUNCTION report_exec.fcn_get_postgres_query_info ( p_as_of_timestamp timestamp DEFAULT null 
                                                        -- 
                                                        , p_domain_code_name text DEFAULT null ) 
-- 
RETURNS TABLE ( 
  line_number int 
--
, domain_code_name text 
, domain_display_name text 
, measured_timestamp timestamp 
--
, queryid bigint 
, query_text text 
--
, calls bigint 
, total_exec_hours numeric(22,2) 
, mean_seconds_per_call numeric(23,3) 
, max_seconds_per_call numeric(23,3)
, mean_rows_per_call numeric(21,1)
--
, calls_change_1_day bigint 
, calls_change_1_week bigint 
, calls_change_1_month bigint 
--
, mean_seconds_change_1_day numeric(23,3) 
, mean_seconds_change_1_week numeric(23,3) 
, mean_seconds_change_1_month numeric(23,3) 
--
, max_seconds_change_1_day numeric(23,3) 
, max_seconds_change_1_week numeric(23,3) 
, max_seconds_change_1_month numeric(23,3) 
--
, mean_rows_change_1_day numeric(21,1) 
, mean_rows_change_1_week numeric(21,1) 
, mean_rows_change_1_month numeric(21,1) 
--
) 
-- 
--
AS $main_def$          
        
        
    WITH cte_postgres_query 
    AS (
    
      SELECT  F.code_name  AS  set_code_name 
      -- 
      ,     X.domain_code_name 
      ,     X.domain_display_name 
      --    
      ,     X.measured_timestamp 
      --    
      ,     Y.queryid 
      ,     CASE WHEN Y.queryid IS NULL 
                 THEN 'ALL TRACKED DB ACTIVITY'
                 ELSE MAX( LEFT( X.query , 50 ) ) 
            END  AS  query_text 
    ---- ,  SUM( X.plans )                 AS  plans 
    ---- ,  SUM( X.total_plan_time )       AS  total_plan_time 
    ---- ,  MAX( X.max_plan_time )         AS  max_plan_time 
      ,     SUM( X.calls )                 AS  calls 
      ,     SUM( X.total_exec_time )       AS  total_exec_time 
      ,     MAX( X.max_exec_time )         AS  max_exec_time 
      ,     SUM( coalesce( X.rows , 0 ) )  AS  rows 
      -- 
      FROM  ( VALUES ( 'N' , current_timestamp::timestamp ) 
              --
              ,      ( 'D' , ( current_timestamp - make_interval( days =>  1 ) )::timestamp ) 
              ,      ( 'W' , ( current_timestamp - make_interval( days =>  7 ) )::timestamp ) 
              ,      ( 'M' , ( current_timestamp - make_interval( days => 30 ) )::timestamp ) 
              --
            )  AS  F ( code_name , upper_limit_measured_timestamp ) 
      -- 
      CROSS JOIN LATERAL metric_exec.fcn_get_postgres_query_info 
                  ( F.upper_limit_measured_timestamp , null )  AS  X 
      -- 
      CROSS JOIN LATERAL ( VALUES ( null::bigint ) 
                           ,      ( X.queryid::bigint ) )  AS  Y  ( queryid ) 
      -- 
      WHERE  X.toplevel = true 
      -- 
      GROUP BY  F.code_name 
      ,         X.domain_code_name 
      ,         X.domain_display_name 
      ,         X.measured_timestamp 
      --        
      ,         Y.queryid 
      --
      
    ) 
    
    SELECT  ROW_NUMBER() OVER( ORDER BY  CASE WHEN C.queryid IS NULL THEN 0 ELSE 1 END 
                               ,         C.total_exec_time  DESC  
                               ,         C.calls  DESC  
                               ,         C.max_exec_time  DESC 
                               ,         C.rows  DESC  
                               ,         C.domain_code_name 
                               ,         C.queryid  )::int  AS  line_number 
    -- 
    ,  C.domain_code_name 
    ,  C.domain_display_name 
    ,  C.measured_timestamp 
    --
    ,  C.queryid 
    ,  C.query_text 
    --
    ,  C.calls 
    ,  ( C.total_exec_time / (1000*60*60)::real )::numeric(22,2)  AS  total_exec_hours 
    ,  ( Z.mean_time_per_call / 1000::real )::numeric(23,3)  AS  mean_seconds_per_call 
    ,  ( C.max_exec_time / 1000::real )::numeric(23,3)  AS  max_seconds_per_call 
    ,  Z.mean_rows_per_call::numeric(21,1)  AS  mean_rows_per_call 
    --
    ,  C.calls - D.calls  AS  calls_change_1_day 
    ,  C.calls - W.calls  AS  calls_change_1_week 
    ,  C.calls - M.calls  AS  calls_change_1_month 
    --
    ,  ( ( Z.mean_time_per_call - Z.mean_time_per_call_back_1_day )::real 
       / 1000::real )::numeric(23,3)  AS  mean_seconds_change_1_day 
    ,  ( ( Z.mean_time_per_call - Z.mean_time_per_call_back_1_week )::real 
       / 1000::real )::numeric(23,3)  AS  mean_seconds_change_1_week 
    ,  ( ( Z.mean_time_per_call - Z.mean_time_per_call_back_1_month )::real 
       / 1000::real )::numeric(23,3)  AS  mean_seconds_change_1_month 
    --
    ,  ( ( C.max_exec_time - D.max_exec_time )::real 
       / 1000::real )::numeric(23,3)  AS  max_seconds_change_1_day 
    ,  ( ( C.max_exec_time - W.max_exec_time )::real 
       / 1000::real )::numeric(23,3)  AS  max_seconds_change_1_week 
    ,  ( ( C.max_exec_time - M.max_exec_time )::real 
       / 1000::real )::numeric(23,3)  AS  max_seconds_change_1_month 
    --
    ,  ( Z.mean_rows_per_call - Z.mean_rows_per_call_back_1_day )::numeric(21,1)  AS  mean_rows_change_1_day 
    ,  ( Z.mean_rows_per_call - Z.mean_rows_per_call_back_1_week )::numeric(21,1)  AS  mean_rows_change_1_week 
    ,  ( Z.mean_rows_per_call - Z.mean_rows_per_call_back_1_month )::numeric(21,1)  AS  mean_rows_change_1_month 
    --
    FROM  ( SELECT  C_s.* 
            -- 
            ,       ROW_NUMBER() OVER( PARTITION BY  C_s.domain_code_name 
                                       ORDER BY  C_s.total_exec_time  DESC 
                                       ,         C_s.max_exec_time    DESC 
                                       ,         C_s.calls            DESC 
                                       ,         C_s.queryid )  AS  rank_for_inclusion_cutoff  
            -- 
            FROM  cte_postgres_query  AS  C_s 
            -- 
            WHERE  C_s.set_code_name = 'N' 
            -- 
            AND  ( C_s.queryid IS NULL OR C_s.calls > 1 )  -- !! 
            -- 
          )  AS  C 
    -- 
    LEFT  JOIN  cte_postgres_query  AS  D  
                         ON  C.domain_code_name = D.domain_code_name 
                         AND D.set_code_name = 'D' 
                         AND C.queryid IS NOT DISTINCT FROM D.queryid 
                         AND D.measured_timestamp BETWEEN C.measured_timestamp - make_interval( hours => 32 ) 
                                                      AND C.measured_timestamp - make_interval( hours => 16 ) 
    LEFT  JOIN  cte_postgres_query  AS  W  
                         ON  C.domain_code_name = W.domain_code_name            
                         AND W.set_code_name = 'W'          
                         AND C.queryid IS NOT DISTINCT FROM W.queryid 
                         AND W.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 9 ) 
                                                      AND C.measured_timestamp - make_interval( days => 5 ) 
    LEFT  JOIN  cte_postgres_query  AS  M  
                         ON  C.domain_code_name = M.domain_code_name         
                         AND M.set_code_name = 'M'          
                         AND C.queryid IS NOT DISTINCT FROM M.queryid 
                         AND M.measured_timestamp BETWEEN C.measured_timestamp - make_interval( days => 33 ) 
                                                      AND C.measured_timestamp - make_interval( days => 27 ) 
    --
    LEFT JOIN LATERAL ( SELECT  CASE WHEN C.calls > 0 AND C.total_exec_time >= 0.0::numeric(32,16)  
                                     THEN C.total_exec_time::real / C.calls::real  
                                      ELSE null 
                                END  AS  mean_time_per_call 
                        ,       CASE WHEN C.calls > 0 AND C.rows >= 0 
                                     THEN C.rows::real / C.calls::real  
                                      ELSE null 
                                END  AS  mean_rows_per_call 
                        --
                        ,       CASE WHEN D.calls > 0 AND D.total_exec_time >= 0.0::numeric(32,16)  
                                     THEN D.total_exec_time::real / D.calls::real  
                                      ELSE null 
                                END  AS  mean_time_per_call_back_1_day 
                        ,       CASE WHEN D.calls > 0 AND D.rows >= 0 
                                     THEN D.rows::real / D.calls::real  
                                      ELSE null 
                                END  AS  mean_rows_per_call_back_1_day 
                        --
                        ,       CASE WHEN W.calls > 0 AND W.total_exec_time >= 0.0::numeric(32,16)  
                                     THEN W.total_exec_time::real / W.calls::real  
                                      ELSE null 
                                END  AS  mean_time_per_call_back_1_week 
                        ,       CASE WHEN W.calls > 0 AND W.rows >= 0 
                                     THEN W.rows::real / W.calls::real  
                                      ELSE null 
                                END  AS  mean_rows_per_call_back_1_week 
                        --
                        ,       CASE WHEN M.calls > 0 AND M.total_exec_time >= 0.0::numeric(32,16)  
                                     THEN M.total_exec_time::real / M.calls::real  
                                      ELSE null 
                                END  AS  mean_time_per_call_back_1_month 
                        ,       CASE WHEN M.calls > 0 AND M.rows >= 0 
                                     THEN M.rows::real / M.calls::real  
                                      ELSE null 
                                END  AS  mean_rows_per_call_back_1_month 
                        --
                      )  AS  Z  ON  true 
    --
    WHERE  C.measured_timestamp > current_timestamp - make_interval( days => 7 ) 
    --
    AND  C.rank_for_inclusion_cutoff <= 100  -- !! 
    --
    ;
            
            
$main_def$ LANGUAGE sql 
           STABLE 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--
--
--

--
--
--
--

--
--

  --
  -- create functions in "purge_exec" schema 
  -- 

--
--

/*** DELETE from metric.postgres_... tables - old "non-month-end" records ***/ 

/***********************************************
 
    PROCEDURE: purge_exec.prc_wipe_old_metric_records 
    
    PARAMETER(S):  none 
    
    
    DESCRIPTION: 
    -- 
    --  Deletes old records from the  "metric"  schema, across many tables: 
    -- 
    --    "postgres_database", "postgres_table", "postgres_index", 
    --     "postgres_function", "postgres_query". 
    -- 
    --    Deletions which occur during this routine 
    --     should not be logged in "..._history" tables. 
    -- 
    
    
    EXAMPLE: 
    
    
     CALL purge_exec.prc_wipe_old_metric_records(); 
    
    
    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE PROCEDURE purge_exec.prc_wipe_old_metric_records ( ) 
--
AS $main_def$ 
<<main_block>> 
DECLARE 
  --
  v_raise_message text := ''; 
  --
  v_row_count int; -- for internal use with: -- GET DIAGNOSTICS v_row_count = ROW_COUNT; 
  --
  --
  v_current_timestamp timestamp := current_timestamp; 
  --
  --
  v_maximum_request_time_received_to_delete timestamp := v_current_timestamp - make_interval( days => 93 ); -- ~ 3 months, as of 2023-02-07 
  --
  v_minimum_request_time_received_to_delete timestamp := v_current_timestamp - make_interval( days => 366 ); -- limit historical scope ( if not deleted already, preserve ) 
  --
  --
  v_deletion_batch_size bigint := (50 * 1000); 
  --
  v_running_row_count bigint; 
  --
  --
BEGIN 
    
    v_raise_message := utility.fcn_console_message('START :: purge_exec.prc_wipe_old_metric_records');  
    RAISE NOTICE '%' , v_raise_message; 
            
--
--

    CREATE TEMPORARY TABLE tt_postgres_database_to_delete ( 
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , postgres_database_pin bigint NOT NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_postgres_database_to_delete PRIMARY KEY ( tmp_pin ) 
    , CONSTRAINT tmp_uix_postgres_database_to_delete UNIQUE ( postgres_database_pin ) 
    -- 
    ) ON COMMIT DROP; 
    
    CREATE TEMPORARY TABLE tt_postgres_table_to_delete ( 
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , postgres_table_pin bigint NOT NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_postgres_table_to_delete PRIMARY KEY ( tmp_pin ) 
    , CONSTRAINT tmp_uix_postgres_table_to_delete UNIQUE ( postgres_table_pin ) 
    -- 
    ) ON COMMIT DROP; 
    
    CREATE TEMPORARY TABLE tt_postgres_index_to_delete ( 
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , postgres_index_pin bigint NOT NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_postgres_index_to_delete PRIMARY KEY ( tmp_pin ) 
    , CONSTRAINT tmp_uix_postgres_index_to_delete UNIQUE ( postgres_index_pin ) 
    -- 
    ) ON COMMIT DROP; 
    
    CREATE TEMPORARY TABLE tt_postgres_function_to_delete ( 
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , postgres_function_pin bigint NOT NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_postgres_function_to_delete PRIMARY KEY ( tmp_pin ) 
    , CONSTRAINT tmp_uix_postgres_function_to_delete UNIQUE ( postgres_function_pin ) 
    -- 
    ) ON COMMIT DROP; 
    
    CREATE TEMPORARY TABLE tt_postgres_query_to_delete ( 
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , postgres_query_pin bigint NOT NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_postgres_query_to_delete PRIMARY KEY ( tmp_pin ) 
    , CONSTRAINT tmp_uix_postgres_query_to_delete UNIQUE ( postgres_query_pin ) 
    -- 
    ) ON COMMIT DROP; 
    
--
--
    
--
--

--
--

    v_raise_message := utility.fcn_console_message('Collect "postgres_database" rows to delete.');
    RAISE NOTICE '%' , v_raise_message; 
      
      INSERT INTO tt_postgres_database_to_delete 
      (
        postgres_database_pin 
      ) 
      
        SELECT  Q.pin  AS  postgres_database_pin 
        -- 
        FROM    metric.postgres_database  AS  Q  
        --
        INNER JOIN  ( SELECT  Qx.domain_pin 
                      ,       EXTRACT(year FROM Qx.measured_timestamp)  AS  measured_year 
                      ,       EXTRACT(month FROM Qx.measured_timestamp)  AS  measured_month 
                      -- 
                      ,       MAX(Qx.measured_timestamp)  AS  max_measured_timestamp 
                      -- 
                      FROM   metric.postgres_database  AS  Qx 
                      WHERE  Qx.measured_timestamp <= v_maximum_request_time_received_to_delete 
                      AND    Qx.measured_timestamp > v_minimum_request_time_received_to_delete 
                      --
                      GROUP BY  Qx.domain_pin 
                      ,         EXTRACT(year FROM Qx.measured_timestamp) 
                      ,         EXTRACT(month FROM Qx.measured_timestamp) 
                      --
                    )  AS  X  ON  Q.domain_pin = X.domain_pin 
                              AND EXTRACT(year FROM Q.measured_timestamp) = X.measured_year 
                              AND EXTRACT(month FROM Q.measured_timestamp) = X.measured_month 
        --
        WHERE   Q.measured_timestamp <= v_maximum_request_time_received_to_delete 
        AND     Q.measured_timestamp > v_minimum_request_time_received_to_delete 
        --
        AND     Q.measured_timestamp < X.max_measured_timestamp  -- !! keep latest-in-month records for each domain and historical month !! 
        --
        ORDER BY  Q.pin  ASC 
        --
        ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
      
--
--

    v_raise_message := utility.fcn_console_message('Collect "postgres_table" rows to delete.');
    RAISE NOTICE '%' , v_raise_message; 
      
      INSERT INTO tt_postgres_table_to_delete 
      (
        postgres_table_pin 
      ) 
      
        SELECT  Q.pin  AS  postgres_table_pin 
        -- 
        FROM    metric.postgres_table  AS  Q  
        --
        INNER JOIN  ( SELECT  Qx.domain_pin 
                      ,       EXTRACT(year FROM Qx.measured_timestamp)  AS  measured_year 
                      ,       EXTRACT(month FROM Qx.measured_timestamp)  AS  measured_month 
                      -- 
                      ,       MAX(Qx.measured_timestamp)  AS  max_measured_timestamp 
                      -- 
                      FROM   metric.postgres_table  AS  Qx 
                      WHERE  Qx.measured_timestamp <= v_maximum_request_time_received_to_delete 
                      AND    Qx.measured_timestamp > v_minimum_request_time_received_to_delete 
                      --
                      GROUP BY  Qx.domain_pin 
                      ,         EXTRACT(year FROM Qx.measured_timestamp) 
                      ,         EXTRACT(month FROM Qx.measured_timestamp) 
                      --
                    )  AS  X  ON  Q.domain_pin = X.domain_pin 
                              AND EXTRACT(year FROM Q.measured_timestamp) = X.measured_year 
                              AND EXTRACT(month FROM Q.measured_timestamp) = X.measured_month 
        --
        WHERE   Q.measured_timestamp <= v_maximum_request_time_received_to_delete 
        AND     Q.measured_timestamp > v_minimum_request_time_received_to_delete 
        --
        AND     Q.measured_timestamp < X.max_measured_timestamp  -- !! keep latest-in-month records for each domain and historical month !! 
        --
        ORDER BY  Q.pin  ASC 
        --
        ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    v_raise_message := utility.fcn_console_message('Collect "postgres_index" rows to delete.');
    RAISE NOTICE '%' , v_raise_message; 
      
      INSERT INTO tt_postgres_index_to_delete 
      (
        postgres_index_pin 
      ) 
      
        SELECT  Q.pin  AS  postgres_index_pin 
        -- 
        FROM    metric.postgres_index  AS  Q  
        --
        INNER JOIN  ( SELECT  Qx.domain_pin 
                      ,       EXTRACT(year FROM Qx.measured_timestamp)  AS  measured_year 
                      ,       EXTRACT(month FROM Qx.measured_timestamp)  AS  measured_month 
                      -- 
                      ,       MAX(Qx.measured_timestamp)  AS  max_measured_timestamp 
                      -- 
                      FROM   metric.postgres_index  AS  Qx 
                      WHERE  Qx.measured_timestamp <= v_maximum_request_time_received_to_delete 
                      AND    Qx.measured_timestamp > v_minimum_request_time_received_to_delete 
                      --
                      GROUP BY  Qx.domain_pin 
                      ,         EXTRACT(year FROM Qx.measured_timestamp) 
                      ,         EXTRACT(month FROM Qx.measured_timestamp) 
                      --
                    )  AS  X  ON  Q.domain_pin = X.domain_pin 
                              AND EXTRACT(year FROM Q.measured_timestamp) = X.measured_year 
                              AND EXTRACT(month FROM Q.measured_timestamp) = X.measured_month 
        --
        WHERE   Q.measured_timestamp <= v_maximum_request_time_received_to_delete 
        AND     Q.measured_timestamp > v_minimum_request_time_received_to_delete 
        --
        AND     Q.measured_timestamp < X.max_measured_timestamp  -- !! keep latest-in-month records for each domain and historical month !! 
        --
        ORDER BY  Q.pin  ASC 
        --
        ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    v_raise_message := utility.fcn_console_message('Collect "postgres_function" rows to delete.');
    RAISE NOTICE '%' , v_raise_message; 
      
      INSERT INTO tt_postgres_function_to_delete 
      (
        postgres_function_pin 
      ) 
      
        SELECT  Q.pin  AS  postgres_function_pin 
        -- 
        FROM    metric.postgres_function  AS  Q  
        --
        INNER JOIN  ( SELECT  Qx.domain_pin 
                      ,       EXTRACT(year FROM Qx.measured_timestamp)  AS  measured_year 
                      ,       EXTRACT(month FROM Qx.measured_timestamp)  AS  measured_month 
                      -- 
                      ,       MAX(Qx.measured_timestamp)  AS  max_measured_timestamp 
                      -- 
                      FROM   metric.postgres_function  AS  Qx 
                      WHERE  Qx.measured_timestamp <= v_maximum_request_time_received_to_delete 
                      AND    Qx.measured_timestamp > v_minimum_request_time_received_to_delete 
                      --
                      GROUP BY  Qx.domain_pin 
                      ,         EXTRACT(year FROM Qx.measured_timestamp) 
                      ,         EXTRACT(month FROM Qx.measured_timestamp) 
                      --
                    )  AS  X  ON  Q.domain_pin = X.domain_pin 
                              AND EXTRACT(year FROM Q.measured_timestamp) = X.measured_year 
                              AND EXTRACT(month FROM Q.measured_timestamp) = X.measured_month 
        --
        WHERE   Q.measured_timestamp <= v_maximum_request_time_received_to_delete 
        AND     Q.measured_timestamp > v_minimum_request_time_received_to_delete 
        --
        AND     Q.measured_timestamp < X.max_measured_timestamp  -- !! keep latest-in-month records for each domain and historical month !! 
        --
        ORDER BY  Q.pin  ASC 
        --
        ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

    v_raise_message := utility.fcn_console_message('Collect "postgres_query" rows to delete.');
    RAISE NOTICE '%' , v_raise_message; 
      
      INSERT INTO tt_postgres_query_to_delete 
      (
        postgres_query_pin 
      ) 
      
        SELECT  Q.pin  AS  postgres_query_pin 
        -- 
        FROM    metric.postgres_query  AS  Q  
        --
        INNER JOIN  ( SELECT  Qx.domain_pin 
                      ,       EXTRACT(year FROM Qx.measured_timestamp)  AS  measured_year 
                      ,       EXTRACT(month FROM Qx.measured_timestamp)  AS  measured_month 
                      -- 
                      ,       MAX(Qx.measured_timestamp)  AS  max_measured_timestamp 
                      -- 
                      FROM   metric.postgres_query  AS  Qx 
                      WHERE  Qx.measured_timestamp <= v_maximum_request_time_received_to_delete 
                      AND    Qx.measured_timestamp > v_minimum_request_time_received_to_delete 
                      --
                      GROUP BY  Qx.domain_pin 
                      ,         EXTRACT(year FROM Qx.measured_timestamp) 
                      ,         EXTRACT(month FROM Qx.measured_timestamp) 
                      --
                    )  AS  X  ON  Q.domain_pin = X.domain_pin 
                              AND EXTRACT(year FROM Q.measured_timestamp) = X.measured_year 
                              AND EXTRACT(month FROM Q.measured_timestamp) = X.measured_month 
        --
        WHERE   Q.measured_timestamp <= v_maximum_request_time_received_to_delete 
        AND     Q.measured_timestamp > v_minimum_request_time_received_to_delete 
        --
        AND     Q.measured_timestamp < X.max_measured_timestamp  -- !! keep latest-in-month records for each domain and historical month !! 
        --
        ORDER BY  Q.pin  ASC 
        --
        ;
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
    
--
--

--
--
--
--

    SET session_replication_role = replica; 
    
--
--
--
--

--
--

    v_raise_message := utility.fcn_console_message('Delete "postgres_database" rows:');
    RAISE NOTICE '%' , v_raise_message; 
      
      
      WHILE ( EXISTS ( SELECT null 
                       FROM tt_postgres_database_to_delete AS T 
                       INNER JOIN metric.postgres_database AS D 
                         ON T.postgres_database_pin = D.pin ) ) 
      LOOP 
        
        DELETE FROM metric.postgres_database AS D 
        WHERE D.pin 
           IN ( SELECT Dx.pin  
                FROM tt_postgres_database_to_delete AS T 
                INNER JOIN metric.postgres_database AS Dx 
                  ON T.postgres_database_pin = Dx.pin 
                ORDER BY  Dx.pin  ASC 
                --
                LIMIT v_deletion_batch_size ) 
        --
        ;
      
        GET DIAGNOSTICS v_row_count = ROW_COUNT; 
        
        v_running_row_count := coalesce( v_running_row_count , 0 ) + v_row_count ;
    
      END LOOP; 

      v_row_count := coalesce( v_running_row_count , 0 ) ; 

    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
      
      v_running_row_count := null ; 
      
--
--

    v_raise_message := utility.fcn_console_message('Delete "postgres_table" rows:');
    RAISE NOTICE '%' , v_raise_message; 
      
      
      WHILE ( EXISTS ( SELECT null 
                       FROM tt_postgres_table_to_delete AS T 
                       INNER JOIN metric.postgres_table AS D 
                         ON T.postgres_table_pin = D.pin ) ) 
      LOOP 
        
        DELETE FROM metric.postgres_table AS D 
        WHERE D.pin 
           IN ( SELECT Dx.pin  
                FROM tt_postgres_table_to_delete AS T 
                INNER JOIN metric.postgres_table AS Dx 
                  ON T.postgres_table_pin = Dx.pin 
                ORDER BY  Dx.pin  ASC 
                --
                LIMIT v_deletion_batch_size ) 
        --
        ;
      
        GET DIAGNOSTICS v_row_count = ROW_COUNT; 
        
        v_running_row_count := coalesce( v_running_row_count , 0 ) + v_row_count ;
    
      END LOOP; 

      v_row_count := coalesce( v_running_row_count , 0 ) ; 

    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
      
      v_running_row_count := null ; 
      
--
--

    v_raise_message := utility.fcn_console_message('Delete "postgres_index" rows:');
    RAISE NOTICE '%' , v_raise_message; 
      
      
      WHILE ( EXISTS ( SELECT null 
                       FROM tt_postgres_index_to_delete AS T 
                       INNER JOIN metric.postgres_index AS D 
                         ON T.postgres_index_pin = D.pin ) ) 
      LOOP 
        
        DELETE FROM metric.postgres_index AS D 
        WHERE D.pin 
           IN ( SELECT Dx.pin  
                FROM tt_postgres_index_to_delete AS T 
                INNER JOIN metric.postgres_index AS Dx 
                  ON T.postgres_index_pin = Dx.pin 
                ORDER BY  Dx.pin  ASC 
                --
                LIMIT v_deletion_batch_size ) 
        --
        ;
      
        GET DIAGNOSTICS v_row_count = ROW_COUNT; 
        
        v_running_row_count := coalesce( v_running_row_count , 0 ) + v_row_count ;
    
      END LOOP; 

      v_row_count := coalesce( v_running_row_count , 0 ) ; 

    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
      
      v_running_row_count := null ; 
      
--
--

    v_raise_message := utility.fcn_console_message('Delete "postgres_function" rows:');
    RAISE NOTICE '%' , v_raise_message; 
      
      
      WHILE ( EXISTS ( SELECT null 
                       FROM tt_postgres_function_to_delete AS T 
                       INNER JOIN metric.postgres_function AS D 
                         ON T.postgres_function_pin = D.pin ) ) 
      LOOP 
        
        DELETE FROM metric.postgres_function AS D 
        WHERE D.pin 
           IN ( SELECT Dx.pin  
                FROM tt_postgres_function_to_delete AS T 
                INNER JOIN metric.postgres_function AS Dx 
                  ON T.postgres_function_pin = Dx.pin 
                ORDER BY  Dx.pin  ASC 
                --
                LIMIT v_deletion_batch_size ) 
        --
        ;
      
        GET DIAGNOSTICS v_row_count = ROW_COUNT; 
        
        v_running_row_count := coalesce( v_running_row_count , 0 ) + v_row_count ;
    
      END LOOP; 

      v_row_count := coalesce( v_running_row_count , 0 ) ; 

    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
      
      v_running_row_count := null ; 
      
--
--

    v_raise_message := utility.fcn_console_message('Delete "postgres_query" rows:');
    RAISE NOTICE '%' , v_raise_message; 
      
      
      WHILE ( EXISTS ( SELECT null 
                       FROM tt_postgres_query_to_delete AS T 
                       INNER JOIN metric.postgres_query AS D 
                         ON T.postgres_query_pin = D.pin ) ) 
      LOOP 
        
        DELETE FROM metric.postgres_query AS D 
        WHERE D.pin 
           IN ( SELECT Dx.pin  
                FROM tt_postgres_query_to_delete AS T 
                INNER JOIN metric.postgres_query AS Dx 
                  ON T.postgres_query_pin = Dx.pin 
                ORDER BY  Dx.pin  ASC 
                --
                LIMIT v_deletion_batch_size ) 
        --
        ;
      
        GET DIAGNOSTICS v_row_count = ROW_COUNT; 
        
        v_running_row_count := coalesce( v_running_row_count , 0 ) + v_row_count ;
    
      END LOOP; 

      v_row_count := coalesce( v_running_row_count , 0 ) ; 

    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message; 
      
      v_running_row_count := null ; 
      
--
--

--
--
--
--

    SET session_replication_role = DEFAULT; 
    
--
--
--
--

--
--
    
    DROP TABLE tt_postgres_database_to_delete ; 
    DROP TABLE tt_postgres_table_to_delete ; 
    DROP TABLE tt_postgres_index_to_delete ; 
    DROP TABLE tt_postgres_function_to_delete ; 
    DROP TABLE tt_postgres_query_to_delete ; 
    
--
--

    --
    --
    
    v_raise_message := utility.fcn_console_message('END :: purge_exec.prc_wipe_old_metric_records');  
    RAISE NOTICE '%' , v_raise_message; 
    
-- 
-- 
END main_block; 
$main_def$ LANGUAGE plpgsql 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--

/*****/

COMMIT; 

/*****/
