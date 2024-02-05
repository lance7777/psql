-- \c performance 
--

/*****/ 

BEGIN; 

/*****/ 


-- # # # # # # # # # # # # 
SET ROLE performance_dbo; 
-- # # # # # # # # # # # # 


/*  Content Summary: 


  SCHEMA metric  ( & metric_history ) 


  TABLE metric.domain_class 
  VIEW metric.vw_domain_class 
  TABLE metric.domain 
  VIEW metric.vw_domain 

  TABLE metric.postgres_database 
  VIEW metric.vw_postgres_database 
  TABLE metric.postgres_table 
  VIEW metric.vw_postgres_table 
  TABLE metric.postgres_index 
  VIEW metric.vw_postgres_index 
  TABLE metric.postgres_function 
  VIEW metric.vw_postgres_function 
  TABLE metric.postgres_query 
  VIEW metric.vw_postgres_query 


*/


--
-- DROP SCHEMA IF EXISTS metric, metric_history CASCADE; 
--


--
--

CREATE SCHEMA metric;
CREATE SCHEMA metric_history;

--
--


GRANT USAGE ON SCHEMA metric TO performance_reader_metric;
GRANT USAGE ON SCHEMA metric TO performance_writer_metric;

ALTER DEFAULT PRIVILEGES IN SCHEMA performance GRANT SELECT ON TABLES TO performance_reader_metric; 
ALTER DEFAULT PRIVILEGES IN SCHEMA performance GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO performance_writer_metric; 


--
--
 
  --
  -- create tables in "metric" schema 
  -- 
 
--
--

/*** domain_class ***/ 

CREATE TABLE metric.domain_class (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, code_name text NOT NULL 
, display_name text NOT NULL 
--
, short_description text NOT NULL 
, technical_note text NULL 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_domain_class PRIMARY KEY ( pin ) 
, CONSTRAINT uix_domain_class_code_name UNIQUE ( code_name ) 
--
, CONSTRAINT excl_domain_class_code_name_case_variation EXCLUDE ( lower(code_name) WITH = ) 
--
--
);

CREATE INDEX ix_domain_class_display_name ON metric.domain_class ( display_name ); 

/* !! */ CALL utility.prc_generate_history_table( 'metric' , 'domain_class' ); 

CREATE VIEW metric.vw_domain_class AS 

SELECT  X.pin  AS  domain_class_pin 
--
,       X.code_name 
,       X.display_name 
--
,       X.short_description 
-- 
FROM    metric.domain_class  AS  X    
-- 
;  

--
--

/***********************************************************************/

INSERT INTO metric.domain_class 
( code_name , display_name , short_description , technical_note ) 

VALUES 
   ( 'pgdb' 
   , 'PostgreSQL Database' 
   , 'A single database within a PostgreSQL cluster/server-instance.' 
   , 'Information is stored in the "metric.postgres_..." tables.' ) 
--
; 

/***********************************************************************/

--
--

/*** domain ***/ 

CREATE TABLE metric.domain (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, domain_class_pin bigint NOT NULL 
--
, code_name text NOT NULL 
, display_name text NOT NULL 
--
, short_description text NOT NULL 
, technical_note text NULL 
--
, is_active boolean NOT NULL DEFAULT false 
--
, display_order_rank int NULL 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_domain PRIMARY KEY ( pin ) 
, CONSTRAINT uix_domain_code_name UNIQUE ( code_name ) 
, CONSTRAINT uix_domain_display_name UNIQUE ( display_name ) 
--
, CONSTRAINT fk_domain_domain_class_pin FOREIGN KEY ( domain_class_pin ) REFERENCES metric.domain_class(pin) 
--
, CONSTRAINT excl_domain_code_name_case_variation EXCLUDE ( lower(code_name) WITH = ) 
, CONSTRAINT excl_domain_display_name_case_variation EXCLUDE ( lower(display_name) WITH = ) 
--
--
);

CREATE INDEX ix_domain_domain_class_pin ON metric.domain ( domain_class_pin ); 
CREATE INDEX ix_domain_is_active ON metric.domain ( is_active ); 
--
CREATE INDEX ix_domain_display_order_rank ON metric.domain ( display_order_rank ); 

/* !! */ CALL utility.prc_generate_history_table( 'metric' , 'domain' ); 

CREATE VIEW metric.vw_domain AS 

SELECT  X.pin  AS  domain_pin 
--
,       X.domain_class_pin 
,       C.code_name  AS  domain_class_code_name 
,       C.display_name  AS  domain_class_display_name 
--
,       X.code_name 
,       X.display_name 
--
,       X.is_active 
--
,       X.short_description 
--
,       X.display_order_rank 
--
FROM  metric.domain  AS  X 
-- 
INNER JOIN  metric.domain_class  AS  C  ON  X.domain_class_pin = C.pin 
--
;  

--
--

/***********************************************************************

INSERT INTO metric.domain 
( 
  domain_class_pin 
--
, code_name 
, display_name 
--
, short_description 
--
, is_active
--
)

SELECT  C.pin  AS  domain_class_pin 
--
, X.domain_code_name     AS  code_name 
, X.domain_display_name  AS  display_name 
--
, X.short_description 
--
, X.is_active
--
FROM ( 
VALUES 
   ( 'pgdb' 
   , 'example' 
   , 'Example DB' 
   , 'A simple database to demonstrate script style' 
   , true 
   ) 
--
,  ( 'pgdb' 
   , 'performance' 
   , 'Performance DB' 
   , 'A database to store technical statistics measured from infrastructure components' 
   , true 
   ) 
--
) AS X ( domain_class_code_name 
       , domain_code_name 
       , domain_display_name 
       , short_description 
       , is_active ) 
-- 
JOIN  metric.domain_class  AS  C  ON  X.domain_class_code_name = C.code_name 	   
--
; 

***********************************************************************/

--
--

--
--

--
--

/*** postgres_database ***/ 

  /******* 
  
    SELECT  X.* 
    --
    ,   pg_catalog.pg_database_size( X.datid )  AS  database_size 
    --
    ,   pg_size_pretty( pg_catalog.pg_database_size( X.datid ) )  AS  database_size_humanized 
    --
    FROM  pg_catalog.pg_stat_database  AS  X 
    --
    WHERE  X.datname = 'postgres' 
    --
    LIMIT 1 ; 

  *******/ 
  
CREATE TABLE metric.postgres_database (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, domain_pin bigint NOT NULL 
, measured_timestamp timestamp NOT NULL -- column descriptions from: https://www.postgresql.org/docs/current/monitoring-stats.html (as of: 2022-06-30)
--
-- from "pg_stat_database": 
-- 
, numbackends int NULL -- "Number of backends currently connected to this database, or NULL for shared objects. This is the only column in this view that returns a value reflecting current state; all other columns return the accumulated values since the last reset." 
, xact_commit bigint NULL -- "Number of transactions in this database that have been committed" 
, xact_rollback bigint NULL -- "Number of transactions in this database that have been rolled back" 
, blks_read bigint NULL -- "Number of disk blocks read in this database" 
, blks_hit bigint NULL -- "Number of times disk blocks were found already in the buffer cache, so that a read was not necessary (this only includes hits in the PostgreSQL buffer cache, not the operating system's file system cache)" 
, tup_returned bigint NULL -- "Number of rows returned by queries in this database" 
, tup_fetched bigint NULL -- "Number of rows fetched by queries in this database" 
, tup_inserted bigint NULL -- "Number of rows inserted by queries in this database" 
, tup_updated bigint NULL -- "Number of rows updated by queries in this database" 
, tup_deleted bigint NULL -- "Number of rows deleted by queries in this database" 
, conflicts bigint NULL -- "Number of queries canceled due to conflicts with recovery in this database. (Conflicts occur only on standby servers; see pg_stat_database_conflicts for details.)" 
, temp_files bigint NULL -- "Number of temporary files created by queries in this database. All temporary files are counted, regardless of why the temporary file was created (e.g., sorting or hashing), and regardless of the log_temp_files setting." 
, temp_bytes bigint NULL -- "Total amount of data written to temporary files by queries in this database. All temporary files are counted, regardless of why the temporary file was created, and regardless of the log_temp_files setting." 
, deadlocks bigint NULL -- "Number of deadlocks detected in this database" 
, checksum_failures bigint NULL -- "Number of data page checksum failures detected in this database (or on a shared object), or NULL if data checksums are not enabled." 
, checksum_last_failure timestamp NULL -- "Time at which the last data page checksum failure was detected in this database (or on a shared object), or NULL if data checksums are not enabled." 
, blk_read_time numeric(32,16) NULL -- "Time spent reading data file blocks by backends in this database, in milliseconds (if track_io_timing is enabled, otherwise zero)" 
, blk_write_time numeric(32,16) NULL -- "Time spent writing data file blocks by backends in this database, in milliseconds (if track_io_timing is enabled, otherwise zero)" 
, session_time numeric(32,16) NULL -- "Time spent by database sessions in this database, in milliseconds (note that statistics are only updated when the state of a session changes, so if sessions have been idle for a long time, this idle time won't be included)" 
, active_time numeric(32,16) NULL -- "Time spent executing SQL statements in this database, in milliseconds (this corresponds to the states active and fastpath function call in pg_stat_activity)" 
, idle_in_transaction_time numeric(32,16) NULL -- "Time spent idling while in a transaction in this database, in milliseconds (this corresponds to the states idle in transaction and idle in transaction (aborted) in pg_stat_activity)" 
, sessions bigint NULL -- "Total number of sessions established to this database" 
, sessions_abandoned bigint NULL -- "Number of database sessions to this database that were terminated because connection to the client was lost" 
, sessions_fatal bigint NULL -- "Number of database sessions to this database that were terminated by fatal errors" 
, sessions_killed bigint NULL -- "Number of database sessions to this database that were terminated by operator intervention" 
, stats_reset timestamp NULL -- "Time at which these statistics were last reset" 
--
-- from function: "pg_database_size": with description from: https://www.postgresql.org/docs/current/functions-admin.html (as of: 2022-06-30)
-- 
, database_size bigint NULL -- "Computes the total disk space used by the database with the specified name or OID. To use this function, you must have CONNECT privilege on the specified database (which is granted by default) or be a member of the pg_read_all_stats role." 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_postgres_database PRIMARY KEY ( pin ) 
, CONSTRAINT uix_postgres_database UNIQUE ( domain_pin , measured_timestamp ) 
--
--
);

/* !! */ CALL utility.prc_generate_history_table( 'metric' , 'postgres_database' ); 

CREATE VIEW metric.vw_postgres_database AS 

SELECT  X.pin  AS  postgres_database_pin 
--
,       D.domain_class_pin 
,       C.code_name  AS  domain_class_code_name 
,       C.display_name  AS  domain_class_display_name 
--
,       D.code_name  AS  domain_code_name 
,       D.display_name  AS  domain_display_name 
--
,       X.measured_timestamp 
-- 
,       X.numbackends 
,       X.xact_commit 
,       X.xact_rollback 
,       X.blks_read 
,       X.blks_hit 
,       X.tup_returned 
,       X.tup_fetched 
,       X.tup_inserted 
,       X.tup_updated 
,       X.tup_deleted 
,       X.conflicts 
,       X.temp_files 
,       X.temp_bytes 
,       X.deadlocks 
,       X.checksum_failures 
,       X.checksum_last_failure 
,       X.blk_read_time 
,       X.blk_write_time 
,       X.session_time 
,       X.active_time 
,       X.idle_in_transaction_time 
,       X.sessions 
,       X.sessions_abandoned 
,       X.sessions_fatal 
,       X.sessions_killed 
,       X.stats_reset 
--
,       X.database_size 
--
FROM  metric.postgres_database  AS  X 
--
INNER JOIN  metric.domain  AS  D  ON  X.domain_pin = D.pin  
-- 
INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
--
;  

--
--

/*** postgres_table ***/ 

  /******* 
  
    SELECT  X.* 
    --
    ,   Y.heap_blks_read 
    ,   Y.heap_blks_hit 
    ,   Y.idx_blks_read 
    ,   Y.idx_blks_hit 
    ,   Y.toast_blks_read 
    ,   Y.toast_blks_hit 
    ,   Y.tidx_blks_read 
    ,   Y.tidx_blks_hit 
    --
    ,   pg_catalog.pg_total_relation_size( X.relid )  AS  total_relation_size 
    ,   pg_catalog.pg_table_size( X.relid )           AS  table_size 
    ,   pg_catalog.pg_indexes_size( X.relid )         AS  indexes_size 
    --
    FROM        pg_catalog.pg_stat_all_tables    AS  X 
    LEFT  JOIN  pg_catalog.pg_statio_all_tables  AS  Y  ON  X.schemaname = Y.schemaname 
                                                        AND X.relname = Y.relname 
    --
    WHERE  X.schemaname = 'pg_catalog' 
    AND    X.relname = 'pg_database' 
    --
    ORDER BY  X.schemaname 
    ,         X.relname 
    --
    ; 

  *******/ 

CREATE TABLE metric.postgres_table (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, domain_pin bigint NOT NULL 
, measured_timestamp timestamp NOT NULL -- column descriptions from: https://www.postgresql.org/docs/current/monitoring-stats.html (as of: 2022-06-30)
--
-- from "pg_stat_all_tables": 
-- 
, schemaname text NOT NULL -- "Name of the schema that this table is in" 
, relname text NOT NULL -- "Name of this table" 
, seq_scan bigint NULL -- "Number of sequential scans initiated on this table" 
, seq_tup_read bigint NULL -- "Number of live rows fetched by sequential scans" 
, idx_scan bigint NULL -- "Number of index scans initiated on this table" 
, idx_tup_fetch bigint NULL -- "Number of live rows fetched by index scans" 
, n_tup_ins bigint NULL -- "Number of rows inserted" 
, n_tup_upd bigint NULL -- "Number of rows updated (includes HOT updated rows)" 
, n_tup_del bigint NULL -- "Number of rows deleted" 
, n_tup_hot_upd bigint NULL -- "Number of rows HOT updated (i.e., with no separate index update required)" 
, n_live_tup bigint NULL -- "Estimated number of live rows" 
, n_dead_tup bigint NULL -- "Estimated number of dead rows" 
, n_mod_since_analyze bigint NULL -- "Estimated number of rows modified since this table was last analyzed" 
, n_ins_since_vacuum bigint NULL -- "Estimated number of rows inserted since this table was last vacuumed" 
, last_vacuum timestamp NULL -- "Last time at which this table was manually vacuumed (not counting VACUUM FULL)" 
, last_autovacuum timestamp NULL -- "Last time at which this table was vacuumed by the autovacuum daemon" 
, last_analyze timestamp NULL -- "Last time at which this table was manually analyzed" 
, last_autoanalyze timestamp NULL -- "Last time at which this table was analyzed by the autovacuum daemon" 
, vacuum_count bigint NULL -- "Number of times this table has been manually vacuumed (not counting VACUUM FULL)" 
, autovacuum_count bigint NULL -- "Number of times this table has been vacuumed by the autovacuum daemon" 
, analyze_count bigint NULL -- "Number of times this table has been manually analyzed" 
, autoanalyze_count bigint NULL -- "Number of times this table has been analyzed by the autovacuum daemon" 
--
-- from "pg_statio_all_tables": 
-- 
, heap_blks_read bigint NULL -- "Number of disk blocks read from this table" 
, heap_blks_hit bigint NULL -- "Number of buffer hits in this table" 
, idx_blks_read bigint NULL -- "Number of disk blocks read from all indexes on this table" 
, idx_blks_hit bigint NULL -- "Number of buffer hits in all indexes on this table" 
, toast_blks_read bigint NULL -- "Number of disk blocks read from this table's TOAST table (if any)" 
, toast_blks_hit bigint NULL -- "Number of buffer hits in this table's TOAST table (if any)" 
, tidx_blks_read bigint NULL -- "Number of disk blocks read from this table's TOAST table indexes (if any)" 
, tidx_blks_hit bigint NULL -- "Number of buffer hits in this table's TOAST table indexes (if any)" 
--
-- from functions: "pg_total_relation_size", "pg_table_size", and "pg_indexes_size": with descriptions from: https://www.postgresql.org/docs/current/functions-admin.html (as of: 2022-06-30)
-- 
, total_relation_size bigint NULL -- "Computes the total disk space used by the specified table, including all indexes and TOAST data. The result is equivalent to pg_table_size + pg_indexes_size." 
, table_size bigint NULL -- "Computes the disk space used by the specified table, excluding indexes (but including its TOAST table if any, free space map, and visibility map)." 
, indexes_size bigint NULL -- "Computes the total disk space used by indexes attached to the specified table." 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_postgres_table PRIMARY KEY ( pin ) 
, CONSTRAINT uix_postgres_table UNIQUE ( domain_pin , measured_timestamp , schemaname , relname ) 
--
--
);

/* !! */ CALL utility.prc_generate_history_table( 'metric' , 'postgres_table' ); 

CREATE VIEW metric.vw_postgres_table AS 

SELECT  X.pin  AS  postgres_table_pin 
--
,       D.domain_class_pin 
,       C.code_name  AS  domain_class_code_name 
,       C.display_name  AS  domain_class_display_name 
--
,       D.code_name  AS  domain_code_name 
,       D.display_name  AS  domain_display_name 
--
,       X.measured_timestamp 
-- 
,       X.schemaname 
,       X.relname 
,       X.seq_scan 
,       X.seq_tup_read 
,       X.idx_scan 
,       X.idx_tup_fetch 
,       X.n_tup_ins 
,       X.n_tup_upd 
,       X.n_tup_del 
,       X.n_tup_hot_upd 
,       X.n_live_tup 
,       X.n_dead_tup 
,       X.n_mod_since_analyze 
,       X.n_ins_since_vacuum 
,       X.last_vacuum 
,       X.last_autovacuum 
,       X.last_analyze 
,       X.last_autoanalyze 
,       X.vacuum_count 
,       X.autovacuum_count 
,       X.analyze_count 
,       X.autoanalyze_count 
--
,       X.heap_blks_read 
,       X.heap_blks_hit 
,       X.idx_blks_read 
,       X.idx_blks_hit 
,       X.toast_blks_read 
,       X.toast_blks_hit 
,       X.tidx_blks_read 
,       X.tidx_blks_hit 
-- 
,       X.total_relation_size 
,       X.table_size 
,       X.indexes_size 
--
FROM  metric.postgres_table  AS  X 
--
INNER JOIN  metric.domain  AS  D  ON  X.domain_pin = D.pin  
-- 
INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
--
;  

--
--

/*** postgres_index ***/ 

  /******* 
  
    SELECT  X.* 
    --
    ,   Y.idx_blks_read 
    ,   Y.idx_blks_hit 
    --
    ,   pg_catalog.pg_total_relation_size( concat( quote_ident( X.schemaname ) 
                                                 , '.'          
                                                 , quote_ident( X.indexrelname ) ) )  AS  index_size 
    --
    FROM        pg_catalog.pg_stat_all_indexes    AS  X 
    LEFT  JOIN  pg_catalog.pg_statio_all_indexes  AS  Y  ON  X.schemaname = Y.schemaname 
                                                         AND X.relname = Y.relname 
                                                         AND X.indexrelname = Y.indexrelname 
    --
    WHERE  X.schemaname = 'pg_catalog' 
    AND    X.relname = 'pg_database' 
    AND    X.indexrelname = 'pg_database_datname_index' 
    --
    ORDER BY  X.schemaname 
    ,         X.relname 
    ,         X.indexrelname 
    --
    ; 

  *******/ 

CREATE TABLE metric.postgres_index (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, domain_pin bigint NOT NULL 
, measured_timestamp timestamp NOT NULL -- column descriptions from: https://www.postgresql.org/docs/current/monitoring-stats.html (as of: 2022-06-30)
--
-- from "pg_stat_all_indexes": 
-- 
, schemaname text NOT NULL -- "Name of the schema this index is in" 
, relname text NOT NULL -- "Name of the table for this index" 
, indexrelname text NOT NULL -- "Name of this index" 
, idx_scan bigint NULL -- "Number of index scans initiated on this index" 
, idx_tup_read bigint NULL -- "Number of index entries returned by scans on this index" 
, idx_tup_fetch bigint NULL -- "Number of live table rows fetched by simple index scans using this index" 
--
-- from "pg_statio_all_indexes": 
-- 
, idx_blks_read bigint NULL -- "Number of disk blocks read from this index" 
, idx_blks_hit bigint NULL -- "Number of buffer hits in this index" 
--
-- from function: "pg_total_relation_size": with (edited & shortened) description from: https://www.postgresql.org/docs/current/functions-admin.html (as of: 2022-06-30) 
-- 
, total_relation_size bigint NULL -- "Computes the total disk space used by the specified table [OR INDEX!]..." 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_postgres_index PRIMARY KEY ( pin ) 
, CONSTRAINT uix_postgres_index UNIQUE ( domain_pin , measured_timestamp , schemaname , relname , indexrelname ) 
--
--
);

/* !! */ CALL utility.prc_generate_history_table( 'metric' , 'postgres_index' ); 

CREATE VIEW metric.vw_postgres_index AS 

SELECT  X.pin  AS  postgres_index_pin 
--
,       D.domain_class_pin 
,       C.code_name  AS  domain_class_code_name 
,       C.display_name  AS  domain_class_display_name 
--
,       D.code_name  AS  domain_code_name 
,       D.display_name  AS  domain_display_name 
--
,       X.measured_timestamp 
-- 
,       X.schemaname 
,       X.relname 
,       X.indexrelname 
,       X.idx_scan 
,       X.idx_tup_read 
,       X.idx_tup_fetch 
--
,       X.idx_blks_read 
,       X.idx_blks_hit 
--
,       X.total_relation_size  
--
FROM  metric.postgres_index  AS  X 
--
INNER JOIN  metric.domain  AS  D  ON  X.domain_pin = D.pin  
-- 
INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
--
;  

--
--

/*** postgres_function ***/ 

  /******* 
  
    SELECT  X.* 
    --
    FROM  pg_catalog.pg_stat_user_functions  AS  X 
    --
    ORDER BY  X.schemaname 
    ,         X.funcname 
    --
    LIMIT 1 ; 

  *******/ 

CREATE TABLE metric.postgres_function (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, domain_pin bigint NOT NULL 
, measured_timestamp timestamp NOT NULL -- column descriptions from: https://www.postgresql.org/docs/current/monitoring-stats.html (as of: 2022-06-30)
--
-- from "pg_stat_user_functions": 
-- 
, schemaname text NOT NULL -- "Name of the schema this function is in" 
, funcname text NOT NULL -- "Name of this function" 
, calls bigint NULL -- "Number of times this function has been called" 
, total_time numeric(32,16) NULL -- "Total time spent in this function and all other functions called by it, in milliseconds" 
, self_time numeric(32,16) NULL -- "Total time spent in this function itself, not including other functions called by it, in milliseconds" 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_postgres_function PRIMARY KEY ( pin ) 
, CONSTRAINT uix_postgres_function UNIQUE ( domain_pin , measured_timestamp , schemaname , funcname ) 
--
--
);

/* !! */ CALL utility.prc_generate_history_table( 'metric' , 'postgres_function' ); 

CREATE VIEW metric.vw_postgres_function AS 

SELECT  X.pin  AS  postgres_function_pin 
--
,       D.domain_class_pin 
,       C.code_name  AS  domain_class_code_name 
,       C.display_name  AS  domain_class_display_name 
--
,       D.code_name  AS  domain_code_name 
,       D.display_name  AS  domain_display_name 
--
,       X.measured_timestamp 
-- 
,       X.schemaname 
,       X.funcname 
,       X.calls 
,       X.total_time 
,       X.self_time 
--
FROM  metric.postgres_function  AS  X 
--
INNER JOIN  metric.domain  AS  D  ON  X.domain_pin = D.pin  
-- 
INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
--
;  

--
--

/*** postgres_query ***/ 

  /******* 
  
   \c postgres 
  
  --
  --
  
    SELECT  D.datname 
    --
    ,   X.* 
    --
    ,   R.rolname 
    --
    FROM  monitor.pg_stat_statements  AS  X 
-- !! a view based on "pg_stat_statements", regularizing columns across PostgreSQL versions 
    LEFT  JOIN  pg_catalog.pg_roles     AS  R  ON  X.userid = R.oid 
    LEFT  JOIN  pg_catalog.pg_database  AS  D  ON  X.dbid = D.oid 
    --
    WHERE  D.datname = 'postgres' 
    --
    ORDER BY  X.queryid  DESC 
    --
    LIMIT 1 ; 

  --
  --
  
   \c performance  
   
  *******/ 

CREATE TABLE metric.postgres_query (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, domain_pin bigint NOT NULL 
, measured_timestamp timestamp NOT NULL -- column descriptions from: https://www.postgresql.org/docs/current/pgstatstatements.html (as of: 2022-06-30)
--
-- from "pg_stat_statements": 
-- 
, toplevel bool NOT NULL -- "True if the query was executed as a top-level statement (always true if pg_stat_statements.track is set to top)" 
, queryid bigint NOT NULL -- "Hash code to identify identical normalized queries." 
, query text NULL -- "Text of a representative statement" 
, plans bigint NULL -- "Number of times the statement was planned (if pg_stat_statements.track_planning is enabled, otherwise zero)"
, total_plan_time numeric(32,16) NULL -- "Total time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)" 
, min_plan_time numeric(32,16) NULL -- "Minimum time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)" 
, max_plan_time numeric(32,16) NULL -- "Maximum time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)" 
, mean_plan_time numeric(32,16) NULL -- "Mean time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)" 
, stddev_plan_time numeric(32,16) NULL -- "Population standard deviation of time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)" 
, calls bigint NULL -- "Number of times the statement was executed" 
, total_exec_time numeric(32,16) NULL -- "Total time spent executing the statement, in milliseconds" 
, min_exec_time numeric(32,16) NULL -- "Minimum time spent executing the statement, in milliseconds" 
, max_exec_time numeric(32,16) NULL -- "Maximum time spent executing the statement, in milliseconds" 
, mean_exec_time numeric(32,16) NULL -- "Mean time spent executing the statement, in milliseconds" 
, stddev_exec_time numeric(32,16) NULL -- "Population standard deviation of time spent executing the statement, in milliseconds" 
, rows bigint NULL -- "Total number of rows retrieved or affected by the statement" 
, shared_blks_hit bigint NULL -- "Total number of shared block cache hits by the statement" 
, shared_blks_read bigint NULL -- "Total number of shared blocks read by the statement" 
, shared_blks_dirtied bigint NULL -- "Total number of shared blocks dirtied by the statement" 
, shared_blks_written bigint NULL -- "Total number of shared blocks written by the statement" 
, local_blks_hit bigint NULL -- "Total number of local block cache hits by the statement" 
, local_blks_read bigint NULL -- "Total number of local blocks read by the statement" 
, local_blks_dirtied bigint NULL -- "Total number of local blocks dirtied by the statement" 
, local_blks_written bigint NULL -- "Total number of local blocks written by the statement" 
, temp_blks_read bigint NULL -- "Total number of temp blocks read by the statement" 
, temp_blks_written bigint NULL -- "Total number of temp blocks written by the statement" 
, blk_read_time numeric(32,16) NULL -- "Total time the statement spent reading blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)" 
, blk_write_time numeric(32,16) NULL -- "Total time the statement spent writing blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)" 
, wal_records bigint NULL -- "Total number of WAL records generated by the statement" 
, wal_fpi bigint NULL -- "Total number of WAL full page images generated by the statement" 
, wal_bytes numeric(32,16) NULL -- "Total amount of WAL generated by the statement in bytes" 
--
-- from view: "pg_roles", with descriptions from: https://www.postgresql.org/docs/current/view-pg-roles.html (as of: 2022-06-30)
-- 
, rolname text NOT NULL -- "Role name" 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_postgres_query PRIMARY KEY ( pin ) 
, CONSTRAINT uix_postgres_query UNIQUE ( domain_pin , measured_timestamp , queryid , toplevel , rolname ) 
--
--
);

/* !! */ CALL utility.prc_generate_history_table( 'metric' , 'postgres_query' ); 

CREATE VIEW metric.vw_postgres_query AS 

SELECT  X.pin  AS  postgres_query_pin 
--
,       D.domain_class_pin 
,       C.code_name  AS  domain_class_code_name 
,       C.display_name  AS  domain_class_display_name 
--
,       D.code_name  AS  domain_code_name 
,       D.display_name  AS  domain_display_name 
--
,       X.measured_timestamp 
-- 
,       X.toplevel 
,       X.queryid 
,       X.query 
,       X.plans 
,       X.total_plan_time 
,       X.min_plan_time 
,       X.max_plan_time 
,       X.mean_plan_time 
,       X.stddev_plan_time 
,       X.calls 
,       X.total_exec_time 
,       X.min_exec_time 
,       X.max_exec_time 
,       X.mean_exec_time 
,       X.stddev_exec_time 
,       X.rows 
,       X.shared_blks_hit 
,       X.shared_blks_read 
,       X.shared_blks_dirtied 
,       X.shared_blks_written 
,       X.local_blks_hit 
,       X.local_blks_read 
,       X.local_blks_dirtied 
,       X.local_blks_written 
,       X.temp_blks_read 
,       X.temp_blks_written 
,       X.blk_read_time 
,       X.blk_write_time 
,       X.wal_records 
,       X.wal_fpi 
,       X.wal_bytes 
--
,       X.rolname
--
FROM  metric.postgres_query  AS  X 
--
INNER JOIN  metric.domain  AS  D  ON  X.domain_pin = D.pin  
-- 
INNER JOIN  metric.domain_class  AS  C  ON  D.domain_class_pin = C.pin 
--
;  

--
--

--
--

/*****/

COMMIT; 

/*****/
