-- \c postgres <<superuser_server_admin>> 
--

CREATE ROLE performance_dbo; 

GRANT performance_dbo TO <<superuser_server_admin>>; 

CREATE DATABASE performance OWNER performance_dbo; 
REVOKE ALL ON DATABASE performance FROM PUBLIC; 

GRANT CREATE ON DATABASE performance TO performance_dbo; 


CREATE ROLE performance_basic_reader WITH LOGIN ENCRYPTED PASSWORD 'fake_password_1'; 
CREATE ROLE performance_data_staff WITH LOGIN ENCRYPTED PASSWORD 'fake_password_2'; 
CREATE ROLE performance_backend_service WITH LOGIN ENCRYPTED PASSWORD 'fake_password_3'; 
--
CREATE ROLE performance_purge_service WITH LOGIN ENCRYPTED PASSWORD 'fake_password_4'; 
--


GRANT CONNECT ON DATABASE performance TO performance_basic_reader; 
GRANT CONNECT ON DATABASE performance TO performance_data_staff; 
GRANT CONNECT ON DATABASE performance TO performance_backend_service; 
--
GRANT CONNECT ON DATABASE performance TO performance_purge_service; 
--


CREATE ROLE performance_reader_metric; 

GRANT performance_reader_metric TO performance_basic_reader; 
GRANT performance_reader_metric TO performance_data_staff; 


CREATE ROLE performance_writer_metric; 

GRANT performance_writer_metric TO performance_data_staff; 


CREATE ROLE performance_exec_metric; 
CREATE ROLE performance_exec_report; 
--
CREATE ROLE performance_exec_purge; 
--

GRANT performance_exec_metric TO performance_backend_service; 
GRANT performance_exec_report TO performance_backend_service; 
GRANT performance_exec_report TO performance_data_staff; 
--
GRANT performance_exec_purge TO performance_purge_service; 
--


-- !! below line (GRANT SET ON PARAMETER) only works in PostgreSQL 15+ !! 
-- !! if using an older version, make sure the superuser account owns the "purge_exec" schema in "performance" !! 
GRANT SET ON PARAMETER session_replication_role TO performance_dbo; -- !! for purge procedure !!


\c performance 

DROP SCHEMA IF EXISTS "public" RESTRICT
--
; 

-- # # # # # # # # # # # # 
SET ROLE performance_dbo; 
-- # # # # # # # # # # # # 


ALTER DATABASE performance SET search_path = metric, "utility"; 


-- CREATE SCHEMA IF NOT EXISTS utility;

-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA utility; 
-- CREATE EXTENSION IF NOT EXISTS "btree_gist" SCHEMA utility; 


--
--

/*

SELECT X.oid 
 , X.datname 
 , X.datdba 
 , pg_catalog.pg_get_userbyid(X.datdba) AS datdba_name 
FROM pg_catalog.pg_database AS X 
ORDER BY X.oid ASC 
--
; 

SELECT * 
FROM information_schema.schemata; 

*/

--
--
