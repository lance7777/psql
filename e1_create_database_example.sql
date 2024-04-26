-- \c postgres <<superuser_server_admin>> 
--

CREATE ROLE example_dbo; 

GRANT example_dbo TO <<superuser_server_admin>>; 

CREATE DATABASE example OWNER example_dbo; 
REVOKE ALL ON DATABASE example FROM PUBLIC; 

GRANT CREATE ON DATABASE example TO example_dbo; 


CREATE ROLE example_basic_reader WITH LOGIN ENCRYPTED PASSWORD 'fake_password_1'; 
CREATE ROLE example_data_staff WITH LOGIN ENCRYPTED PASSWORD 'fake_password_2'; 
CREATE ROLE example_backend_service WITH LOGIN ENCRYPTED PASSWORD 'fake_password_3'; 


GRANT CONNECT ON DATABASE example TO example_basic_reader; 
GRANT CONNECT ON DATABASE example TO example_data_staff; 
GRANT CONNECT ON DATABASE example TO example_backend_service; 


CREATE ROLE example_reader_reference; 

GRANT example_reader_reference TO example_basic_reader; 
GRANT example_reader_reference TO example_data_staff; 


CREATE ROLE example_writer_reference; 

GRANT example_writer_reference TO example_data_staff; 


CREATE ROLE example_exec_reference; 

GRANT example_exec_reference TO example_backend_service; 


\c example 

DROP SCHEMA IF EXISTS "public" RESTRICT
--
; 

-- # # # # # # # # # # 
SET ROLE example_dbo; 
-- # # # # # # # # # # 


ALTER DATABASE example SET search_path = reference, "utility"; 


-- CREATE SCHEMA IF NOT EXISTS utility;

-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA utility; 
-- CREATE EXTENSION IF NOT EXISTS "btree_gist" SCHEMA utility; 


--
--

/*

\l+

\dn

*/

--
--
