-- \c example 
--

/*****/

BEGIN;

/*****/


-- # # # # # # # # # # 
SET ROLE example_dbo; 
-- # # # # # # # # # # 


/*  Content Summary: 


  SCHEMA utility  ( & utility_history ) 


  FUNCTION utility.fcn_console_message 

  FUNCTION utility.fcn_history_trigger_before_update 
  FUNCTION utility.fcn_history_trigger_before_insert 
  FUNCTION utility.fcn_history_trigger_before_delete 

  PROCEDURE utility.prc_generate_history_table 
  
  PROCEDURE utility.prc_purge_history_table 
  PROCEDURE utility.prc_purge_all_history_tables 


*/

--
--

CREATE SCHEMA IF NOT EXISTS utility;
CREATE SCHEMA IF NOT EXISTS utility_history;

--
--

/**** UTILITY FUNCTION TO PRINT "DEBUG INFO"-STYLE MESSAGES (TO CONSOLE OR LOG) ****/

/***********************************************

    FUNCTION:  utility.fcn_console_message 

    PARAMETER(S):  p_input_message  text  OPTIONAL 


    DESCRIPTION:
    --
    --  Returns the input message (or a blank/"empty-string" message if no input is provided),
    --   trimmed from the right side,
    --    prefixed on the left with a system timestamp (truncated to seconds)
    --     and a separator string between the timestamp and the message.
    --


    EXAMPLE:

     -- 1. 
     SELECT utility.fcn_console_message() AS output_sample;

     -- 2. 
     SELECT utility.fcn_console_message('test');


    HISTORY:

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE OR REPLACE FUNCTION utility.fcn_console_message ( p_input_message text DEFAULT '' ) RETURNS text AS $main_def$
<<main_block>>
DECLARE
v_clean_input_message text := trim(trailing from coalesce(p_input_message,''));
v_current_timestamp text := to_char(clock_timestamp()::timestamp,'YYYY-MM-DD HH24:MI:SS');
BEGIN

  RETURN format( '%s%s%s' , v_current_timestamp , ' :: '::text , v_clean_input_message );

END main_block;
$main_def$ LANGUAGE plpgsql 
           SECURITY INVOKER 
           SET search_path = utility, pg_temp;

--
--

/**** THREE UTILITY FUNCTIONS TO BE REFERENCED/EXECUTED AS HISTORY TRIGGERS ON TABLES ****/

--
-- 1. 
--

/***********************************************

    TRIGGER FUNCTION:  utility.fcn_history_trigger_before_update 


    DESCRIPTION:
    --
    --  For use with procedure: utility.prc_generate_history_table. 
    --
    --  Intended to be referenced/executed as a BEFORE UPDATE trigger (FOR EACH ROW) on tables. 
    --


    HISTORY:

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.
 
***********************************************/
CREATE OR REPLACE FUNCTION utility.fcn_history_trigger_before_update() RETURNS trigger AS $main_def$
<<main_block>>
DECLARE
v_triggering_action_code char(1) := left(TG_OP,1); -- 'I'/'INSERT' , 'U'/'UPDATE' , 'D'/'DELETE' , 'T'/'TRUNCATE' 
v_current_user text := session_user;
v_current_timestamp timestamp := now();
--
v_history_schema_suffix text := '_history';
v_history_insertion_sql text;
--
BEGIN

  IF ( v_triggering_action_code IN ( 'U' , 'D' , 'T' ) ) THEN 

    IF OLD IS DISTINCT FROM NULL THEN 
       
      v_history_insertion_sql = concat( format( 'INSERT INTO %I.%I ' 
                                              , TG_TABLE_SCHEMA || v_history_schema_suffix 
                                              , TG_TABLE_NAME )
                                      , ' SELECT ''' || v_current_timestamp::text || ''' AS history_insert_time ' 
                                      , ', ''' || format( '%s' , v_current_user  ) || ''' AS history_insert_by ' 
                                      , ', ''' || v_triggering_action_code || ''' AS history_triggering_action_code ' 
                                      , ', ($1).* ' );

      EXECUTE v_history_insertion_sql USING OLD;

    END IF;

  END IF;

  NEW.update_by := v_current_user;
  NEW.update_time := v_current_timestamp;
  RETURN NEW;


EXCEPTION
  WHEN OTHERS THEN
    RAISE WARNING 'Error ignored in triggered historical record-auditing function: %', TG_NAME;
    RETURN NEW;

END main_block;
$main_def$ LANGUAGE plpgsql 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--

--
-- 2. 
--

/***********************************************

    TRIGGER FUNCTION:  utility.fcn_history_trigger_before_insert 


    DESCRIPTION:
    --
    --  For use with procedure: utility.prc_generate_history_table.
    --
    --  Intended to be referenced/executed as a BEFORE INSERT trigger (FOR EACH ROW) on tables.
    --


    HISTORY:

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE OR REPLACE FUNCTION utility.fcn_history_trigger_before_insert() RETURNS trigger AS $main_def$
<<main_block>>
DECLARE
v_current_user text := session_user;
v_current_timestamp timestamp := now();
BEGIN

  NEW.update_by := v_current_user;
  NEW.update_time := v_current_timestamp;
  RETURN NEW;


EXCEPTION
  WHEN OTHERS THEN 
    RAISE WARNING 'Error ignored in triggered historical record-auditing function: %', TG_NAME;
    RETURN NEW;

END main_block;
$main_def$ LANGUAGE plpgsql 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--

--
-- 3. 
--

/***********************************************

    TRIGGER FUNCTION: utility.fcn_history_trigger_before_delete 


    DESCRIPTION:
    --
    --  For use with procedure: utility.prc_generate_history_table.
    --
    --  Intended to be referenced/executed as a BEFORE DELETE trigger (FOR EACH ROW) on tables.
    --


    HISTORY:

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.

***********************************************/
CREATE OR REPLACE FUNCTION utility.fcn_history_trigger_before_delete() RETURNS trigger AS $main_def$
<<main_block>>
DECLARE
v_triggering_action_code char(1) := left(TG_OP,1); -- 'I'/'INSERT' , 'U'/'UPDATE' , 'D'/'DELETE' , 'T'/'TRUNCATE' 
v_current_user text := session_user;
v_current_timestamp timestamp := current_timestamp;
--
v_history_schema_suffix text := '_history';
v_history_insertion_sql text;
--
BEGIN

  IF ( v_triggering_action_code IN ( 'D' , 'T' , 'U' ) ) THEN 

    IF OLD IS DISTINCT FROM NULL THEN 

      v_history_insertion_sql = concat( format( 'INSERT INTO %I.%I ' 
                                              , TG_TABLE_SCHEMA || v_history_schema_suffix 
                                              , TG_TABLE_NAME )
                                      , ' SELECT ''' || v_current_timestamp::text || ''' AS history_insert_time ' 
                                      , ', ''' || format( '%s' , v_current_user  ) || ''' AS history_insert_by ' 
                                      , ', ''' || v_triggering_action_code || ''' AS history_triggering_action_code ' 
                                      , ', ($1).* ' );

      EXECUTE v_history_insertion_sql USING OLD; 

    END IF;

  END IF;


  RETURN OLD;


EXCEPTION 
  WHEN OTHERS THEN 
    RAISE WARNING 'Error ignored in triggered historical record-auditing function: %', TG_NAME;
    RETURN OLD;

END main_block;
$main_def$ LANGUAGE plpgsql 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

--
--

/**** UTILITY PROCEDURE TO CREATE A "..._history"-SCHEMA COMPANION TABLE FOR AN INPUT TABLE, & ALSO CONFIGURE TRIGGERS ON THE INPUT TABLE ****/

/***********************************************

    PROCEDURE:  utility.prc_generate_history_table 

    PARAMETER(S):  p_target_table_schema  text  REQUIRED 
                   p_target_table_name    text  REQUIRED 

    DESCRIPTION:
    --
    --  For a provided/target table (with schema; in the current database) 
    --   create a companion "history" table, with the same name, 
    --    in a specially suffixed schema which must already exist. 
    --
    --  Then, create 3 triggers on the provided/target table 
    --   to populate the "history" table before any UPDATE or DELETE actions, 
    --    and to prevent unscrupulous or accidental tampering with "update audit fields" 
    --     (expected to exist in the target/requested table) during INSERT or UPDATE actions. 
    --


    EXAMPLE: 

     CALL utility.prc_generate_history_table( 'reference' , 'alphabet_letter' );


    HISTORY:

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.
 
***********************************************/
CREATE OR REPLACE PROCEDURE utility.prc_generate_history_table( p_target_table_schema text , p_target_table_name text ) AS $main_def$
<<main_block>>
DECLARE
v_raise_message text := '';
--
v_current_database text := current_database();
--
v_history_schema_suffix text := '_history';
--
v_table_type_expected text := 'BASE TABLE';
--
v_history_trigger_prefix text := 'tg_';
v_history_trigger_suffix_before_insert text := '_set_update_audit_fields';
v_history_trigger_suffix_before_update text := '_set_fields_and_populate_history';
v_history_trigger_suffix_before_delete text := '_populate_history_table';
--
BEGIN 

    v_raise_message := utility.fcn_console_message('START :: utility.prc_generate_history_table');  
    RAISE NOTICE '%' , v_raise_message;
    
    v_raise_message := utility.fcn_console_message('Current Database = ' || format('%I',coalesce(v_current_database,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message;
    v_raise_message := utility.fcn_console_message('Input/Target Table Schema = ' || format('%I',coalesce(p_target_table_schema,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message;
    v_raise_message := utility.fcn_console_message('Input/Target Table Name = ' || format('%I',coalesce(p_target_table_name,'<<NULL>>')) );  
    RAISE NOTICE '%' , v_raise_message;

    --
    --  Check input parameters ... 
    --

    IF p_target_table_schema IS NULL THEN 
    
      v_raise_message := utility.fcn_console_message('Input parameter "p_target_table_schema" is NULL.');
      RAISE NOTICE '%' , v_raise_message;
      
      v_raise_message := 'A non-null value must be provided for input parameter "p_target_table_schema".';
      RAISE EXCEPTION '%' , v_raise_message;
      
    END IF; 
    
    IF p_target_table_name IS NULL THEN 
    
      v_raise_message := utility.fcn_console_message('Input parameter "p_target_table_name" is NULL.');
      RAISE NOTICE '%' , v_raise_message;
      
      v_raise_message := 'A non-null value must be provided for input parameter "p_target_table_name".';
      RAISE EXCEPTION '%' , v_raise_message;
      
    END IF; 


    IF RIGHT(p_target_table_schema,char_length(v_history_schema_suffix)) = v_history_schema_suffix THEN 
    
      v_raise_message := 'The target schema has a special/reserved/forbidden suffix: ' || format('%I',v_history_schema_suffix);
      RAISE EXCEPTION '%' , v_raise_message;
      
    END IF; 

    --
    --  Validate request ... 
    --
    
    v_raise_message := utility.fcn_console_message( format( 'Check that the requested/target table exists: "%I"."%I"."%I".' 
                                                          , v_current_database
                                                          , p_target_table_schema 
                                                          , p_target_table_name ) );  
    RAISE NOTICE '%' , v_raise_message; 
    
    IF NOT( EXISTS( SELECT null 
                    FROM information_schema.tables AS X 
                    WHERE X.table_catalog = v_current_database 
                    AND X.table_schema = p_target_table_schema 
                    AND X.table_name = p_target_table_name 
                    AND upper(X.table_type) = v_table_type_expected ) ) 
    THEN 
    
      v_raise_message := format( 'No table %I in schema %I exists in current database (%I).' 
                               , p_target_table_name 
                               , p_target_table_schema 
                               , v_current_database );
      RAISE EXCEPTION '%' , v_raise_message;
    
    END IF; 

    
    v_raise_message := utility.fcn_console_message( format( 'Check that a "history" schema exists, for new companion table: "%I".' 
                                                          , concat(p_target_table_schema,v_history_schema_suffix) ) );
    RAISE NOTICE '%' , v_raise_message;
    
    IF NOT( EXISTS ( SELECT null 
                     FROM information_schema.schemata AS X 
                     WHERE X.catalog_name = v_current_database 
                     AND X.schema_name = concat(p_target_table_schema,v_history_schema_suffix) ) ) 
    THEN 
    
      v_raise_message := format( 'No schema %I exists in in current database (%I).' 
                               , concat(p_target_table_schema,v_history_schema_suffix)
                               , v_current_database );
      RAISE EXCEPTION '%' , v_raise_message;
    
    END IF; 


    v_raise_message := utility.fcn_console_message( format( 'Check that no table exists already with new/planned name: "%I"."%I".' 
                                                          , concat(p_target_table_schema,v_history_schema_suffix)
                                                          , p_target_table_name ) );
    RAISE NOTICE '%' , v_raise_message;
    
    IF EXISTS( SELECT null 
               FROM information_schema.tables AS X 
               WHERE X.table_catalog = v_current_database 
               AND X.table_schema = concat(p_target_table_schema,v_history_schema_suffix) 
               AND X.table_name = p_target_table_name ) 
    THEN

      v_raise_message := format( 'A table object %I in schema %I already exists in current database (%I).' 
                               , p_target_table_name 
                               , concat(p_target_table_schema,v_history_schema_suffix) 
                               , v_current_database ); 
      RAISE EXCEPTION '%' , v_raise_message;

    END IF;


    v_raise_message := utility.fcn_console_message('Check that no triggers exist already with new/planned names, on target table.');
    RAISE NOTICE '%' , v_raise_message;

    IF EXISTS( SELECT null 
               FROM information_schema.triggers AS X 
               INNER JOIN ( SELECT concat( v_history_trigger_prefix 
                                         , p_target_table_name 
                                         , history_trigger_suffix ) AS history_trigger_name 
                            FROM ( VALUES ( v_history_trigger_suffix_before_insert ) 
                                   ,      ( v_history_trigger_suffix_before_update ) 
                                   ,      ( v_history_trigger_suffix_before_delete ) 
                                 ) AS Ys ( history_trigger_suffix ) 
                          ) AS Y ON X.trigger_name = Y.history_trigger_name 
               WHERE X.event_object_catalog = v_current_database 
               AND X.event_object_schema = p_target_table_schema 
               AND X.event_object_table = p_target_table_name ) 
    THEN

      v_raise_message := 'A trigger already exists on the target table having a name planned/reserved for a new trigger to create.';
      RAISE EXCEPTION '%' , v_raise_message;

    END IF;


    v_raise_message := utility.fcn_console_message('Check that "update_time" and "update_by" exist as columns in the target table.');
    RAISE NOTICE '%' , v_raise_message;

    IF NOT( EXISTS( SELECT null 
                    FROM information_schema.columns AS X 
                    WHERE X.table_catalog = v_current_database 
                    AND X.table_schema = p_target_table_schema 
                    AND X.table_name = p_target_table_name 
                    AND X.column_name = 'update_time' 
                    AND X.column_default ilike '%current%timestamp%' ) ) 
    OR NOT( EXISTS( SELECT null 
                    FROM information_schema.columns AS X 
                    WHERE X.table_catalog = v_current_database 
                    AND X.table_schema = p_target_table_schema 
                    AND X.table_name = p_target_table_name 
                    AND X.column_name = 'update_by' 
                    AND X.column_default ilike '%session%user%' ) ) 
    THEN
    
      v_raise_message := 'Either "update_time" or "update_by" does not exist (with expected DEFAULT expression) in the target table''s column list.';
      RAISE EXCEPTION '%' , v_raise_message;
    
    END IF;

    --
    --  Perform request ... 
    --

    <<create_history_table>> 
    DECLARE
      --
      v_sql_column_list text;
      v_create_table_sql text;
      --
    BEGIN

        v_raise_message := utility.fcn_console_message('Creating history table...');
        RAISE NOTICE '%' , v_raise_message;

        v_sql_column_list := ( SELECT string_agg( concat( ' 
, '                                                     , format('%I',X.column_name)
                                                        , ' text null ' ) , '' ORDER BY X.ordinal_position ASC ) 
                               FROM information_schema.columns AS X 
                               WHERE X.table_catalog = v_current_database 
                               AND X.table_schema = p_target_table_schema 
                               AND X.table_name = p_target_table_name );


        v_create_table_sql := concat( format( 'CREATE TABLE %I.%I ( ' 
                                            , concat(p_target_table_schema,main_block.v_history_schema_suffix) 
                                            , p_target_table_name ) 
                                    , '

  history_insert_time timestamp NOT NULL DEFAULT current_timestamp
, history_insert_by text NOT NULL DEFAULT session_user
, history_triggering_action_code char(1) NOT NULL
--
'                                   , v_sql_column_list 
                                    , ' 
--
, history_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
--
, CONSTRAINT pk_history_' || p_target_table_name || ' PRIMARY KEY ( history_pin ) 
, CONSTRAINT ck_history_' || p_target_table_name || '_action_code CHECK ( history_triggering_action_code IN ( ''I'' , ''U'' , ''D'' , ''T'' ) ) 
--
);

' );

        
        EXECUTE v_create_table_sql;

    --
    END create_history_table; 
    --
    
    
    <<configure_history_triggers>> 
    DECLARE
      --
      v_trigger_name_before_insert text := concat( main_block.v_history_trigger_prefix 
                                                 , p_target_table_name 
                                                 , main_block.v_history_trigger_suffix_before_insert ); 
      v_trigger_name_before_update text := concat( main_block.v_history_trigger_prefix 
                                                 , p_target_table_name 
                                                 , main_block.v_history_trigger_suffix_before_update ); 
      v_trigger_name_before_delete text := concat( main_block.v_history_trigger_prefix 
                                                 , p_target_table_name 
                                                 , main_block.v_history_trigger_suffix_before_delete ); 
      --
      v_create_trigger_sql_01 text; 
      v_create_trigger_sql_02 text; 
      v_create_trigger_sql_03 text; 
      --
      v_create_trigger_sql_all text; 
      --
    BEGIN

        v_raise_message := utility.fcn_console_message('Creating triggers on target table...');  
        RAISE NOTICE '%' , v_raise_message; 

--
--  BEFORE INSERT ... 
--

          v_create_trigger_sql_01 := concat( format( 'CREATE TRIGGER %I BEFORE INSERT ON %I.%I ' 
                                                   , v_trigger_name_before_insert 
                                                   , p_target_table_schema 
                                                   , p_target_table_name ) 
                                           , '
FOR EACH ROW EXECUTE FUNCTION utility.fcn_history_trigger_before_insert();
' );

--
--  BEFORE UPDATE ... 
--

          v_create_trigger_sql_02 := concat( format( 'CREATE TRIGGER %I BEFORE UPDATE ON %I.%I ' 
                                                   , v_trigger_name_before_update 
                                                   , p_target_table_schema 
                                                   , p_target_table_name ) 
                                           , '
FOR EACH ROW EXECUTE FUNCTION utility.fcn_history_trigger_before_update();
' );
        
--
--  BEFORE DELETE ... 
--

          v_create_trigger_sql_03 := concat( format( 'CREATE TRIGGER %I BEFORE DELETE ON %I.%I ' 
                                                   , v_trigger_name_before_delete 
                                                   , p_target_table_schema
                                                   , p_target_table_name )
                                           , '
  FOR EACH ROW EXECUTE FUNCTION utility.fcn_history_trigger_before_delete();
  ' );


      v_create_trigger_sql_all := concat( v_create_trigger_sql_01
                                        , v_create_trigger_sql_02
                                        , v_create_trigger_sql_03 );

      EXECUTE v_create_trigger_sql_all;

    -- 
    END configure_history_triggers;
    --


        v_raise_message := utility.fcn_console_message('Procedure completed successfully.');
        RAISE NOTICE '%' , v_raise_message;


    v_raise_message := utility.fcn_console_message('END :: utility.prc_generate_history_table');
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

/**** UTILITY PROCEDURE TO DELETE RECORDS FROM A SINGLE "..._history"-SCHEMA COMPANION TABLE ****/

/***********************************************

    PROCEDURE:  utility.prc_purge_history_table 

    PARAMETER(S):  p_history_table_schema  text     REQUIRED 
                   p_history_table_name    text     REQUIRED 
                   p_days_back_to_keep     int      REQUIRED 
                   p_delete_records        boolean  REQUIRED 

    DESCRIPTION:
    --
    --  For a provided companion "history" table, 
    --   deletes records older than the "p_days_back_to_keep" parameter-value, 
    --    or deletes all records if "p_days_back_to_keep" is NULL. 
    --     Action will only be performed if "p_delete_records" is set TRUE. 
    --


    EXAMPLE:

     CALL utility.prc_purge_history_table( p_history_table_schema  =>  'reference_history' 
                                         , p_history_table_name    =>  'alphabet_letter' 
                                         , p_days_back_to_keep     =>  3333  -- null 
                                         , p_delete_records        =>  false 
                                         --
                                         );


    HISTORY:

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.
 
***********************************************/
CREATE OR REPLACE PROCEDURE utility.prc_purge_history_table 
( p_history_table_schema text
, p_history_table_name text
, p_days_back_to_keep int
, p_delete_records boolean ) AS $main_def$
<<main_block>>
DECLARE
v_raise_message text := '';
v_row_count bigint;
--
v_current_database text := current_database();
--
v_current_timestamp timestamp := current_timestamp;
-- 
v_history_schema_suffix text := '_history';
--
v_table_type_expected text := 'BASE TABLE';
--
--
v_action_sql text; 
--
--
BEGIN

    v_raise_message := utility.fcn_console_message('START :: utility.prc_purge_history_table');
    RAISE NOTICE '%' , v_raise_message;

    v_raise_message := utility.fcn_console_message('Current Database = ' || format('%I',coalesce(v_current_database,'<<NULL>>')) );
    RAISE NOTICE '%' , v_raise_message;
    v_raise_message := utility.fcn_console_message('History Table Schema = ' || format('%I',coalesce(p_history_table_schema,'<<NULL>>')) );
    RAISE NOTICE '%' , v_raise_message;
    v_raise_message := utility.fcn_console_message('History Table Name = ' || format('%I',coalesce(p_history_table_name,'<<NULL>>')) );
    RAISE NOTICE '%' , v_raise_message;

    --
    --  Check input parameters ... 
    --

    IF p_history_table_schema IS NULL THEN 
    
      v_raise_message := utility.fcn_console_message('Input parameter "p_history_table_schema" is NULL.');
      RAISE NOTICE '%' , v_raise_message;
      
      v_raise_message := 'A non-null value must be provided for input parameter "p_history_table_schema".';
      RAISE EXCEPTION '%' , v_raise_message;
      
    END IF; 
    
    IF p_history_table_name IS NULL THEN 
    
      v_raise_message := utility.fcn_console_message('Input parameter "p_history_table_name" is NULL.');
      RAISE NOTICE '%' , v_raise_message;
      
      v_raise_message := 'A non-null value must be provided for input parameter "p_history_table_name".';
      RAISE EXCEPTION '%' , v_raise_message;
      
    END IF; 


    IF RIGHT(p_history_table_schema,char_length(v_history_schema_suffix)) IS DISTINCT FROM v_history_schema_suffix THEN 
    
      v_raise_message := 'The provided history schema name does not have the expected suffix: ' || format('%I',v_history_schema_suffix);
      RAISE EXCEPTION '%' , v_raise_message;
      
    END IF;


    IF ( p_days_back_to_keep IS NOT NULL AND p_days_back_to_keep < 0 ) THEN 
    
      v_raise_message := utility.fcn_console_message('Input parameter "p_days_back_to_keep" must be either NULL or non-negative.');
      RAISE NOTICE '%' , v_raise_message;
      
      v_raise_message := 'If a non-null value is provided for input parameter "p_days_back_to_keep" then it should be non-negative.';
      RAISE EXCEPTION '%' , v_raise_message;
      
    END IF;

    --
    --  Validate request ... 
    --

    v_raise_message := utility.fcn_console_message( format( 'Check that the requested/target table exists: "%I"."%I"."%I".' 
                                                          , v_current_database 
                                                          , p_history_table_schema 
                                                          , p_history_table_name ) );
    RAISE NOTICE '%' , v_raise_message; 
    
    IF NOT( EXISTS( SELECT null 
                    FROM information_schema.tables AS X 
                    WHERE X.table_catalog = v_current_database 
                    AND X.table_schema = p_history_table_schema 
                    AND X.table_name = p_history_table_name 
                    AND upper(X.table_type) = v_table_type_expected ) ) 
    THEN 
    
      v_raise_message := format( 'No table %I in schema %I exists in current database (%I).' 
                               , p_history_table_name 
                               , p_history_table_schema 
                               , v_current_database );  
      RAISE EXCEPTION '%' , v_raise_message;
    
    END IF;


    v_raise_message := utility.fcn_console_message( format( 'Check that a companion table exists: "%I"."%I"."%I".' 
                                                          , v_current_database
                                                          , LEFT( p_history_table_schema , char_length(p_history_table_schema) - char_length(v_history_schema_suffix) ) 
                                                          , p_history_table_name ) );  
    RAISE NOTICE '%' , v_raise_message; 
    
    IF NOT( EXISTS( SELECT null 
                    FROM information_schema.tables AS X 
                    WHERE X.table_catalog = v_current_database 
                    AND X.table_schema = LEFT( p_history_table_schema , char_length(p_history_table_schema) - char_length(v_history_schema_suffix) ) 
                    AND X.table_name = p_history_table_name 
                    AND upper(X.table_type) = v_table_type_expected ) ) 
    THEN 
    
      v_raise_message := format( 'No table %I in schema %I exists in current database (%I).' 
                               , p_history_table_name 
                               , LEFT( p_history_table_schema , char_length(p_history_table_schema) - char_length(v_history_schema_suffix) )  
                               , v_current_database );  
      RAISE EXCEPTION '%' , v_raise_message;
    
    END IF;
    
    
    v_raise_message := utility.fcn_console_message('Check that "history_insert_time" exists as a column in the target table.');  
    RAISE NOTICE '%' , v_raise_message; 
    
    IF NOT( EXISTS( SELECT null 
                    FROM information_schema.columns AS X 
                    WHERE X.table_catalog = v_current_database 
                    AND X.table_schema = p_history_table_schema 
                    AND X.table_name = p_history_table_name 
                    AND X.column_name = 'history_insert_time'     
                    AND X.column_default ilike '%current%timestamp%' ) ) 
    THEN 
    
      v_raise_message := '"history_insert_time" does not exist (with expected DEFAULT expression) in the requested history table''s column list.';
      RAISE EXCEPTION '%' , v_raise_message;
    
    END IF;

    --
    --  Perform request ... 
    --

        IF p_delete_records = true 
        THEN 
        -- 

            v_raise_message := utility.fcn_console_message('Delete history table records...');  
            RAISE NOTICE '%' , v_raise_message; 


            v_action_sql := ( SELECT format( 'DELETE FROM %I.%I '
                                           , p_history_table_schema
                                           , p_history_table_name ) 
                                || CASE WHEN p_days_back_to_keep IS NULL 
                                        THEN '; ' 
                                        ELSE ' 
WHERE history_insert_time < ''' || ( v_current_timestamp - make_interval( days => p_days_back_to_keep ) )::text || ''' ; ' 
                                   END 
                            );


            EXECUTE v_action_sql;


            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );
            RAISE NOTICE '%' , v_raise_message;

        --
        ELSE
        --

            v_raise_message := utility.fcn_console_message('Count history table records eligible for deletion...');
            RAISE NOTICE '%' , v_raise_message;


            v_action_sql := ( SELECT format( 'SELECT COUNT(*)::bigint FROM %I.%I ' 
                                           , p_history_table_schema 
                                           , p_history_table_name ) 
                                || CASE WHEN p_days_back_to_keep IS NULL 
                                        THEN '; ' 
                                        ELSE ' 
WHERE history_insert_time < ''' || ( v_current_timestamp - make_interval( days => p_days_back_to_keep ) )::text || ''' ; ' 
                                   END 
                            );


            EXECUTE v_action_sql 
            INTO v_row_count ; 


            v_raise_message := utility.fcn_console_message(' number of rows = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) ); 
            RAISE NOTICE '%' , v_raise_message; 


        -- 
        END IF;


        v_raise_message := utility.fcn_console_message('Procedure completed successfully.');
        RAISE NOTICE '%' , v_raise_message;


    v_raise_message := utility.fcn_console_message('END :: utility.prc_purge_history_table');
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

/**** UTILITY PROCEDURE TO DELETE RECORDS FROM ALL "..._history"-SCHEMA TABLES ****/

/***********************************************

    PROCEDURE:  utility.prc_purge_all_history_tables 

    PARAMETER(S):  p_history_table_schema  text     REQUIRED 
                   p_days_back_to_keep     int      REQUIRED 
                   p_delete_records        boolean  REQUIRED 

    DESCRIPTION:
    --
    --  For all "history" tables (limited to one schema if "p_history_table_schema" is provided), 
    --   deletes records older than the "p_days_back_to_keep" parameter-value, 
    --    or deletes all records if "p_days_back_to_keep" is NULL. 
    --     Actions will only be performed if "p_delete_records" is set TRUE. 
    --


    EXAMPLE:

     CALL utility.prc_purge_all_history_tables( p_history_table_schema  =>  'reference_history' 
                                              , p_days_back_to_keep     =>  3333  -- null 
                                              , p_delete_records        =>  false 
                                              );


    HISTORY:

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-02-05  First published version.
 
***********************************************/
CREATE OR REPLACE PROCEDURE utility.prc_purge_all_history_tables 
( p_history_table_schema text 
, p_days_back_to_keep int 
, p_delete_records boolean ) AS $main_def$
<<main_block>>
DECLARE
v_raise_message text := ''; 
v_row_count bigint; 
--
v_current_database text := current_database(); 
--
v_history_schema_suffix text := '_history'; 
--
v_table_type_expected text := 'BASE TABLE'; 
--
--
v_loop_current_iteration int := 1; 
v_loop_total_history_table_count int; 
--
v_loop_current_history_table_schema text; 
v_loop_current_history_table_name text; 
--
--
BEGIN

    v_raise_message := utility.fcn_console_message('START :: utility.prc_purge_all_history_tables');
    RAISE NOTICE '%' , v_raise_message;
    
    v_raise_message := utility.fcn_console_message('Current Database = ' || format('%I',coalesce(v_current_database,'<<NULL>>')) );
    RAISE NOTICE '%' , v_raise_message;
    v_raise_message := utility.fcn_console_message('History Table Schema = ' || format('%I',coalesce(p_history_table_schema,'<<NULL>>')) );
    RAISE NOTICE '%' , v_raise_message;


CREATE TEMPORARY TABLE tmp_internal_prc_p_a_h_t_table_to_purge 
(
  tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
--
--
, history_table_schema text NOT NULL 
, history_table_name text NOT NULL 
--
--
, CONSTRAINT tmp_pk_table_to_purge PRIMARY KEY ( tmp_pin ) 
, CONSTRAINT tmp_uix_table_to_purge UNIQUE ( history_table_schema 
                                           , history_table_name ) 
--
) ON COMMIT DROP;


    --
    --  Check input parameters ... 
    --

    IF ( p_history_table_schema IS NOT NULL 
       AND RIGHT(p_history_table_schema,char_length(v_history_schema_suffix)) IS DISTINCT FROM v_history_schema_suffix ) 
    THEN 

      v_raise_message := 'The provided history schema name does not have the expected suffix: ' || format('%I',v_history_schema_suffix);
      RAISE EXCEPTION '%' , v_raise_message;

    END IF;

    IF ( p_days_back_to_keep IS NOT NULL AND p_days_back_to_keep < 0 ) THEN 
    
      v_raise_message := utility.fcn_console_message('Input parameter "p_days_back_to_keep" must be either NULL or non-negative.');
      RAISE NOTICE '%' , v_raise_message; 
      
      v_raise_message := 'If a non-null value is provided for input parameter "p_days_back_to_keep" then it should be non-negative.';
      RAISE EXCEPTION '%' , v_raise_message; 
      
    END IF;

    --
    --  Validate request ... 
    --

    v_raise_message := utility.fcn_console_message('Gather list of tables to purge.');
    RAISE NOTICE '%' , v_raise_message;
    
    INSERT INTO tmp_internal_prc_p_a_h_t_table_to_purge 
    ( 
      history_table_schema 
    , history_table_name 
    ) 
    
      SELECT  H.table_schema 
      ,       H.table_name 
      -- 
      FROM        information_schema.tables  AS  H 
      INNER JOIN  information_schema.tables  AS  C 
        ON  H.table_catalog = C.table_catalog 
        AND LEFT( H.table_schema , GREATEST( char_length(H.table_schema) - char_length(v_history_schema_suffix) , 0 ) ) = C.table_schema 
        AND H.table_name = C.table_name 
        AND upper(H.table_type) = upper(C.table_type) 
      -- 
      WHERE H.table_catalog = v_current_database 
      AND RIGHT(H.table_schema,char_length(v_history_schema_suffix)) = v_history_schema_suffix 
      AND ( p_history_table_schema IS NULL 
          OR H.table_schema = p_history_table_schema ) 
      AND upper(H.table_type) = v_table_type_expected  
      --
      ORDER BY  H.table_schema  
      ,         H.table_name    
      -- 
      ;  
      
    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) );
    RAISE NOTICE '%' , v_raise_message;


    v_loop_total_history_table_count := v_row_count; 
    
    IF v_loop_total_history_table_count IS NULL 
    OR v_loop_total_history_table_count = 0 
    THEN 
    
      v_raise_message := 'No eligible tables were identified for purging.';
      RAISE EXCEPTION '%' , v_raise_message;
      
    END IF; 

    --
    --  Perform request ... 
    --

    v_raise_message := utility.fcn_console_message('Loop through tables in list...');
    RAISE NOTICE '%' , v_raise_message;

    WHILE ( v_loop_current_iteration <= v_loop_total_history_table_count ) 
    LOOP 
    --
    
        SELECT  X.history_table_schema 
        ,       X.history_table_name 
        -- 
        INTO    v_loop_current_history_table_schema 
        ,       v_loop_current_history_table_name 
        -- 
        FROM  tmp_internal_prc_p_a_h_t_table_to_purge  AS  X 
        -- 
        WHERE  X.tmp_pin = v_loop_current_iteration 
        -- 
        ; 


    CALL utility.prc_purge_history_table( p_history_table_schema  =>  v_loop_current_history_table_schema
                                        , p_history_table_name    =>  v_loop_current_history_table_name 
                                        , p_days_back_to_keep     =>  p_days_back_to_keep 
                                        , p_delete_records        =>  p_delete_records 
                                        -- 
                                        ); 


    v_loop_current_iteration := v_loop_current_iteration + 1; 


    --
    END LOOP; 

    v_raise_message := utility.fcn_console_message('Loop finished.');
    RAISE NOTICE '%' , v_raise_message;
    
    
        v_raise_message := utility.fcn_console_message('Procedure completed successfully.');
        RAISE NOTICE '%' , v_raise_message;

    
    v_raise_message := utility.fcn_console_message('END :: utility.prc_purge_all_history_tables');
    RAISE NOTICE '%' , v_raise_message;
    
--
--
END main_block;
$main_def$ LANGUAGE plpgsql 
           SECURITY DEFINER 
           SET search_path = utility, pg_temp;

/*****/

COMMIT;

/*****/
