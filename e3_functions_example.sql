-- \c example 
--

/*****/ 

BEGIN; 

/*****/ 


-- # # # # # # # # # # 
SET ROLE example_dbo; 
-- # # # # # # # # # # 


/*  Content Summary: 


  SCHEMA reference_exec 


  FUNCTION reference_exec.fcn_generate_calendar_date 

  PROCEDURE reference_exec.prc_populate_calendar_date 


*/


--
-- DROP SCHEMA IF EXISTS reference_exec CASCADE; 
--

 
  --
  -- create "reference_exec" schema for general, non-modifying or only-safely-&-routinely-modifying functions relating to the "reference" schema 
  --


--
--

CREATE SCHEMA reference_exec; 

--
--


GRANT USAGE ON SCHEMA reference_exec TO example_exec_reference; 

ALTER DEFAULT PRIVILEGES IN SCHEMA reference_exec GRANT EXECUTE ON FUNCTIONS TO example_exec_reference; 


--
--

  --
  -- create functions in "reference_exec" schema 
  -- 
 
--
--

/*** SELECT list of dates ***/ 

/***********************************************

    FUNCTION: reference_exec.fcn_generate_calendar_date 

    PARAMETER(S):  p_start_date  date  REQUIRED 
                   p_end_date    date  REQUIRED 


    DESCRIPTION: 
    --  
    --  Returns a list of dates between and including
    --   the provided "p_start_date" and "p_end_date" parameter values. 
    -- 


    EXAMPLE: 


     SELECT  X.* 
     --
     FROM  reference_exec.fcn_generate_calendar_date 
               (  p_start_date  =>  '1969-05-01'::date 
               ,  p_end_date    =>  '1969-09-30'::date 
               )  
                  AS  X 
     --
     ORDER BY  X.date_value  ASC 
     -- 
     ; 	


    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-04-22  First published version. 

***********************************************/
CREATE FUNCTION reference_exec.fcn_generate_calendar_date 
( p_start_date date 
, p_end_date date 
) 
RETURNS TABLE 
( date_value date 
--
, year_number int 
, month_number int 
, day_number int 
--
, day_in_year int 
--
, days_after_sunday int 
-- 
)  
AS $main_def$ 


    SELECT  C.date_value 
    --
    ,  C.year_number 
    ,  date_part('month', C.date_value)::int  AS  month_number 
    ,  date_part('day', C.date_value)::int  AS  day_number 
    --
    ,  C.day_in_year 
    --
    , (( EXTRACT( day FROM ( make_date( date_part('year', p_start_date)::int , 1 , 1 )::timestamp 
                           - '2000-01-02'::timestamp -- !! any sunday !! 
                           )::interval )::int 
       % 7 + 7 ) % 7 + C.day_in_calc - 1 ) % 7  AS  days_after_sunday 
    --
    FROM  ( SELECT  GS.date_value::date  AS  date_value 
            -- 
            ,   X.year_number 
            --
            ,   ROW_NUMBER() OVER ( PARTITION BY X.year_number 
                                    ORDER BY GS.date_value )  AS  day_in_year 
            -- 
            ,   ROW_NUMBER() OVER ( ORDER BY GS.date_value )  AS  day_in_calc 
            -- 
             FROM  GENERATE_SERIES( make_date( date_part('year', p_start_date)::int , 1 , 1 ) 
                                  , make_date( date_part('year', p_end_date)::int , 12 , 31 ) 
                                  , make_interval(days => 1) ) 
                      AS GS( date_value ) 
            --
            LEFT JOIN LATERAL (
                SELECT date_part('year', GS.date_value)::int  AS  year_number 
              ) AS X ON true 
            -- 
            WHERE  p_start_date <= p_end_date 
            -- 
          )  AS  C 
    --
    WHERE  C.date_value BETWEEN p_start_date AND p_end_date 
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

/*** INSERT calendar_date ***/ 

/***********************************************

    PROCEDURE:  reference_exec.prc_populate_calendar_date 

    PARAMETER(S):  p_start_date  date  REQUIRED 
                   p_end_date    date  REQUIRED 


    DESCRIPTION: 
    --  
    --  Inserts rows into the "reference"."calendar_date" table. 
    --  


    EXAMPLE: 


     CALL reference_exec.prc_populate_calendar_date 
              (  p_start_date  =>  '1950-01-01'::date 
              ,  p_end_date    =>  '2099-12-31'::date 
              ) 
     -- 
     ;  


    HISTORY: 

--  ----- Date  ----------------------- Note 
--  ----------  ---------------------------- 
--  2024-04-22  First published version. 

***********************************************/
CREATE PROCEDURE reference_exec.prc_populate_calendar_date 
( p_start_date date 
, p_end_date date 
) 
AS $main_def$ 
<<main_block>> 
DECLARE 
  --
  v_raise_message text := ''; 
  --
  v_row_count int; -- for internal use with: -- GET DIAGNOSTICS v_row_count = ROW_COUNT; 
  --
  --
  v_max_range_days int := 99999; 
  --
  --
BEGIN 
	
    v_raise_message := utility.fcn_console_message('START :: reference_exec.prc_populate_calendar_date'); 
    RAISE NOTICE '%' , v_raise_message; 
	
--
--

    IF ( p_start_date IS NULL 
       OR p_end_date IS NULL 
       OR p_start_date > p_end_date ) 
    THEN 
    
      v_raise_message := utility.fcn_console_message('Check input parameter values.'); 
      RAISE NOTICE '%' , v_raise_message; 
      
      v_raise_message := '"p_start_date" and "p_end_date" input parameter values must both be non-null with the start earlier than the end.'; 
      RAISE EXCEPTION '%' , v_raise_message; 
      
    END IF; 

--
--

    CREATE TEMPORARY TABLE tmp_internal_prc_p_c_d_staged_insert 
    (
      tmp_pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
    -- 
    --
    , date_value date NOT NULL 
    --
    , year_number int NOT NULL 
    , month_number int NOT NULL 
    , day_number int NOT NULL 
    --
    , day_in_year int NOT NULL 
    --
    , days_after_sunday int NOT NULL 
    -- 
    -- 
    , CONSTRAINT tmp_pk_i_prc_p_c_d_staged_insert PRIMARY KEY ( tmp_pin ) 
    -- 
    ) ON COMMIT DROP; 

--
--

    v_raise_message := utility.fcn_console_message('Generate "calendar_date" rows for the requested date-range:'); 
    RAISE NOTICE '%' , v_raise_message; 

      INSERT INTO tmp_internal_prc_p_c_d_staged_insert 
      (
        date_value 
      --
      , year_number 
      , month_number 
      , day_number 
      --
      , day_in_year 
      --
      , days_after_sunday 
      --
      )
	  
        SELECT  X.date_value 
        --
        ,   X.year_number 
        ,   X.month_number 
        ,   X.day_number 
        --
        ,   X.day_in_year 
        --
        ,   X.days_after_sunday 
        --
        FROM  reference_exec.fcn_generate_calendar_date 
               (  p_start_date  =>  p_start_date 
               ,  p_end_date    =>  p_end_date 
               )  
                  AS  X 
        --
        ORDER BY  X.date_value 
        --
        ;
		
--
--
	  
    GET DIAGNOSTICS v_row_count = ROW_COUNT; 
    v_raise_message := utility.fcn_console_message(' rows affected = ' || format('%s',coalesce(v_row_count::text,'<<NULL>>')) ); 
    RAISE NOTICE '%' , v_raise_message; 
	
--
--

    IF ( v_row_count > v_max_range_days ) 
    THEN 
    
      v_raise_message := utility.fcn_console_message('Choose a smaller date-range or alter procedure''s maximum row-count.'); 
      RAISE NOTICE '%' , v_raise_message; 
      
      v_raise_message := 'The number of rows staged for insertion exceeds the procedure''s configured upper limit.'; 
      RAISE EXCEPTION '%' , v_raise_message; 
      
    END IF; 
	
--
--
	
    IF EXISTS ( SELECT  null 
                FROM  tmp_internal_prc_p_c_d_staged_insert  AS  X 
                JOIN  reference.calendar_date  AS  E  ON  X.date_value = E.date_value 
                WHERE  X.year_number IS DISTINCT FROM E.year_number 
                OR     X.month_number IS DISTINCT FROM E.month_number 
                OR     X.month_number IS DISTINCT FROM E.month_number 
                OR     X.day_number IS DISTINCT FROM E.day_number 
                OR     X.day_in_year IS DISTINCT FROM E.day_in_year 
                OR     X.days_after_sunday IS DISTINCT FROM E.days_after_sunday ) 
    THEN 

        v_raise_message := utility.fcn_console_message('This shouldn''t happen! Delete or update conflicting table rows if appropriate.'); 
        RAISE NOTICE '%' , v_raise_message; 
        
        v_raise_message := 'There is a conflict between the rows staged for insert and the existing table records.'; 
        RAISE EXCEPTION '%' , v_raise_message; 
  
    END IF; 
	
--
--
	
    v_raise_message := utility.fcn_console_message('Insert new records into "reference"."calendar_date":');  
    RAISE NOTICE '%' , v_raise_message; 
	
      INSERT INTO reference.calendar_date 
      (
        date_value 
      --
      , year_number 
      , month_number 
      , day_number 
      --
      , day_in_year 
      --
      , days_after_sunday 
      --
      )  
	
        SELECT  N.date_value 
        --
        ,       N.year_number 
        ,       N.month_number 
        ,       N.day_number 
        --      
        ,       N.day_in_year 
        --      
        ,       N.days_after_sunday 
        -- 
        FROM  tmp_internal_prc_p_c_d_staged_insert  AS  N 
        --
        LEFT  JOIN  reference.calendar_date  AS  E  ON  N.date_value = E.date_value 
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

    DROP TABLE tmp_internal_prc_p_c_d_staged_insert; 
	
--
--

    v_raise_message := utility.fcn_console_message('END :: reference_exec.prc_populate_calendar_date');  
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

/*****/

COMMIT; 

/*****/
