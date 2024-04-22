-- \c example 
--

/*****/ 

BEGIN; 

/*****/ 


-- # # # # # # # # # # 
SET ROLE example_dbo; 
-- # # # # # # # # # # 


/*  Content Summary: 


  SCHEMA reference  ( & reference_history ) 


  TABLE reference.alphabet_letter 
  VIEW reference.vw_alphabet_letter 

  TABLE reference.weekday 
  VIEW reference.vw_weekday 
  TABLE reference.month 
  VIEW reference.vw_month 
  TABLE reference.calendar_date 
  VIEW reference.vw_calendar_date 


*/


--
-- DROP SCHEMA IF EXISTS reference, reference_history CASCADE; 
--


--
--

CREATE SCHEMA reference;
CREATE SCHEMA reference_history;

--
--


GRANT USAGE ON SCHEMA reference TO example_reader_reference;
GRANT USAGE ON SCHEMA reference TO example_writer_reference;

ALTER DEFAULT PRIVILEGES IN SCHEMA reference GRANT SELECT ON TABLES TO example_reader_reference; 
ALTER DEFAULT PRIVILEGES IN SCHEMA reference GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO example_writer_reference; 


--
--
 
  --
  -- create tables in "reference" schema 
  -- 
 
--
--

/*** alphabet_letter ***/ 

CREATE TABLE reference.alphabet_letter (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, order_number int NOT NULL 
, uppercase text NOT NULL 
, lowercase text NOT NULL 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_alphabet_letter PRIMARY KEY ( pin ) 
, CONSTRAINT uix_alphabet_letter_order_number UNIQUE ( order_number ) 
, CONSTRAINT uix_alphabet_letter_uppercase UNIQUE ( uppercase ) 
, CONSTRAINT uix_alphabet_letter_lowercase UNIQUE ( lowercase ) 
--
, CONSTRAINT ck_alphabet_letter_order_number CHECK( order_number BETWEEN 1 AND 26 ) 
, CONSTRAINT ck_alphabet_letter_uppercase CHECK( uppercase = LEFT( upper(uppercase) || '0' , 1 ) ) 
, CONSTRAINT ck_alphabet_letter_lowercase CHECK( lowercase = lower(uppercase) ) 
--
--
);

/* !! */ CALL utility.prc_generate_history_table( 'reference' , 'alphabet_letter' ); 

CREATE VIEW reference.vw_alphabet_letter AS 

SELECT  X.order_number 
,       X.uppercase 
,       X.lowercase 
-- 
FROM  reference.alphabet_letter  AS  X 
-- 
;  

--
--

/***********************************************/

INSERT INTO reference.alphabet_letter 
( order_number , uppercase , lowercase )

VALUES 
  (  1 , 'A' , 'a' ) 
, (  2 , 'B' , 'b' ) 
, (  3 , 'C' , 'c' ) 
, (  4 , 'D' , 'd' ) 
, (  5 , 'E' , 'e' ) 
, (  6 , 'F' , 'f' ) 
, (  7 , 'G' , 'g' ) 
, (  8 , 'H' , 'h' ) 
, (  9 , 'I' , 'i' ) 
, ( 10 , 'J' , 'j' ) 
, ( 11 , 'K' , 'k' ) 
, ( 12 , 'L' , 'l' ) 
, ( 13 , 'M' , 'm' ) 
, ( 14 , 'N' , 'n' ) 
, ( 15 , 'O' , 'o' ) 
, ( 16 , 'P' , 'p' ) 
, ( 17 , 'Q' , 'q' ) 
, ( 18 , 'R' , 'r' ) 
, ( 19 , 'S' , 's' ) 
, ( 20 , 'T' , 't' ) 
, ( 21 , 'U' , 'u' ) 
, ( 22 , 'V' , 'v' ) 
, ( 23 , 'W' , 'w' ) 
, ( 24 , 'X' , 'x' ) 
, ( 25 , 'Y' , 'y' ) 
, ( 26 , 'Z' , 'z' ) 
--
; 

/***********************************************/

--
--

/*** weekday ***/ 

CREATE TABLE reference.weekday (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, days_after_sunday int NOT NULL 
, display_name text NOT NULL 
, abbreviation3 text NOT NULL 
, abbreviation1 text NOT NULL 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_weekday PRIMARY KEY ( pin ) 
, CONSTRAINT uix_weekday_days_after_sunday UNIQUE ( days_after_sunday ) 
, CONSTRAINT uix_weekday_display_name UNIQUE ( display_name ) 
, CONSTRAINT uix_weekday_abbreviation3 UNIQUE ( abbreviation3 ) 
, CONSTRAINT uix_weekday_abbreviation1 UNIQUE ( abbreviation1 ) 
--
, CONSTRAINT ck_weekday_days_after_sunday CHECK( days_after_sunday BETWEEN 0 AND 6 ) 
, CONSTRAINT ck_weekday_abbreviation3 CHECK( abbreviation3 = LEFT( upper(abbreviation3) || 'XXX' , 3 ) ) 
, CONSTRAINT ck_weekday_abbreviation1 CHECK( abbreviation1 = LEFT( upper(abbreviation1) || 'X' , 1 ) ) 
, CONSTRAINT excl_weekday_display_name_case_variation EXCLUDE ( lower(display_name) WITH = ) 
--
--
);

/* !! */ CALL utility.prc_generate_history_table( 'reference' , 'weekday' ); 

CREATE VIEW reference.vw_weekday AS 

SELECT  X.days_after_sunday 
,       X.display_name 
,       X.abbreviation3 
,       X.abbreviation1 
-- 
FROM  reference.weekday  AS  X 
-- 
;  

--
--

/***********************************************/

INSERT INTO reference.weekday 
( days_after_sunday , display_name , abbreviation3 , abbreviation1 )

VALUES 
  (  0 , 'Sunday'    , 'SUN' , 'U' ) 
, (  1 , 'Monday'    , 'MON' , 'M' ) 
, (  2 , 'Tuesday'   , 'TUE' , 'T' ) 
, (  3 , 'Wednesday' , 'WED' , 'W' ) 
, (  4 , 'Thursday'  , 'THU' , 'R' ) 
, (  5 , 'Friday'    , 'FRI' , 'F' ) 
, (  6 , 'Saturday'  , 'SAT' , 'S' ) 
--
; 

/***********************************************/

--
--

/*** month ***/ 

CREATE TABLE reference."month" (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, order_number int NOT NULL 
, display_name text NOT NULL 
, abbreviation3 text NOT NULL 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_month PRIMARY KEY ( pin ) 
, CONSTRAINT uix_month_order_number UNIQUE ( order_number ) 
, CONSTRAINT uix_month_display_name UNIQUE ( display_name ) 
, CONSTRAINT uix_month_abbreviation3 UNIQUE ( abbreviation3 ) 
--
, CONSTRAINT ck_month_order_number CHECK( order_number BETWEEN 1 AND 12 ) 
, CONSTRAINT ck_month_abbreviation3 CHECK( abbreviation3 = LEFT( upper(abbreviation3) || 'XXX' , 3 ) ) 
, CONSTRAINT excl_month_display_name_case_variation EXCLUDE ( lower(display_name) WITH = ) 
--
--
);

/* !! */ CALL utility.prc_generate_history_table( 'reference' , 'month' ); 

CREATE VIEW reference.vw_month AS 

SELECT  X.order_number 
,       X.display_name 
,       X.abbreviation3 
-- 
FROM  reference."month"  AS  X 
-- 
;  

--
--

/***********************************************/

INSERT INTO reference."month" 
( order_number , display_name , abbreviation3 )

VALUES 
  (  1 , 'January'   , 'JAN' ) 
, (  2 , 'February'  , 'FEB' ) 
, (  3 , 'March'     , 'MAR' ) 
, (  4 , 'April'     , 'APR' ) 
, (  5 , 'May'       , 'MAY' ) 
, (  6 , 'June'      , 'JUN' ) 
, (  7 , 'July'      , 'JUL' ) 
, (  8 , 'August'    , 'AUG' ) 
, (  9 , 'September' , 'SEP' ) 
, ( 10 , 'October'   , 'OCT' ) 
, ( 11 , 'November'  , 'NOV' ) 
, ( 12 , 'December'  , 'DEC' ) 
--
; 

/***********************************************/

--
--

/*** calendar_date ***/ 

CREATE TABLE reference.calendar_date (
  pin bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) 
-- 
--
, date_value date NOT NULL 
, year_number int NOT NULL 
, month_number int NOT NULL 
, day_number int NOT NULL 
, day_in_year int NOT NULL 
, days_after_sunday int NOT NULL 
--
--
, insert_time timestamp NOT NULL DEFAULT current_timestamp 
, insert_by text NOT NULL DEFAULT session_user 
, update_time timestamp NOT NULL DEFAULT current_timestamp 
, update_by text NOT NULL DEFAULT session_user 
--
-- 
, CONSTRAINT pk_calendar_date PRIMARY KEY ( pin ) 
, CONSTRAINT uix_calendar_date_date_value UNIQUE ( date_value ) 
, CONSTRAINT uix_calendar_date_numbers UNIQUE ( year_number , month_number , day_number ) 
, CONSTRAINT uix_calendar_date_day_in_year UNIQUE ( year_number , day_in_year ) 
--
, CONSTRAINT fk_calendar_date_month_number FOREIGN KEY ( month_number ) REFERENCES reference."month" ( order_number ) 
, CONSTRAINT fk_calendar_date_days_after_sunday FOREIGN KEY ( days_after_sunday ) REFERENCES reference.weekday ( days_after_sunday ) 
--
, CONSTRAINT ck_calendar_date_numbers CHECK( 
       date_part('year', date_value) = year_number 
   AND date_part('month', date_value) = month_number 
   AND date_part('day', date_value) = day_number 
 ) 
--
--
);

/* !! */ CALL utility.prc_generate_history_table( 'reference' , 'calendar_date' ); 

CREATE VIEW reference.vw_calendar_date AS 

SELECT  X.date_value 
,       X.year_number 
,       X.month_number 
,       M.display_name  AS  month_name 
,       M.abbreviation3  AS  month_abbreviation3 
,       X.day_number 
,       X.day_in_year 
,       X.days_after_sunday 
,       W.display_name  AS  weekday_name 
,       W.abbreviation3  AS  weekday_abbreviation3 
--
,    W.display_name || ', ' 
  || M.display_name || ' ' 
  || RIGHT('0' || X.day_number::text, 2) || ', ' 
  || X.year_number 
    AS  display_string 
-- 
FROM  reference.calendar_date  AS  X 
-- 
JOIN  reference."month"  AS  M  ON  X.month_number = M.order_number 
JOIN  reference.weekday  AS  W  ON  X.days_after_sunday = W.days_after_sunday 
-- 
;  

--
--

/*****/

COMMIT; 

/*****/
