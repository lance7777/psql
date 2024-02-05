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
, CONSTRAINT uix_alphabet_letter_code_name UNIQUE ( order_number ) 
, CONSTRAINT uix_alphabet_letter_uppercase UNIQUE ( uppercase ) 
, CONSTRAINT uix_alphabet_letter_lowercase UNIQUE ( lowercase ) 
--
, CONSTRAINT ck_alphabet_letter_order_number CHECK( order_number BETWEEN 1 AND 26 ) 
, CONSTRAINT ck_alphabet_letter_uppercase CHECK( uppercase = LEFT( upper(uppercase) , 1 ) ) 
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

/*****/

COMMIT; 

/*****/
