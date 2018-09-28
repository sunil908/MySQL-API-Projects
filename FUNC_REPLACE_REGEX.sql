/*
FUNCTION: Accepts two parameters. 
Parameter 1: Any string of lenght upto 21K characters including UTF-8
Paramter  2: Regular expression that will evaluate what is characters to be removed

EXAMPLE CALL:
SELECT sakila.FUNC_REPLACE_REGEX('test#@@/_2211','^[a-zA-Z0-9_/]+$') cleansed
*/


DROP FUNCTION IF EXISTS FUNC_REPLACE_REGEX; 
DELIMITER | 
CREATE FUNCTION FUNC_REPLACE_REGEX( str VARCHAR(21000), REGEXSTR VARCHAR(255) ) RETURNS CHAR(255) DETERMINISTIC
BEGIN 
  DECLARE i, len SMALLINT DEFAULT 1; 
  DECLARE ret VARCHAR(21000) DEFAULT ''; 
  DECLARE c CHAR(1); 
  SET len = CHAR_LENGTH( str ); 
  REPEAT 
    BEGIN 
      SET c = MID( str, i, 1 ); 
      IF c REGEXP REGEXSTR THEN 
        SET ret=CONCAT(ret,c); 
      END IF; 
      SET i = i + 1; 
    END; 
  UNTIL i > len END REPEAT; 
  RETURN ret; 
END | 
DELIMITER ; 



