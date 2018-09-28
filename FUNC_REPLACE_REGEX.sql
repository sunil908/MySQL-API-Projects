/* Function to cleanse a passed string with regular expression */
/* SELECT sakila.alphanum('test#@@/_2211','^[a-zA-Z0-9_/]+$') cleansed */
DROP FUNCTION IF EXISTS alphanum; 
DELIMITER | 
CREATE FUNCTION alphanum( str VARCHAR(500), REGEXSTR VARCHAR(255) ) RETURNS CHAR(255) DETERMINISTIC
BEGIN 
  DECLARE i, len SMALLINT DEFAULT 1; 
  DECLARE ret CHAR(255) DEFAULT ''; 
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


