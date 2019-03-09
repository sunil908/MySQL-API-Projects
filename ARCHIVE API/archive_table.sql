DELIMITER $$
DROP PROCEDURE IF EXISTS archive_table;
CREATE PROCEDURE archive_table(srcjson JSON, archivejson JSON) 
    BEGIN
            DECLARE sqlfrom VARCHAR(10000) DEFAULT '';
            DECLARE sqlwhere VARCHAR(10000) DEFAULT '1=1';
            DECLARE sqlarchive VARCHAR(10000);
            DECLARE filtervalues VARCHAR(10000);
            DECLARE filtercolumns VARCHAR(10000);
            DECLARE filtercond VARCHAR(10000) DEFAULT ' ';

            #loop counters
            DECLARE relcount int DEFAULT 0;
            DECLARE keycount int DEFAULT 0;
			DECLARE filtercount int DEFAULT 0;

            DECLARE num_relation int DEFAULT 0;
            DECLARE jsontbl VARCHAR(1000);
            DECLARE jsonright_tbl VARCHAR(1000);
            DECLARE jsonleft_tbl VARCHAR(1000);
            DECLARE jsonright_keycol VARCHAR(1000);
            DECLARE jsonleft_keycol VARCHAR(1000);
            DECLARE num_keys INT DEFAULT 0;
			DECLARE num_filter INT DEFAULT 0;
            
			#final statements for execution
            DECLARE sqlquery VARCHAR(10000)  DEFAULT ' SELECT DISTINCT ';
            DECLARE sqlinsert VARCHAR(10000);
            DECLARE sqldelete VARCHAR(10000) DEFAULT ' DELETE ';

			#constant literals for better reading	
            DECLARE CONST_COMMA VARCHAR(1)  DEFAULT ',';
            # Use VARCHAR for space character else truncates spaces on assignment on left and right
            DECLARE CONST_AND_CLAUSE VARCHAR(5)  DEFAULT ' AND ';
            DECLARE CONST_SPACE VARCHAR(1) DEFAULT ' ';
            
			#session configurations
            SET autocommit = OFF;

            #variable initializations
            SET num_relation=JSON_LENGTH(srcjson,'$.tablerelationpath');
            SET num_filter= JSON_LENGTH(srcjson,'$.archtabledetails.filtercond');
            SET sqlquery = CONCAT(sqlquery,  srcjson->>'$.sourceschema', '.', json_unquote(json_extract(srcjson, '$.archtabledetails.tablename')),'.','*');
            SET sqldelete = CONCAT(sqldelete,  srcjson->>'$.sourceschema', '.', json_unquote(json_extract(srcjson, '$.archtabledetails.tablename')));
            #SET sqlfrom = CONCAT(srcjson->>'$.sourceschema', '.',json_unquote(json_extract(srcjson, '$.archtabledetails.tablename')));
        
            # Prechecks
            #number of tables on right and left should be equal
            #number of key columns on right and left should be equal
            #mandatory - sourceschema , tablename, masterkeydetails.keytable, masterkeydetails.keycolumn, masterkeydetails.keyvalue
			
			/*
            DROP TEMPORARY TABLE IF EXISTS tablelist;
            CREATE TEMPORARY TABLE IF NOT EXISTS tablelist
            SELECT  CAST('X' AS CHAR(10000)) AS tablename LIMIT 0;
			
            INSERT INTO tablelist SELECT CONCAT(srcjson->>'$.sourceschema', '.',json_unquote(json_extract(srcjson, '$.archtabledetails.tablename'))) AS tablename ;
			*/	
            #Assign FROM clause
            SELECT json_unquote(json_extract(srcjson,'$.tablelist')) INTO sqlfrom;
            #Assign FROM relationship route
            SELECT concat(sqlwhere,json_unquote(json_extract(srcjson,'$.tablerelationpath'))) INTO sqlwhere;
			
            # Adding the masterkey condition for selecting the table records to be archived
            SET sqlwhere = CONCAT(sqlwhere, CONST_AND_CLAUSE,
							json_unquote(json_extract(srcjson, '$.masterkeydetails.keytable')), '.',
                            json_unquote(json_extract(srcjson, '$.masterkeydetails.keycolumn')), '=',
                            json_unquote(json_extract(srcjson, '$.masterkeydetails.keyvalue'))
						);
			
             #Building the filer condition from json
            WHILE  filtercount < num_filter DO
				SET filtercolumns=
								
                                JSON_UNQUOTE(
                                JSON_EXTRACT(
											JSON_KEYS(srcjson,'$.archtabledetails.filtercond')
										   ,concat('$[',filtercount,']')
								)
                                );
				SET filtervalues=     
						JSON_EXTRACT(srcjson
						,concat(
								'$.archtabledetails.filtercond.',
								JSON_EXTRACT(JSON_KEYS(srcjson,'$.archtabledetails.filtercond'),concat('$[',filtercount,']'))
								)
							);
                SET filtercond = concat(filtercond, CONST_AND_CLAUSE, filtercolumns,' IN (', filtervalues,')');
                SET filtercount = filtercount + 1;
			END WHILE;
            
            
            #logging
            #INSERT INTO logtable select concat("filtervalues=",filtervalues);
            #INSERT INTO logtable select concat("filtercolumns=",filtercolumns);
            #INSERT INTO logtable select concat("filtercond=",filtercond);
			#INSERT INTO logtable select concat("sqlquery=",sqlquery );
			#INSERT INTO logtable select concat("sqlfrom=",sqlfrom);
			#INSERT INTO logtable select concat("sqlwhere=",sqlwhere);
			
            SET @sqlquery= CONCAT(sqlquery,
                                        CONST_SPACE,' FROM ',sqlfrom,
                                        CONST_SPACE,' WHERE ',sqlwhere,
                                        ' FOR UPDATE '
                                        );
			SET @sqlinsert = 	CONCAT( ' INSERT INTO ', archivejson->>'$.schema', '.', archivejson->>'$.tablename',
										CONST_SPACE,
                                        @sqlquery
                                        );
			SET @sqldelete = 	CONCAT( sqldelete,
										 CONST_SPACE,' FROM ',sqlfrom,
                                         CONST_SPACE,' WHERE ',sqlwhere
                                        );        

        
        INSERT INTO logtable SELECT CONCAT('sqlquery=',@sqlquery);
        INSERT INTO logtable SELECT CONCAT('sqlinsert=',@sqlinsert);
        INSERT INTO logtable SELECT CONCAT('sqldelete=',@sqldelete);
        
START TRANSACTION;       
		
		PREPARE selectstmt FROM @sqlquery; 
		EXECUTE selectstmt; 
			
		PREPARE insertstmt FROM @sqlinsert;
		EXECUTE insertstmt; 
        
        INSERT INTO logtable SELECT CONCAT('insert count=',ROW_COUNT());

		SET SQL_SAFE_UPDATES = 0;       
        
        PREPARE deletestmt FROM @sqldelete;
		EXECUTE deletestmt;
        
        INSERT INTO logtable SELECT CONCAT('delete count (seems to be an issue right now)=',ROW_COUNT());
        
        SET SQL_SAFE_UPDATES = 1;       
        
		DEALLOCATE PREPARE selectstmt;
        DEALLOCATE PREPARE insertstmt;
        DEALLOCATE PREPARE deletestmt;
COMMIT;
    END $$
    DELIMITER ;