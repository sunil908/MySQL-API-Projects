DELIMITER $$
DROP PROCEDURE IF EXISTS archive_cdr;
CREATE PROCEDURE archive_cdr(srcjson JSON, archivejson JSON) 
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
            DECLARE sqlquery VARCHAR(10000)  DEFAULT ' SELECT ';
            DECLARE sqlinsert VARCHAR(10000);
            DECLARE sqldelete VARCHAR(10000) DEFAULT ' DELETE ';

			#constant literals for better reading	
            DECLARE CONST_COMMA VARCHAR(1)  DEFAULT ',';
            # Use VARCHAR for space character else truncates spaces on assignment on left and right
            DECLARE CONST_AND_CLAUSE VARCHAR(5)  DEFAULT ' AND ';
            DECLARE CONST_SPACE VARCHAR(1) DEFAULT ' ';
            
			#session configurations
            SET autocommit = OFF;
			
			/*		
            # sample json parameters. In case string value please provide single quote within the json value pair
            SET srcjson = '{
					"operation": "archive",
                    "archivefamily":"study",
					"sourceschema":"gdpr",
                    "archtabledetails": { "keycolumn":["order","orderlinenumber"],
										  "tablename":"orderdetails",
										  "filtercond": {
															"productcode":[18,23],
															"quantityOrdered":[39,41]
														}            
										}, 
					"masterkeydetails": {"keytable":"orderdetails" , "keycolumn":"ordernumber", "keyvalue":"10100"}, 
					"tablerelationpath":[]
						}';
			
            SET archivejson = '{
								"schema":"gdpr",
								"tablename":"archive_orderdetails"
						}';
			*/		
			
            #variable initializations
            SET num_relation=JSON_LENGTH(srcjson,'$.tablerelationpath');
            SET num_filter= JSON_LENGTH(srcjson,'$.archtabledetails.filtercond');
            SET sqlquery = CONCAT(sqlquery,  srcjson->>'$.sourceschema', '.', json_unquote(json_extract(srcjson, '$.archtabledetails.tablename')),'.','*');
            SET sqldelete = CONCAT(sqldelete,  srcjson->>'$.sourceschema', '.', json_unquote(json_extract(srcjson, '$.archtabledetails.tablename')));
            SET sqlfrom = CONCAT(srcjson->>'$.sourceschema', '.',json_unquote(json_extract(srcjson, '$.archtabledetails.tablename')));
        
            # Prechecks
            #number of tables on right and left should be equal
            #number of key columns on right and left should be equal
            #mandatory - sourceschema , tablename, masterkeydetails.keytable, masterkeydetails.keycolumn, masterkeydetails.keyvalue
			
			DROP TEMPORARY TABLE IF EXISTS tablelist;
            CREATE TEMPORARY TABLE IF NOT EXISTS tablelist
            SELECT  CAST('X' AS CHAR(10000)) AS tablename LIMIT 0;
			
            INSERT INTO tablelist SELECT CONCAT(srcjson->>'$.sourceschema', '.',json_unquote(json_extract(srcjson, '$.archtabledetails.tablename'))) AS tablename ;
                        
            SET relcount=0;
            # ITERATOR FOR - FROM clause buildup 
			WHILE  relcount < num_relation DO
					SET jsontbl=concat('$.tablerelationpath[', cast(relcount AS char) , '].righttable');
					#SET sqlfrom = CONCAT(sqlfrom, CONST_COMMA,json_unquote(json_extract(srcjson, jsontbl)));
                    INSERT INTO tablelist SELECT concat(srcjson->>'$.sourceschema', '.',json_unquote(json_extract(srcjson, jsontbl))) AS tablename;
                    SET jsontbl=concat('$.tablerelationpath[', cast(relcount AS char) , '].lefttable');
                    INSERT INTO tablelist SELECT concat(srcjson->>'$.sourceschema', '.',json_unquote(json_extract(srcjson, jsontbl))) AS tablename;
                    SET relcount = relcount + 1;
                    #logging
			END  WHILE;
            
            #Create the FROM clause
            SELECT GROUP_CONCAT(distinct tablename) INTO sqlfrom  FROM tablelist;
            
			
            SET relcount=0;
            SET keycount=0;
            # Building the WHERE clause in the below loop
			WHILE  relcount < num_relation DO
				SET jsonright_tbl=concat('$.tablerelationpath[', cast(relcount AS char) , '].righttable');
                SET jsonleft_tbl=concat('$.tablerelationpath[', cast(relcount AS char) , '].lefttable');
                SET num_keys=JSON_LENGTH(srcjson, '$.tablerelationpath[0].rightkeys');
					WHILE  keycount < num_keys DO # key relations loop starts
						SET jsonright_keycol=concat('$.tablerelationpath[', cast(relcount AS char) , '].rightkeys[',cast(keycount AS char),']');
						SET jsonleft_keycol=concat('$.tablerelationpath[', cast(relcount AS char) , '].leftkeys[',cast(keycount AS char),']');
						SET sqlwhere = CONCAT(sqlwhere, CONST_AND_CLAUSE, 
												json_unquote(json_extract(srcjson,jsonright_tbl)),'.',
												json_unquote(json_extract(srcjson,jsonright_keycol)),
												"=",
												json_unquote(json_extract(srcjson,jsonleft_tbl)),'.',
												json_unquote(json_extract(srcjson,jsonleft_keycol))
											 );
						SET keycount = keycount + 1;
					 END  WHILE; #key relations loop ends
                     SET keycount=0;
                     SET relcount = relcount + 1;
			END  WHILE;
            
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
			#NSERT INTO logtable select concat("sqlwhere=",sqlwhere);
			
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

TRUNCATE TABLE logtable;
# Example 2: Relationship with two table relationships
CALL archive_cdr (
'{
					"operation": "delete",
                    "archlevel":"study",
					"sourceschema":"gdpr",
                    "archtabledetails": {"tablename":"orderdetails","keycolumn":["order","orderlinenumber"]}, 
					"masterkeydetails": {"keytable":"customers" , "keycolumn":"customernumber", "keyvalue":"181"}, 
					"tablerelationpath":[
										{ "righttable": "orders",
										  "rightkeys": ["customernumber"], 
										  "lefttable": "customers",
										  "leftkeys": ["customernumber"]
										},
										{ "righttable": "orders", 
										  "rightkeys": ["ordernumber"], 
										  "lefttable":  "orderdetails",
										  "leftkeys": ["orderNumber"]
										}    
								]
						}','{
								"schema":"gdpr",
								"tablename":"archive_orderdetails"
						}');

SELECT * from logtable;


# Example 1: No relationship with single table values
CALL archive_cdr ('{
					"operation": "archive",
                    "archivefamily":"study",
					"sourceschema":"gdpr",
                    "archtabledetails": { "keycolumn":["order","orderlinenumber"],
										  "tablename":"orderdetails",
										  "filtercond": {
															"productcode":[18,23],
															"quantityOrdered":[39,41]
														}            
										}, 
					"masterkeydetails": {"keytable":"orderdetails" , "keycolumn":"ordernumber", "keyvalue":"10100"}, 
					"tablerelationpath":[]
						}','{
								"schema":"gdpr",
								"tablename":"archive_orderdetails"
						}');
SELECT * from logtable;
