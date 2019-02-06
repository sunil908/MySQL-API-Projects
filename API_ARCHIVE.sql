DELIMITER $$

DROP PROCEDURE IF EXISTS archive_cdr $$
CREATE PROCEDURE archive_cdr(srcjson JSON, archivejson JSON) 
    BEGIN
            DECLARE sqlfrom VARCHAR(10000) DEFAULT '';
            DECLARE sqlwhere VARCHAR(10000) DEFAULT '1=1';
            DECLARE sqlarchive VARCHAR(10000);

            #loop counters
            DECLARE relcount int DEFAULT 0;
            DECLARE keycount int DEFAULT 0;


            DECLARE num_relation int DEFAULT 0;
            DECLARE jsontbl VARCHAR(1000);
            DECLARE jsonright_tbl VARCHAR(1000);
            DECLARE jsonleft_tbl VARCHAR(1000);
            DECLARE jsonright_keycol VARCHAR(1000);
            DECLARE jsonleft_keycol VARCHAR(1000);
            DECLARE num_keys INT DEFAULT 0;

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
					"operation": "delete",
                    "archlevel":"study",
					"sourceschema":"gdpr",
                    "archtabledetails": {"tablename":"orderdetails","keycolumn":["order","orderlinenumber"]}, 
					"masterkeydetails": {"keytable":"customers" , "keycolumn":"customernumber", "keyvalue":"181"}, 
					"tablerelationpath":[
										{ "righttable": "customers",
										  "rightkeys": ["customernumber"], 
										  "lefttable": "orders",
										  "leftkeys": ["customernumber"]
										},
										{ "righttable": "orders", 
										  "rightkeys": ["ordernumber"], 
										  "lefttable":  "orderdetails",
										  "leftkeys": ["orderNumber"]
										}    
								]
						}';
			
            SET archivejson = '{
								"schema":"gdpr",
								"tablename":"archive_orderdetails"
						}';
            
			*/
            #variable initializations
            SET num_relation = JSON_LENGTH(srcjson,'$.tablerelationpath');
            SET sqlquery = CONCAT(sqlquery,  srcjson->>'$.sourceschema', '.', json_unquote(json_extract(srcjson, '$.archtabledetails.tablename')),'.','*');
            SET sqldelete = CONCAT(sqldelete,  srcjson->>'$.sourceschema', '.', json_unquote(json_extract(srcjson, '$.archtabledetails.tablename')));
            SET sqlfrom = CONCAT(srcjson->>'$.sourceschema', '.',json_unquote(json_extract(srcjson, '$.archtabledetails.tablename')));
        
            # Prechecks
            #number of tables on right and left should be equal
            #number of key columns on right and left should be equal
            #mandatory - sourceschema , tablename, masterkeydetails.keytable, masterkeydetails.keycolumn, masterkeydetails.keyvalue
            

			# ITERATOR FOR - FROM clause buildup 
			WHILE  relcount < num_relation DO
					SET jsontbl=concat('$.tablerelationpath[', cast(relcount AS char) , '].righttable');
					SET sqlfrom = CONCAT(sqlfrom, CONST_COMMA,json_unquote(json_extract(srcjson, jsontbl)));
                    SET relcount = relcount + 1;
                    #logging
			END  WHILE;
            
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
            
            #logging
			INSERT INTO logtable select concat("sqlquery=",sqlquery );
			INSERT INTO logtable select concat("sqlfrom=",sqlfrom);
			INSERT INTO logtable select concat("sqlwhere=",sqlwhere);
			
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

		PREPARE deletestmt FROM @sqldelete;
		EXECUTE deletestmt;
        
        INSERT INTO logtable SELECT CONCAT('delete count=',ROW_COUNT());
        
		DEALLOCATE PREPARE selectstmt;
        DEALLOCATE PREPARE insertstmt;
COMMIT;
    END $$
DELIMITER ;

TRUNCATE TABLE logtable;
CALL archive_cdr ('
{
					"operation": "delete",
                    "archlevel":"study",
					"sourceschema":"gdpr",
                    "archtabledetails": {"tablename":"orders","keycolumn":["order","orderlinenumber"]}, 
					"masterkeydetails": {"keytable":"customers" , "keycolumn":"customernumber", "keyvalue":"181"}, 
					"tablerelationpath":[
										{ "righttable": "customers",
										  "rightkeys": ["customernumber"], 
										  "lefttable": "orders",
										  "leftkeys": ["customernumber"]
										}   
								]
						}
','{
								"schema":"gdpr",
								"tablename":"archive_orderdetails"
						}');
#SELECT * from logtable;








