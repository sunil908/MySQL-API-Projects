/*
Description : 
=============
This procedure runs the complete algorithm for archiving on a given schema with data model defined in rel_ex. It takes care of the
table and forming the relationship based on the path.

Depending implementation: 
==========================
Finding the topology order of all the nodes (tables) in the schema for which archiving is to be performed. Workaround is a column 
topology order given in table rel_ex that is manually fed currently.

Error handling. There needs number of custom error raised in the current code.

Prerequisite data requirements:
================================
rel_ex : Table contaniing Relationship Data CREATE TABLE rel_ex (ReferenceID, Tablename, ColumnName, ReferencedTableName, 
ReferencedColumnName, TopologyOrderNum ). You can alternatively build a table view with the foreign and reference key using 
system table in Mysql.

log : Create a log table for tracing the operations and validatity. You can also comment all the insert into log table if you wish. 
CREATE TABLE log (message varchar(10000));

archive schema: It is expected that archive schema and tables already exist. This parameter is passed and lo

Module dependencies
===================

This contains calls to other procedures:

[Procedure 1]: dijResolve - Resolves the path between any two table (nodes) with defined reference relationship. This uses 
dijkstra shortest route algorithm to resolve the path.

[Procedure 2]: archive_table - archives a table and target records between the master table and target archive table

Example call:  
=============
Use like order_sales (classicmodels) schema from mysql sample database. It can be downloaded easily. 
call archive_schema('','customers','customernumber','181','gdpr_archive');
SELECT * from logtable;
*/

DELIMITER $$
DROP PROCEDURE IF EXISTS archive_schema;
CREATE PROCEDURE archive_schema(
								varchivelevel varchar(100), 
                                vmastertablename varchar(100),
                                vmasterkeycolumnname varchar(100), 
                                masterkeyvalue varchar(200),
                                varchiveschema varchar(200)
                                ) 
    BEGIN
			DECLARE vschema , vnodename, varchiveprefix VARCHAR(200);
            DECLARE vtableproperties, varchiveproperties, vtablelist, vrelationcond varchar(10000);
			DECLARE tablecount , vnodeid, num_tables INTEGER;
			DECLARE vfinished INTEGER DEFAULT 0;
            
            # Table listing from node table
            DECLARE cursor_tablelist CURSOR FOR SELECT Nodename, tableorder from dijnodes order by tableorder DESC;
			
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET vfinished = 1;
           
			SELECT database() into vschema;
			
            DROP TABLE IF EXISTS dijnodes,dijpaths, map2; 
			CREATE TABLE dijnodes ( 
			  NodeID int PRIMARY KEY AUTO_INCREMENT NOT NULL, 
			  Nodename varchar (20) NOT NULL, 
			  Cost int NULL, 
			  PathID int NULL, 
			  Calculated tinyint NULL ,
              studyroot tinyint NULL DEFAULT 0,
              tableorder int NULL
			); 
  
			CREATE TABLE dijpaths ( 
			  PathID int PRIMARY KEY AUTO_INCREMENT, 
			  FromNodeID int NOT NULL , 
			  ToNodeID int NOT NULL , 
			  Cost int NOT NULL,
			  RelationReferenceID int NULL
			);
			
            CREATE TEMPORARY TABLE map2 ( 
				RowID INT PRIMARY KEY AUTO_INCREMENT, 
				FromRouteNodeID INT, 
				FromNodeName VARCHAR(20), 
				ToRouteNodeID INT,
				ToNodeName VARCHAR(20), 
				Cost INT ,
				RelationReferenceID INT
			) ENGINE=MEMORY; 
            
            
            SET SQL_SAFE_UPDATES = 0;
			SET group_concat_max_len = 10000;
			SET varchiveprefix = '';
            
			INSERT INTO dijnodes(Nodename) SELECT tablename from (SELECT TableName from rel_ex  UNION select ReferredTableName as tablename from rel_ex) tablist;
            

			UPDATE dijnodes SET studyroot=1 WHERE Nodename=vmastertablename;
            # Update the topology order for traversing through all the tables
            UPDATE
				dijnodes AS nodes, 
				(SELECT b.Nodename, COALESCE(a.TopologyOrderNum,0) TopologyOrderNum from dijnodes b LEFT JOIN rel_ex a ON a.ReferredTableName=b.Nodename) AS topology
				SET nodes.tableorder = topology.TopologyOrderNum
				WHERE nodes.Nodename = topology.Nodename;
			
            
		   INSERT INTO dijpaths(FromNodeID, ToNodeID, Cost, RelationReferenceID ) SELECT b1.NodeID, b2.NodeID, 1, ReferenceID 
																	from rel_ex a, dijnodes b1 , dijnodes b2
                                                                    where b1.Nodename=a.tablename
                                                                    AND b2.Nodename=a.ReferredTableName;
            
			
			
            /* target archive tables */
            #select * from dijnodes;
            /* relationship path */
            #select * from map;
            #INSERT INTO relationmap select * from map;
            

            
            OPEN cursor_tablelist ;
			
            readnodes_loop: LOOP
				FETCH cursor_tablelist INTO vnodename, vnodeid;
                
				IF vfinished = 1 THEN 
                    LEAVE readnodes_loop;
				END IF;
				
				SET vtableproperties = '{}';
				SET varchiveproperties = '{}';        
                SET vrelationcond='';
                SET vtablelist='';
                
                SET SQL_SAFE_UPDATES = 0;

                TRUNCATE TABLE map2;
                TRUNCATE TABLE map;
                
                
				CALL dijResolve(vmastertablename,vnodename);
                
                
				IF  (vmastertablename<>vnodename AND NOT EXISTS (select * from map)) THEN
							INSERT INTO logtable SELECT CONCAT('Warning: Path doesnt exists for From=',vmastertablename,' To=',vnodename);
                            ITERATE readnodes_loop;
				END IF;
                
                
                INSERT INTO map2 SELECT * FROM map;
                
                SELECT JSON_INSERT(vtableproperties, '$.sourceschema', vschema) INTO vtableproperties;
                
                SELECT JSON_INSERT(vtableproperties, '$.archtabledetails', CAST(CONCAT('{"tablename":', JSON_QUOTE(vnodename),'}') AS JSON) ) INTO vtableproperties;
				SELECT JSON_INSERT(vtableproperties, '$.masterkeydetails', 
					 cast( 
							CONCAT('{"keytable":',JSON_QUOTE(vmastertablename) , ',"keycolumn":', JSON_QUOTE(vmasterkeycolumnname), ',"keyvalue":', JSON_QUOTE(masterkeyvalue),'}')
					  AS JSON)
                ) INTO vtableproperties;
                
                /* get the table path within the route */
                select GROUP_CONCAT(
						CONCAT(' AND ',b.tablename,'.',b.columnname, '=',b.ReferredTableName, '.',b.ReferredColumnName)
						SEPARATOR '') into vrelationcond
					from 
						map a,
						rel_ex b
					where a.RelationReferenceID=b.ReferenceID
					GROUP BY 'all';
                
                SELECT JSON_INSERT(vtableproperties, '$.tablerelationpath', vrelationcond) INTO vtableproperties;
                
				/* get the table list within the route */
                select GROUP_CONCAT(DISTINCT tlist.tablename  SEPARATOR ', ') into vtablelist
						from (
						select FromNodeName tablename from 
							map
						 UNION 
						select ToNodeName tablename from 
							map2
						UNION
                        select vmastertablename tablename
							) tlist
						group by 'all';
                
                SELECT JSON_INSERT(vtableproperties, '$.tablelist', vtablelist) INTO vtableproperties;
                

                
                SELECT JSON_INSERT(varchiveproperties, '$.schema', varchiveschema)  INTO varchiveproperties;
                SELECT JSON_INSERT(varchiveproperties, '$.tablename',concat(varchiveprefix,vnodename))  INTO varchiveproperties;
                
                INSERT INTO logtable SELECT CONCAT('archive_json=',varchiveproperties);
                
                CALL archive_table(vtableproperties,varchiveproperties);
                
            END LOOP readnodes_loop;
            CLOSE cursor_tablelist;

            END $$
			DELIMITER ;
    END $$
    DELIMITER ;
