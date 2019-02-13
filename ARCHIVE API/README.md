# ARCHIVING API

PROCEDURE NAME: archive_cdr
PURPOSE: API archive the desired table in the schema to target archive schema and table. It requires details of the tables and extraction conditions (srcjson) 
		 based on which it will select the records to be archived to target schema (archjson). This expects a formatted JSON as described below to move the data 
		 from source to target archive table. It will work upon various attributes like described below. The main filters driving the selection of records are 
		 stored in two JSON attributes namely filtercond and keyvalue.

PARAMETERS: srcjson, archjson

	srcjson:    -> Accepts json as string
			    -> all processing is limited to length of 100 char for table names and field names 
			    -> array size within JSON structure can be extended idefinitely but only limited by database SQL 
			  	 IN clause. It also will be limited by the 32000 character processing of API.
			    -> Maximum limit of the string is limited to 32000 characters
			    -> structure is described in later section
			    -> value to be passed with single quote escape string. This is applicable for filter values where applicable. For ex. "''testvalue''" = 'testvalue'
	Example srcjson:
				'{
					"operation": "delete",
                    "archlevel":"study",
					"sourceschema":"gdpr",
                    "archtabledetails": {"tablename":"orderdetails",
					 "keycolumn":["order","orderlinenumber"]
					 "filtercond": {
								"productcode":[18,23],
								"quantityOrdered":[39,41]
							} 
					}, 
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
						}'

srcjson_structure: {
			"operation": 	type= string, 
					mandatory=yes, 
					default=archive, 
					possible_val = delete,archive
					comment='currently not implemented. describes operation to be performed'
			"archlevel": 	type= string, 
					mandatory=yes, 
					default=none, 
					possible_val=study, site,...
					comment='currently not implemented. describes level at which the archiving happens'
			"sourceschema": type= string, 
					mandatory=yes, 
					default=none, 
					possible_val = any_database_schema
					comment='gives the source schema where the table exists for archiving'
	 	   "archtabledetails": 	type= json, 
					mandatory=yes,
					default=none,
					possible_val = json_formatted
					comment='details of the table that needs to be archived'
  	"archtabledetails.tablename": 	type= string, 
					mandatory=yes,
					default=none,
					possible_val = any_tablename
					comment='target table that requires archiving'
  	"archtabledetails.keycolumn":   type= array string, 
					mandatory=yes,
					default=none,
					possible_val = any_key_fieldnames
					comment='defines keys to be used for deleting the records from archive table. The records will be will marked post applying of filters and relation paths.'
	"archtabledetails.filtercond":  type= json, 
					mandatory=no,
					default=none,
					possible_val = {"column_name1": [array values], "column_name2": [array values], ...}
					comment='you may select records based on multiple filters from the archive table. AND 
										 clause is applied across the filters defined'
		"masterkeydetails": 	type= json, 
					mandatory=yes,
					default=none,
					possible_val = {"keytable": "table_name", "keycolumn": "column_name","keyvalue":"value" }
					comment='provides the master reference details that will be filtered to arrive at the records to 
				 be archived. This table mentioned will have to be part of the table path from the archive table'
		"tablerelationpath":	type= json array, 
					mandatory=no,
					default=none,
					possible_val = {  "righttable": "table_name",
									   "rightkeys": ["column_name1","column_name2",...], 
									   "lefttable": "table_name",
									   "leftkeys": ["column_name1","column_name2",...], 
									}
					 comment='provide the relationship path between archive table to master table. It can be empty if the archive table and master reference table is same. For ex. archive table='SUBJECT' , master table = 'STUDY'. Then provide relationship path between SUBJECT table to the STUDY table for archiving the target records in SUBJECT.'
							}

archjson:  
	   -> Accepts json as string
	   -> Maximum limit of the string is limited to 32000 characters

Example: 

archjson:      {
				"schema":"gdpr",
				"tablename":"archive_orderdetails"
				}
archjson_structure: {
		"schema": 	type= string, 
				mandatory=yes, 
				default=none, 
				possible_val = any_database_schema
				comment='gives the target archive schema where the table exists for archiving'
		"tablename":	type= string, 
				mandatory=yes,
				default=none,
				possible_val = json_formatted
				comment='details of the table that will be used for archiving the records. This table should already 
				exist'
		    }

