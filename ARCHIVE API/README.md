# ARCHIVING API

MAIN PROCEDURE CALL: archive_schema
PURPOSE: API to archive desired records across all of the schema to target archive schema. It requires details of the table and extraction conditions based on which it will select the records to be archived to target schema (archjson). This expects a formatted JSON as described below to move the data from source to target archive table. 

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

"srcjson_structure: {
			"operation": 	type= string,
					mandatory=yes,
					default=archive,
					possible_val = delete,archive
					comment='currently not implemented. describes operation to be performed'
			"mastertable":  type= string, 
					mandatory=yes, 
					default=none, 
					possible_val = any_database_table
					comment='it is the table that contains the master record which is seeding point for archiving'
		"masterkeycolumn": 	type= string, 
					mandatory=yes,
					default=none,
					possible_val = column_from_mastertable
					comment='provides the master reference column used for filtering to arrive at the records.
		"masterkeyval": 	type= string, 
					mandatory=yes,
					default=none,
					possible_val = value_from_masterkeycolumn
					comment='provides the master reference value used for filtering to arrive at the archive records.
			}
"
archjson:  
	   -> Accepts json as string
	   -> Maximum limit of the string is limited to 32000 characters

Example Call:

call archive_schema('','customers','customernumber','181','gdpr_archive');

