# ARCHIVING API

MAIN PROCEDURE CALL: archive_schema

PURPOSE: API to archive desired records across all of the schema to target archive schema. It requires details of the master or seed table with key column and extraction condition on this table. The API will select the records across the schema using relationship built within relationship constraint (to be provided by caller) to target archive schema (archjson). This expects a formatted JSON as described below to move the data from source to target archive table. 

It uses dijkstra's shortest route algorithm to travese across all tables and archive the relationship records in cascading fashion. It takes care of the topology order to make sure child records are archived before any parent is archived.

PARAMETERS: srcjson, archjson

	srcjson:    -> Accepts json as string
			    -> all processing is limited to length of 100 char for table names and field names 
			    -> array size within JSON structure can be extended idefinitely but only limited by database SQL 
			  	 IN clause. It also will be limited by the 32000 character processing of API.
			    -> Maximum limit of the string is limited to 32000 characters
			    -> structure is described in later section
			    -> value to be passed with single quote escape string. This is applicable for filter values where applicable. For ex. "''testvalue''" = 'testvalue'
	
srcjson_structure: {
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

archjson:  
	   -> Accepts json as string
	   -> Maximum limit of the string is limited to 32000 characters

Example Call:

call archive_schema('','customers','customernumber','181','gdpr_archive');

