# MongoDB_client

MongoDB Client with [connection string](https://www.mongodb.com/docs/manual/reference/connection-string/)

## Description
These tools are python client programs for `only retrieving` db records from mongodb and then store them to local csv file. 

## Requirement

- Python3 + PyMongo + Pandas 
- export TARGET_MONGODB=`connection string`

## Scripts

### list_databases_tables_counts.py
- listing database and table name from the target mongodb.

### test_1record_dump_records2csv.py *database_name* *table_name*
- using find_one({}) of pymongo instead of find() for checking the target data schema.

### dump_records2csv.py *database_name* *table_name*
- using find({}) i.e; download all records of the database/table
- fetch documents every 10000 records
- converting pandas df every document, so if documents are too much, you feels slow.

### query_records2csv.py *database_name* *table_name* *query* *timefield*
- using find(*query*)
- Argument examples (timestamp is the timestamp field in this case, status and portnum are just examples.)
- - query_records2csv.py *database_name* *table_name* '{"$and":[{"status":True},{"portnum":80}]}'
- - query_records2csv.py *database_name* *table_name* '{"timestamp":{"$gt":1677633240}}' timestamp
- - query_records2csv.py *database_name* *table_name* '{"timestamp":{"$gt":1677633240,"$lte":1679968394}}' timestamp
- - query_records2csv.py *database_name* *table_name* '{"$and":[{"timestamp":{"$gt":1677633240}},{"portnum":80}]}' timestamp
- Only epoch time is available as timestamp value to this script, it's converted to datetime obj inside this script.
- Timezone is not considered in the conversion, it will follow the executing system timezone.

**NOTE: scripts write the connection string to stdout by `print("# connecting to %s\n"%target_mongo)` line**

**Please comment out if you don't prefer this in terms of security like your mongodb's user/pass credential showing**

## Test Environment
1. OS: Ubuntu 16.04
- Python 3.5.2
- pymongo-3.13.0, dnspython-1.16.0
- numpy-1.18.5 pandas-0.24.2 python-dateutil-2.8.2 pytz-2023.2
2. OS: macOS 12.2
- Python 3.8.8
- pymongo-4.3.3, dnspython-2.3.0
- numpy-1.24.2 pandas-1.5.3 python-dateutil-2.8.2 pytz-2023.2 six-1.16.0

## Altenative method
MongoDB's copy functionality
- mongodump
- mongorestore
