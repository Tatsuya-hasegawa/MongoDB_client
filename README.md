# MongoDB_client

MongoDB Client with [connection string](https://www.mongodb.com/docs/manual/reference/connection-string/)

## Description
This tools are python client for downloading records from mongodb to local csv file. 

## Requirement

- Python3 + PyMongo + Pandas 
- export TARGET_MONGODB=`connection string`

## Scripts

- list_databases_tables_counts.py
- - listing database and table name from the target mongodb.
- test_1record_dump_records2csv.py *database_name* *table_name*
- - using find_one({}) of pymongo instead of find() for checking the target data schema.
- dump_records2csv.py *database_name* *table_name*
- - using find({}) i.e; download all records of the database/table
- - fetch documents every 10000 records
- - converting pandas df every document, so if documents are too much, you feels slow.

## (FYI) Test Environment
1. Python 3.5.2
2. pymongo-3.13.0, dnspython-1.16.0
3. numpy-1.18.5 pandas-0.24.2 python-dateutil-2.8.2 pytz-2023.2

## Altenative method
MongoDB's copy functionality
- mongodump
- mongorestore
