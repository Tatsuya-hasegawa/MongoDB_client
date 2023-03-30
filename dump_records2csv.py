import sys
from os import environ as os_environ
import pprint,traceback
from pymongo import MongoClient, version as pymongo_version 
from datetime import datetime
import pandas as pd
import ast

WINDOW_SIZE = 10000

def dump2csv(filename,records_json,counts):
	print("# using pandas version %s"%pd.__version__)
	pandas_major,pandas_minor,pandas_micro = pd.__version__.split(".")
	try:
		if int(pandas_major) >= 1:
			df = pd.json_normalize(records_json)
		else:
			df = pd.io.json.json_normalize(records_json)
		df.to_csv(filename, index=False, encoding='utf-8')
		print("# Saved to %s"%filename)
	except Exception as e:
		print("E|<%s> by %s"%(filename, traceback.format_exc()))
		return

	file_rows = sum([1 for _ in open(filename)])
	print("# file row num: %d"%file_rows)
	if counts == file_rows-1:
		print("INTEGRITEY CHECK: SUCCESS !")
	else:
		print("INTEGRITEY CHECK: FAILURE  (%d - %d = %d)"%(counts,file_rows-1,counts-file_rows-1))

def fetch_mongo_records(mongodb_url,database_name,table_name):
	with MongoClient(mongodb_url) as client:
		assert client is not None
		print("# %s\n"%client)
		print("# using pymongo version: %s"%pymongo_version)
		pymongo_major,pymongo_minor,pymongo_micro = pymongo_version.split(".")		
		records_json = []
		try:
			db = client[database_name]
			collection = db[table_name]
			try: 
				counts = collection.estimated_document_count({}) 
			except:
				try: 
					counts = collection.estimated_document_count() # e.g. 3.13.0
				except: 
					counts = collection.count_documents({}) # unexpected version
			else:
				counts = collection.count_documents({})
			print("[%s](%s) %drecords"%(database_name,table_name,counts))

			# faster fetch way from belows
			# https://stackoverflow.com/questions/38647962/how-to-quickly-fetch-all-documents-mongodb-pymongo
			# https://stackoverflow.com/questions/52904815/using-natural-sort-in-pymongo-mongodb
			# without limit [ records_json.append(record) for record in collection.find({},cursor_type=CursorType.EXHAUST).sort([( '$natural', 1 )]) ]
			position = 0
			while position < counts:
				# E| Can't use limit and exhaust together.
				[ records_json.append(record) for record in collection.find({}).sort([( '$natural', 1 )]).skip(position).limit(WINDOW_SIZE) ]
				print("# [%s] position in progress: %d, stored record counts = %d"%(datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ'),position,len(records_json)))
				sys.stdout.flush()
				position += WINDOW_SIZE


			# dump to csv file
			print("\n# CSV dump phaise")
			timestr = datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ')
			filename = database_name + "_" + table_name + "_" + str(counts) + "records_" + timestr + ".csv"
			dump2csv(filename,records_json,counts)

		except Exception as e:
			print("E| %s on [%s](%s)"%(str(e),database_name,table_name))

if __name__ == '__main__':
	target_mongo = os_environ["TARGET_MONGODB"] # connection string ref: https://www.mongodb.com/docs/manual/reference/connection-string/
	print("# connecting to %s\n"%target_mongo)
	database_name = sys.argv[1]
	table_name = sys.argv[2]
	fetch_mongo_records(target_mongo,database_name,table_name)
	print("\n# mongodb session closed.")
