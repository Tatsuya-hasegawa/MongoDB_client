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
	try:
		if float(pd.__version__.replace(".","")) >= 100:
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


def adjust_query(query,timefield):
	query_json = ast.literal_eval(query)

	if timefield:
		if "$and" in query_json.keys() or "$or" in query_json.keys():
			try:
				for complex_name in query_json:
					if complex_name=="$and" or complex_name=="$or":
						i = 0
						while i < len(query_json[complex_name]):
							if timefield in query_json[complex_name][i]:
								for condition,value_epoc in query_json[complex_name][i][timefield].items():
									if condition in ("$gt","$lt","$gte","$lte"):
										epoch_time = None
										epoch_time = value_epoc
										query_json[complex_name][i][timefield][condition] = datetime.fromtimestamp(value_epoc)
							i+=1
			except Exception as e:
				print("E| %s query: %s"%(query_json,traceback.format_exc()))
		else:
			for key_name,value_epoc in query_json[timefield].items():
				if key_name in ("$gt","$lt","$gte","$lte"):
					epoch_time = None
					epoch_time = value_epoc
					query_json[timefield][key_name] = datetime.fromtimestamp(value_epoc)
	return query_json


def fetch_mongo_records(mongodb_url,database_name,table_name,query,timefield):
	with MongoClient(mongodb_url) as client:
		assert client is not None
		print("# %s\n"%client)
		print("# using pymongo version: %s"%pymongo_version)
		records_json = []
		try:
			db = client[database_name]
			collection = db[table_name]
			try:
				query_json = adjust_query(query,timefield)
			except Exception as e:
				print("E| %s on [%s](%s) %s"%(str(e),database_name,table_name,traceback.format_exc()))
				print("E| Did you quote your query by using single-quote or double-quote with escaping inside double-quote ?")
				return
			print("## set query into find():  %s "%query_json)
			
			if float(pymongo_version.replace(".","")) >= 370:
				counts = collection.estimated_document_count(query_json)
			else:
				counts = collection.count_documents(query_json)
			print("[%s](%s) ?%s \n%drecords"%(database_name,table_name,query_json,counts))
			# faster fetch way from belows
			# https://stackoverflow.com/questions/38647962/how-to-quickly-fetch-all-documents-mongodb-pymongo
			# https://stackoverflow.com/questions/52904815/using-natural-sort-in-pymongo-mongodb
			# without limit [ records_json.append(record) for record in collection.find({},cursor_type=CursorType.EXHAUST).sort([( '$natural', 1 )]) ]
			position = 0
			while position < counts:
				# E| Can't use limit and exhaust together.
				[ records_json.append(record) for record in collection.find(query_json).sort([( '$natural', 1 )]).skip(position).limit(WINDOW_SIZE) ]
				print("# [%s] position in progress: %d, stored record counts = %d"%(datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ'),position,len(records_json)))
				sys.stdout.flush()
				position += WINDOW_SIZE


			# dump to csv file
			print("\n# CSV dump phaise")
			timestr = datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ')
			filename = database_name + "_" + table_name  + "_" + str(counts) + "records_" + timestr + ".csv"
			dump2csv(filename,records_json,counts)

		except Exception as e:
			print("E| %s on [%s](%s)"%(str(e),database_name,table_name))

if __name__ == '__main__':
	target_mongo = os_environ["TARGET_MONGODB"] # connection string ref: https://www.mongodb.com/docs/manual/reference/connection-string/
	print("# connecting to %s\n"%target_mongo)
	database_name = sys.argv[1]
	table_name = sys.argv[2]
	if len(sys.argv) > 3:
		query = sys.argv[3]
		if len(sys.argv) > 4:
			timefield = sys.argv[4]
		else:
			timefield = None
	else:
		query = "{}"
		timefield = None
	fetch_mongo_records(target_mongo,database_name,table_name,query,timefield)
	print("\n# mongodb session closed.")
