import sys
from os import environ as os_environ
import pprint,traceback
from pymongo import MongoClient
from datetime import datetime
import pandas as pd

def dump2csv(filename,records_json,counts):
	try:
		df_master = pd.DataFrame()
		c=0
		for row_json in records_json:
			del row_json["_id"]
			if c==0:
				print("### record schema check")
				pprint.pprint(row_json)
			c+=1
			df_json = pd.io.json.json_normalize(row_json)
			df_master = pd.concat([df_master, df_json])
		df_master.to_csv(filename, index=False, encoding='utf-8')
		print("# Saved to %s"%filename)
	except Exception as e:
		print("E|<%s> by %s"%(filename, traceback.format_exc()))

	file_rows = sum([1 for _ in open(filename)])
	print("# file row num: %d"%file_rows)
	if counts == file_rows-1:
		print("INTEGRITEY CHECK: SUCCESS !")
	else:
		print("INTEGRITEY CHECK: FAILURE  (%d - %d = %d)"%(counts,file_rows-1,counts-file_rows-1))

def pull_mongo_records(mongodb_url,database_name,table_name):
	with MongoClient(mongodb_url) as client:
		assert client is not None
		print("# %s\n"%client)
		records_json = []
		try:
			db = client[database_name]
			collection = db[table_name]
			counts = collection.count_documents({})
			print("[%s](%s) %drecords"%(database_name,table_name,counts))

			[records_json.append(record) for record in collection.find({})]

			# dump to csv file
			print("\n # CSV dump phaise")
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
	pull_mongo_records(target_mongo,database_name,table_name)
	print("\n# mongodb session closed.")
