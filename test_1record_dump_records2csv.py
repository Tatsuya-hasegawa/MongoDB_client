import sys
from os import environ as os_environ
import pprint,traceback
from pymongo import MongoClient
from datetime import datetime
from pandas.io.json import json_normalize

def dump2csv(filename,records_json):
	try:
		df_json = json_normalize(records_json)
		df_json.to_csv(filename, encoding='utf-8')
		'''old
		import csv
		if isinstance(records_json,list):
			fieldnames = records_json[0].keys()
		if isinstance(records_json,dict):
			fieldnames = records_json.keys()
		else:
			raise TypeError
		print(fieldnames)
		with open(filename,"w") as wf:
			writer = csv.DictWriter(wf, fieldnames=fieldnames,doublequote=True,quoting=csv.QUOTE_ALL)
			writer.writeheader()
			writer.writerows(records_json)
		'''
		print("# Saved to %s"%filename)
	except Exception as e:
		print("E|<%s> by %s"%(filename, traceback.format_exc()))


def pull_mongo_records(mongodb_url,database_name,table_name):
	with MongoClient(mongodb_url) as client:
		assert client is not None
		print("# %s\n"%client)
		try:
			db = client[database_name]
			collection = db[table_name]
			counts = collection.count_documents({})
			print("[%s](%s) %drecords"%(database_name,table_name,counts))
			records_json = collection.find_one({})
			pprint.pprint(records_json)

			# dump to csv file
			print("\n # CSV dump phaise")
			timestr = datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ')
			filename = "test1" + "-" + database_name + "_" + table_name + "_" + str(counts) + "records_" + timestr + ".csv"
			dump2csv(filename,records_json)

		except Exception as e:
			print("E| %s on [%s](%s)"%(str(e),database_name,table_name))

if __name__ == '__main__':
	target_mongo = os_environ["TARGET_MONGODB"] # connection string ref: https://www.mongodb.com/docs/manual/reference/connection-string/
	print("# connecting to %s\n"%target_mongo)
	database_name = sys.argv[1]
	table_name = sys.argv[2]
	pull_mongo_records(target_mongo,database_name,table_name)
	print("\n# mongodb session closed.")
