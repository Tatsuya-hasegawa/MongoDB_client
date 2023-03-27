from os import environ as os_environ
from pymongo import MongoClient

def list_dbs(mongodb_url):
	with MongoClient(mongodb_url) as client:
		assert client is not None
		print("# %s\n"%client)
		for database_name in client.database_names():
			print("\nD|\t %s"%database_name)
			try:
				for table_name in client[database_name].collection_names(include_system_collections=False):
					counts = client[database_name][table_name].count_documents({})
					print("T|\t\t - %s (%d records)"%(table_name,counts))
			except Exception as e:
				print("E| %s on %s"%(str(e),database_name))


if __name__ == '__main__':
	target_mongo = os_environ["TARGET_MONGODB"] # connection string ref: https://www.mongodb.com/docs/manual/reference/connection-string/
	print("# connecting to %s\n"%target_mongo)
	list_dbs(target_mongo)
	print("\n# mongodb session closed.")

