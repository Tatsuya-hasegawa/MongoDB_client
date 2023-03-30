from os import environ as os_environ
from pymongo import MongoClient, version as pymongo_version 

def list_dbs(mongodb_url):
	with MongoClient(mongodb_url) as client:
		assert client is not None
		print("# %s\n"%client)
		print("# using pymongo version: %s"%pymongo_version)
		pymongo_major,pymongo_minor,pymongo_micro = pymongo_version.split(".")
		if int(pymongo_major) >= 4:
			databases = client.list_databases()
		else: 
			databases = client.database_names()
		for database_name in databases:
			print("\nD|\t %s"%database_name)
			if isinstance(database_name,dict):
				database_name = database_name.get("name")
			try:
				if int(pymongo_major) >= 4 or ( int(pymongo_major) >= 3 and int(pymongo_minor) >= 7):
					tables = client[database_name].list_collection_names()
					for table_name in tables:
						try: 
							counts = client[database_name][table_name].estimated_document_count({}) 
						except:
							try: 
								counts = client[database_name][table_name].estimated_document_count() # e.g. 3.13.0
							except: 
								counts = client[database_name][table_name].count_documents({}) # unexpected version
						print("T|\t\t - %s (%d records)"%(table_name,counts))
				else:
					tables = client[database_name].collection_names(include_system_collections=False)
					for table_name in tables:
						counts = client[database_name][table_name].count_documents({})
						print("T|\t\t - %s (%d records)"%(table_name,counts))
			except Exception as e:
				print("E| %s on %s"%(str(e),database_name))


if __name__ == '__main__':
	target_mongo = os_environ["TARGET_MONGODB"] # connection string ref: https://www.mongodb.com/docs/manual/reference/connection-string/
	print("# connecting to %s\n"%target_mongo)
	list_dbs(target_mongo)
	print("\n# mongodb session closed.")

