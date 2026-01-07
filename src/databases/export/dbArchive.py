import os
import sys
import shutil
import sqlite3
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils import  get_files, get_directories, get_workspace_paths
##################################################################################
workspace_paths = get_workspace_paths()
workspace_path = workspace_paths["workspace"]
print(workspace_path)
##################################################################################
conn_runtime = sqlite3.connect(os.path.join(workspace_path, "runtime.db"))
cursor_runtime = conn_runtime.cursor() 
query_runtime = "SELECT * FROM site_info"
cursor_runtime.execute(query_runtime)
site_info = cursor_runtime.fetchone()
siteInfo_headers = [description[0] for description in cursor_runtime.description]

siteInfoDict = dict(zip(siteInfo_headers, site_info))
locationId = siteInfoDict['NAME']

conn_runtime.close()
print("Location ID:", locationId)
##################################################################################
dataStorage_dirPath = r"C:\Users\Kjslo\Documents\data\DLL_Preprocessed"
dirs = get_directories(dataStorage_dirPath)
database_list = ["runtime", "usda_nass_cdl", "tempGeo"]
if locationId not  in dirs:
    # Create a new directory in the data storage directory witht the locationId
    os.makedirs(os.path.join(dataStorage_dirPath, locationId))
    print(f"Created directory for site: {locationId}")
else:
    print(f"Directory for site {locationId} exists.")
    shutle_path = os.path.join(dataStorage_dirPath, locationId)
    files = get_files(shutle_path)

    tablesToKeep = ["dir_lib"]
    for fName in database_list:
        db_fName = f"{fName}.db"
        if db_fName not in files:
            print(f"Copying {db_fName} to shutle path")
            #print(f"{db_fName}: {table_names}")

            try:
                db_path = os.path.join(os.path.join(workspace_path, db_fName))
                shutil.copy(db_path, os.path.join(shutle_path, db_fName))
                print(f"Copied to shutle path: {os.path.join(shutle_path, db_fName)}")
                
                conn = sqlite3.connect(db_fName)
                cursor = conn.cursor()         
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                table_names = [table[0] for table in tables]

                for tableName in table_names:
                    if tableName not in tablesToKeep:
                        cursor.execute(f"DELETE FROM {tableName}")
                        print(f"DB:{db_fName} - {tableName} CLEARED")
                conn.close()

            except Exception as e:
                print("Error copying to shutle path:", e)
##################################################################################