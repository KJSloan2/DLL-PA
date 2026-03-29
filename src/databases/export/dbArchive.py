import os
import sys
import shutil
import sqlite3
import duckdb
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from global_functions.utils import  get_files, get_directories, get_workspace_paths
##################################################################################
workspace_paths = get_workspace_paths()
workspace_path = workspace_paths["parent"]
print(workspace_paths)
##################################################################################
conn_runtime = sqlite3.connect(os.path.join(workspace_path, "runtime.db"))
cursor_runtime = conn_runtime.cursor() 

query = "SELECT * FROM dir_lib WHERE DIR_NAME = ?"
cursor_runtime.execute(query, ("PROCESSED_ARCHIVE",))

target_row = cursor_runtime.fetchone()
if target_row:
    headers = [description[0] for description in cursor_runtime.description]
    target_data = dict(zip(headers, target_row))
    target_path = target_data["DIR_PATH"]
    print(f"PROCESSED_ARCHIVE: {target_path}")
else:
    target_path = None
    print("PROCESSED_ARCHIVE not found in dir_lib")

##################################################################################
query_runtime = "SELECT * FROM site_info"
cursor_runtime.execute(query_runtime)
site_info = cursor_runtime.fetchone()
siteInfo_headers = [description[0] for description in cursor_runtime.description]

siteInfoDict = dict(zip(siteInfo_headers, site_info))
locationId = siteInfoDict['NAME']

conn_runtime.close()
print("Location ID:", locationId)
##################################################################################
archiveProcessedData_path = target_path
##################################################################################
dirs = get_directories(archiveProcessedData_path)
print(dirs)

database_list = ["runtime.db", "usda_nass_cdl.duckdb", "tempGeo.duckdb"]
if locationId not in dirs:
    # Create a new directory in the data storage directory witht the locationId
    os.makedirs(os.path.join(archiveProcessedData_path, locationId))
    print(f"Created directory for site: {locationId}")
else:
    print(f"Directory for site {locationId} exists.")
    shutle_path = os.path.join(archiveProcessedData_path, locationId)
    files = get_files(shutle_path)

    tablesToKeep = ["dir_lib"]
    for db_fName in database_list:
        parts = db_fName.split(".")
        print(parts)

        if parts[-1] in ["db", "duckdb"]:
            db_source_path = os.path.join(workspace_path, db_fName)
            db_dest_path = os.path.join(shutle_path, db_fName)
            
            # Copy database if it doesn't exist
            if db_fName not in files:
                print(f"Copying {db_fName} to shutle path")
                try:
                    shutil.copy(db_source_path, db_dest_path)
                    print(f"Copied to shutle path: {db_dest_path}")
                except Exception as e:
                    print(f"Error copying {db_fName}: {e}")
                    continue
            
            # Clear tables in SOURCE database (workspace)
            try:
                if parts[-1] == "db":
                    # SQLite database
                    conn = sqlite3.connect(db_source_path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = cursor.fetchall()
                    table_names = [table[0] for table in tables]
                    
                    for tableName in table_names:
                        if tableName not in tablesToKeep:
                            cursor.execute(f"DELETE FROM {tableName}")
                            print(f"SOURCE DB:{db_fName} - {tableName} CLEARED")
                            conn.commit()
                    conn.close()
                    
                elif parts[-1] == "duckdb":
                    # DuckDB database
                    conn = duckdb.connect(db_source_path)
                    tables_result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
                    table_names = [table[0] for table in tables_result]
                    
                    for tableName in table_names:
                        if tableName not in tablesToKeep:
                            conn.execute(f"DELETE FROM {tableName}")
                            print(f"SOURCE DB:{db_fName} - {tableName} CLEARED")
                            conn.commit()
                    conn.close()
                    
            except Exception as e:
                print(f"Error clearing tables in SOURCE {db_fName}: {e}")
##################################################################################