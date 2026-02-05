import os
import csv
import shutil
import sqlite3
import duckdb
##################################################################################
# Get site info from runtime.db
conn = sqlite3.connect("runtime.db")
cursor = conn.cursor()

query = "SELECT * FROM dir_lib WHERE DIR_NAME = ?"
cursor.execute(query, ("FRONTEND_BASE",))

frontendBase_row = cursor.fetchone()
if frontendBase_row:
    headers = [description[0] for description in cursor.description]
    frontendBase_data = dict(zip(headers, frontendBase_row))
    frontendBase_path = frontendBase_data["DIR_PATH"]
    print(f"FRONTEND_BASE: {frontendBase_path}")
else:
    frontendBase_path = None
    print("FRONTEND_BASE not found in dir_lib")
##################################################################################
query = "SELECT * FROM site_info"
cursor.execute(query)

siteInfo = cursor.fetchone()
headers = [description[0] for description in cursor.description]
siteInfo = dict(zip(headers, siteInfo))
locationId = siteInfo['NAME']
print(locationId)
##################################################################################

# Prest parameters for DB and Table to export

shutleMapping = {
    "dll_public_terrain":os.path.join(frontendBase_path,"frontend", "public", "terrain"),
}

dbExportMapping = [
    {"db_name": "usda_nass_cdl", "table": "cdl_data", "output_dir": "cdl", "fname_as_site": False, "shutle_to_public": False, "shutle_path_key": None},
    {
        "db_name": "tempGeo", 
        "table": "terrain_composite", 
        "output_dir": "MultiSpecTemp", 
        "fname_as_site": True, 
        "shutle_to_public": True,
        "shutle_path_key": "dll_public_terrain"
    },
    {
        "db_name": "tempGeo",
        "table": "ct_aoi_filtered",
        "output_dir": os.path.join("output", "aoi_filtered"),
        "fname_as_site": True,
        "shutle_to_public": False,
        "shutle_path_key": None
    },
]
##################################################################################
# Setup conditions for export
mapping = dbExportMapping[2]
output_fName = mapping["table"]
if mapping["fname_as_site"]:
    output_fName = f"{locationId}_{mapping['table']}"

DB_NAME = mapping["db_name"]
TABLE_NAME = mapping["table"]
##################################################################################
# Connect to db to get data
conn = duckdb.connect(f"{DB_NAME}.duckdb")
cursor = conn.cursor()
# Setup export path
output_path = os.path.join(mapping["output_dir"], output_fName+".csv")
# Write to csv
with open(output_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    query = f"SELECT * FROM {TABLE_NAME}"
    cursor.execute(query)
    
    headers = [description[0] for description in cursor.description]
    writer.writerow(headers)

    for row in cursor.fetchall():
        writer.writerow(row)

conn.close()
print(f"{DB_NAME} - {TABLE_NAME} exported to CSV")
##################################################################################
# If specified by mapping parameters, copy exported csv to public
if mapping["shutle_to_public"]:
    shutle_path = shutleMapping[mapping["shutle_path_key"]]
    try:
        shutil.copy(output_path, os.path.join(shutle_path, output_fName+".csv"))
        print(f"Copied to public_data: {os.path.join(shutle_path, output_fName+'.csv')}")
    except Exception as e:
        print("Error copying to public_data:", e)
##################################################################################