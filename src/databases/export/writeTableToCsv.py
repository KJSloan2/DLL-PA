import sqlite3
import csv
import os

dbExportMapping = [
    {"db_name": "usda_nass_cdl", "table": "cdl_data", "output_dir": "cdl"},
    {"db_name": "tempGeo", "table": "terrain_composite", "output_dir": "MultiSpecTemp"},
]
mapping = dbExportMapping[1]
OUTPUT_FNAME = mapping["table"]
OUTPUT_SUBFOLDER = mapping["output_dir"]

DB_NAME = mapping["db_name"]
TABLE_NAME = mapping["table"]

conn = sqlite3.connect(f"{DB_NAME}.db")
cursor = conn.cursor()

with open(os.path.join("data", OUTPUT_SUBFOLDER, OUTPUT_FNAME+".csv"), mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    query = f"SELECT * FROM {TABLE_NAME}"
    cursor.execute(query)
    
    headers = [description[0] for description in cursor.description]
    writer.writerow(headers)

    for row in cursor.fetchall():
        writer.writerow(row)

conn.close()
print(f"{DB_NAME} - {TABLE_NAME} exported to CSV")