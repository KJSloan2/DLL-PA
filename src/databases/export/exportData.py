import os
import sys
import io
import math
import csv
import json
import shutil
import sqlite3
import duckdb
##################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from global_functions.utils import  json_serialize
##################################################################################
MAX_FILE_SIZE_MB = 23.5
##################################################################################
# Get site info from runtime.db
conn = sqlite3.connect("runtime.db")
cursor = conn.cursor()
##################################################################################
query = "SELECT * FROM dir_lib WHERE DIR_NAME = ?"
cursor.execute(query, ("FRONTEND_BASE",))
##################################################################################
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
print(siteInfo)
headers = [description[0] for description in cursor.description]
siteInfo = dict(zip(headers, siteInfo))
locationId = siteInfo['NAME']
print(locationId)
##################################################################################

# Prest parameters for DB and Table to export

shutleMapping = {
    "dll_public_terrain":os.path.join(frontendBase_path,"frontend", "public", "terrain"),
}

FIELD_FILTERS = {
    "mapbox":[
        'geoid', 'lat', 'lon', 
        'lstf', 'lstf_serc', 'lstf_arc', 
        'ndvi', 'ndvi_serc', 'ndvi_arc', 
        'ndmi', 'ndmi_serc', 'ndmi_arc',
        'lstf_temporal', 'ndvi_temporal', 'ndmi_temporal',
        'lstf_flag', 'ndvi_flag', 'ndmi_flag',
        'elv_rel', 'elv', 
        'mdrdasp', 'mdrdconc', 'mdrdgrv', 'mdrdunp', 'mdconst', 'mdbldg', 
        'lstf_ndvi_corr', 'lstf_ndmi_corr', 'ndvi_ndmi_corr', 'lstf_ndvi_pval', 'lstf_ndmi_pval', 'ndvi_ndmi_pval', 
        'ls_land_cover', 'cl_land_cover', 'terrain_classification', 
        'anisotropy', 'downhill_fraction', 'dom_angle_deg', 'dom_dir'
    ]
}

dbExportMapping = [
    {
        "db_name": "usda_nass_cdl", 
        "field_filter": "general",
        "table": "cdl_data", 
        "output_dir": "cdl",
        "export_as": ["csv"],
        "fname_as_site": False, 
        "shutle_to_public": False, 
        "shutle_path_key": None
    },
    {
        "db_name": "tempGeo",
        "field_filter": "vercel",
        "table": "terrain_composite", 
        "export_as": ["csv", "geojson"],
        "output_dir": os.path.join("output", "multiSpec"),
        "fname_as_site": True, 
        "shutle_to_public": True,
        "shutle_path_key": "dll_public_terrain"
    },
    {
        "db_name": "tempGeo",
        "field_filter": "general",
        "table": "ct_aoi_filtered",
        "export_as": ["csv"],
        "output_dir": os.path.join("output", "aoi_filtered"),
        "fname_as_site": True,
        "shutle_to_public": False,
        "shutle_path_key": None
    },
]
##################################################################################
# Setup conditions for export
mapping = dbExportMapping[1]

if mapping["fname_as_site"]:
    output_fName = f"{locationId}_{mapping['table']}"

dbName = mapping["db_name"]
tableName = mapping["table"]
fieldFilter = mapping["field_filter"]
exportAs = mapping["export_as"]
output_fName = mapping["table"]+"_"+fieldFilter
##################################################################################
# Connect to db to get data
conn = duckdb.connect(f"{dbName}.duckdb")
cursor = conn.cursor()

if fieldFilter in list(FIELD_FILTERS.keys()):
    columns = FIELD_FILTERS[fieldFilter]
    column_list = ", ".join(columns)
    rows = conn.execute(f"SELECT {column_list} FROM {tableName}").fetchall()
    headers = columns
else:
    # Default behavior: get all columns
    rows = conn.execute(f"SELECT * FROM {tableName}").fetchall()
    headers = [row[1] for row in conn.execute(f"PRAGMA table_info('{tableName}')").fetchall()]

validRows = []
for row in rows:
    nullCount = 0
    for val in row:
        #if val is None or (isinstance(val, float) and math.isnan(val)):
        if val is None or val == "None" or (isinstance(val, float) and math.isnan(val)):
            nullCount += 1
    #print(nullCount)
    if nullCount <= 10: 
        validRows.append(row)

rowCount = len(validRows)

def write_to_csv(path, headers, rowsToWrite):
    with open(path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for row in rowsToWrite:
            writer.writerow(row)

def replace_nulls(data):
    if isinstance(data, dict):
        return {k: (0 if v is None or (isinstance(v, float) and math.isnan(v)) else v) for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        return [0 if v is None or (isinstance(v, float) and math.isnan(v)) else v for v in data]
    else:
        return data

def write_to_geojson(path, headers, rowsToWrite):
    features = []
    for row in rowsToWrite:
        row_dict = dict(zip(headers, row))
        row_dict = replace_nulls(row_dict)
        feature = {
            "type": "Feature",
            "properties": {k: v for k, v in row_dict.items() if k not in ['lat', 'lon']},
            "geometry": {
                "type": "Point",
                "coordinates": [row_dict['lon'], row_dict['lat']]
            }
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    with open(path, "w", encoding='utf-8') as f:
        f.write(json.dumps(geojson, indent=1, default=json_serialize, ensure_ascii=False))

def csv_row_size_bytes(row):
    """
    Return the number of bytes this row would occupy in a UTF-8 CSV file,
    including the newline.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    row = replace_nulls(row)
    writer.writerow(row)
    return len(buffer.getvalue().encode("utf-8"))


header_buffer = io.StringIO()
header_writer = csv.writer(header_buffer, lineterminator="\n")
header_writer.writerow(headers)
header_size = len(header_buffer.getvalue().encode("utf-8"))

row_sizes = [csv_row_size_bytes(row) for row in validRows]
avgRowSize = sum(row_sizes) / len(row_sizes) if row_sizes else 0

maxFileSizeBytes = MAX_FILE_SIZE_MB * 1024 * 1024

print(f"Average row size: {avgRowSize} bytes")
print(f"Maximum file size: {maxFileSizeBytes} bytes")

rowsPerFile = int(maxFileSizeBytes / avgRowSize)
print(f"Rows per file: {rowsPerFile}")

if rowCount > rowsPerFile:
    neededFiles = (rowCount // rowsPerFile) + (1 if rowCount % rowsPerFile > 0 else 0)
    
    for i in range(neededFiles):
        startIdx = i * rowsPerFile
        endIdx = min(startIdx + rowsPerFile, rowCount)
        rowChunk = validRows[startIdx:endIdx]

        if "csv" in exportAs:
            output_path = os.path.join(mapping["output_dir"], output_fName+"_"+str(i)+".csv")
            write_to_csv(output_path, headers, rowChunk)
        if "geojson" in exportAs:
            output_path = os.path.join(mapping["output_dir"], output_fName+".geojson")
            write_to_geojson(output_path, headers, rowChunk)

# Export full data in one file
if "csv" in exportAs:
    output_path = os.path.join(mapping["output_dir"], output_fName+"_allTiles.csv")
    write_to_csv(output_path, headers, validRows)
if "geojson" in exportAs:
    output_path = os.path.join(mapping["output_dir"], output_fName+"_allTiles.geojson")
    write_to_geojson(output_path, headers, validRows)



conn.close()
print(f"{dbName} - {tableName} exported to CSV")
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
print("Export complete.")