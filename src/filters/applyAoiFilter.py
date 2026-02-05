import os
from os import listdir
from os.path import isfile, join
import sys
from datetime import datetime

import json
import sqlite3
import duckdb

import pandas as pd
from geopy.distance import geodesic
import shapely
from shapely.geometry import Point, Polygon
from scipy.spatial import cKDTree
######################################################################################
'''from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
from shapely.geometry.base import BaseGeometry'''
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.utils import  ensure_folder
######################################################################################
def list_files(directory):
	return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
######################################################################################
conn_runtime = sqlite3.connect('runtime.db')
cursor_runtime = conn_runtime.cursor()
cursor_runtime.execute("SELECT * FROM site_info")
site_info = cursor_runtime.fetchone()
siteInfo_headers = [description[0] for description in cursor_runtime.description]

siteInfoDict = dict(zip(siteInfo_headers, site_info))
LOCATION_ID = siteInfoDict['NAME']
OUT_SUBDIR = "aoi_filtered"
aoi_id = "00"

conn_runtime.close()
print("Location ID:", LOCATION_ID)

######################################################################################
# Ensure the targert output folder exists, if not, ensure_folder creates it
ensure_folder(os.path.join(r"output", LOCATION_ID, OUT_SUBDIR), LOCATION_ID)
######################################################################################
# Load the AOI json file
aoiJson_fName = f"{LOCATION_ID}_aoi_{aoi_id}.json"
aoiJson_path = os.path.join(r"resources", "aoi", aoiJson_fName)
# Transform the AOI json into a standard geojson object for shapely processing
with open(aoiJson_path) as f:
    json_file = json.load(f)
aoi_coords = json_file["coordinates"]

aoi_obj = {
    "type": "Feature",
    "properties": {
          "name": "aoi"
    },
    "geometry": {
        "type": "Polygon",
        "coordinates": aoi_coords
    }
}

aoi = shapely.geometry.shape(aoi_obj["geometry"])
######################################################################################
TABLE_NAME = "terrain_composite"
conn_tempGeo = duckdb.connect('tempGeo.duckdb')
# Fetch as pandas DataFrame (more efficient for large datasets)
df = conn_tempGeo.execute(f"SELECT * FROM {TABLE_NAME}").df()
fieldNames = df.columns.tolist()
rows = df.to_records(index=False).tolist()
rowsCount = len(rows)
######################################################################################
filtered_rows = []

fieldsToKeep = [
        'geoid',
        'lat',
        'lon', 
        'lstf',
        'lstf_serc', 
        'lstf_arc',
        'ndvi', 
        'ndvi_serc', 
        'ndvi_arc', 
        'ndmi',
        'ndmi_serc',
        'ndmi_arc',
        'elv_rel',
        'elv',
        'idx_row',
        'idx_col',
        'mdrdasp',
        'mdrdconc',
        'mdrdgrv',
        'mdrdunp',
        'mdconst',
        'mdbldg',
        'lstf_ndvi_corr',
        'lstf_ndmi_corr', 
        'ndvi_ndmi_corr',
        'lstf_ndvi_pval', 
        'lstf_ndmi_pval', 
        'ndvi_ndmi_pval',
        'lstf_temporal',
        'ndvi_temporal',
        'ndmi_temporal',
        'ls_land_cover',
        'cl_land_cover',
        'terrain_classification',
        'anisotropy',
        'cgap',
        'downhill_fraction',
        'dom_angle_deg',
        'dom_dir',
        'dom_dir_elv',
        'dd_geoid',
]

COLS_TO_REFORMAT = ['lstf_temporal', 'ndvi_temporal', 'ndmi_temporal']

for row in rows:
    rowDict = dict(zip(fieldNames, row))
    point = Point(rowDict["lon"], rowDict["lat"])
    inAoi = False
    if aoi.covers(point):
        inAoi = True
        filteredRow = {key: rowDict[key] for key in fieldsToKeep}
        for col in COLS_TO_REFORMAT:
            # Replace "," deliminators with ";" 
            filteredRow[col] = rowDict[col].replace(",", ";")
        filtered_rows.append(filteredRow)
######################################################################################
if filtered_rows:
    filtered_df = pd.DataFrame(filtered_rows)

    conn_tempGeo.execute("""
         CREATE TABLE IF NOT EXISTS ct_aoi_filtered AS 
        SELECT * FROM filtered_df WHERE 1=0
    """)

    conn_tempGeo.execute("TRUNCATE TABLE ct_aoi_filtered")
    conn_tempGeo.execute("INSERT INTO ct_aoi_filtered SELECT * FROM filtered_df")
conn_tempGeo.close()
######################################################################################
# Calculate the percent of the original table the filtered data represents
aoiPrctTotal = (len(filtered_rows) / rowsCount) * 100 if rowsCount > 0 else 0
print(f"AOI filtering completed")
print(f"ct_aoi_filtered rows updated with: {len(filtered_rows)} rows")
print(f"Filtered table represents {aoiPrctTotal:.2f}% of total site")