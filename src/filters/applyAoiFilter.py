import os
from os import listdir
from os.path import isfile, join
import sys

import csv
import json
import sqlite3

import pandas as pd
import numpy as np
from geopy.distance import geodesic
import shapely
from shapely.geometry import Point, Polygon
from scipy.spatial import cKDTree
from datetime import datetime
'''from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
from shapely.geometry.base import BaseGeometry'''
################### ###################################################################
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
aoi_id = "01"

conn_runtime.close()
print("Location ID:", LOCATION_ID)

######################################################################################
# Ensure the targert output folder exists, if not, ensure_folder creates it
ensure_folder(os.path.join(r"output", LOCATION_ID, OUT_SUBDIR), LOCATION_ID)
######################################################################################
aoiJson_fName = f"{LOCATION_ID}_aoi_{aoi_id}.json"
aoiJson_path = os.path.join(r"resources", "aoi", aoiJson_fName)
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

conn_tempGeo = sqlite3.connect('tempGeo.db')
cursor_tempGeo = conn_tempGeo.cursor()

query = f"SELECT * FROM {TABLE_NAME}"
cursor_tempGeo.execute(query)

rows = cursor_tempGeo.fetchall()
fieldNames = [description[0] for description in cursor_tempGeo.description]
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
        "mdrdasp",
        "mdrdconc",
        "mdrdgrv",
        "mdrdunp",
        "mdconst",
        "mdbldg",
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

# Write filtered rows to csv
output_fName = f"{LOCATION_ID}_aoi_{aoi_id}-filtered.csv"
output_path = os.path.join(r"output", LOCATION_ID, output_fName)
with open(output_path, mode='w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fieldsToKeep)
    writer.writeheader()
    writer.writerows(filtered_rows)
######################################################################################

print(f"AOI filtering completed. Filtered data saved to: {output_path}")