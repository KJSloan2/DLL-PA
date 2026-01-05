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
APPLY_SPATIAL_FILTER = False
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import safe_round, polygon_filter, fill_nulls
from spatial_utils import pt_in_geom, nearby_feature, haversine
from multispecTools import classify_land_cover
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
######################################################################################
if APPLY_SPATIAL_FILTER:
	filterPolygonFileName = "aoi_polygon_2025-12-09.geojson"
	filterPolygonFile_path = os.path.join(r"data", "spatial_filters", filterPolygonFileName)
	with open(filterPolygonFile_path) as f:
		filterPolygonJson = json.load(f)
	filterPolygonCoords = filterPolygonJson["features"][0]["geometry"]["coordinates"]
	filterPolygon = shapely.geometry.shape(filterPolygonJson["features"][0]["geometry"])
######################################################################################
conn_tempGeo = sqlite3.connect('tempGeo.db')
cursor_tempGeo = conn_tempGeo.cursor()

query_terrainComposite = f"SELECT * FROM terrain_composite"
cursor_tempGeo.execute(query_terrainComposite)

conn_cdl = sqlite3.connect('usda_nass_cdl.db')
cursor_cdl = conn_cdl.cursor()

query_cdlData = f"SELECT * FROM cdl_data"
cursor_cdl.execute(query_cdlData)

'''[
	'geoid', 'lat', 'lon', 
	'lstf', 'lstf_serc', 'lstf_arc', 
	'ndvi', 'ndvi_serc', 'ndvi_arc', 
	'ndmi', 'ndmi_serc', 'ndmi_arc', 
	'idx_row', 'idx_col', 
	'lstf_ndvi_corr', 'lstf_ndmi_corr', 
	'ndvi_ndmi_corr', 'lstf_ndvi_pval', 
	'lstf_ndmi_pval', 'ndvi_ndmi_pval', 
	'lstf_temporal', 'ndvi_temporal', 'ndmi_temporal'
'''
#multiSpecData = pd.DataFrame(cursor_rows, columns=columnNames_spectralTemporal)
def db_to_ckdtree(x_key, y_key, db_cursor):
	column_keys = [description[0] for description in db_cursor.description]
	rows = db_cursor.fetchall()
	pts = []
	try:
		for row in rows:
			rowDict = dict(zip(column_keys, row))
			pt = Point(rowDict[y_key], rowDict[x_key])
			pts.append(pt)
	except Exception as e:
		print("Error creating points from db:", e)
	if len(pts) != 0:
		try:
			pts_array = np.array([[p.x, p.y] for p in pts])
			ckdTree = cKDTree(pts_array)
			print("CKDTree created with", len(pts_array), "points")
		except Exception as e:
			print("Error creating cKDTree:", e)
			ckdTree = None
	else:
		ckdTree = None
		print("No points available to create cKDTree")
	return ckdTree, column_keys, rows


cld_ckdTree, cld_headers, cld_rows = db_to_ckdtree("LAT", "LON", cursor_cdl)
cdlDataframe = pd.DataFrame(cld_rows, columns=cld_headers)

cursor_tempGeo
tempGeo_keys = [description[0] for description in cursor_tempGeo.description]
tempGeo_rows = cursor_tempGeo.fetchall()

# ckdTree distance threshold in meters
DIST_THRESH = 35
update_count = 0

for row in tempGeo_rows:
    rowDict = dict(zip(tempGeo_keys, row))
    geoid = rowDict['geoid']
    query_coords = np.array([rowDict['lon'], rowDict['lat']])
    distance, cpidx = cld_ckdTree.query(query_coords)
    cp_data = cdlDataframe.iloc[cpidx]
    cp_lat, cp_lon = cp_data['LAT'], cp_data['LON']
    hDist = haversine(query_coords, [cp_lon, cp_lat])["m"]
    
    if hDist <= DIST_THRESH:
        lcVal, lcLabel = cp_data['LC_VAL'], cp_data['LC_LABEL']
        
        # Update the terrain_composite table
        cursor_tempGeo.execute(
            "UPDATE terrain_composite SET ls_land_cover = ? WHERE geoid = ?",
            (int(lcVal), geoid)
        )
        update_count += 1
        
        if update_count % 1000 == 0:
            print(f"Updated {update_count} rows...")
            conn_tempGeo.commit()

# Final commit
conn_tempGeo.commit()
print(f"Total rows updated: {update_count}")

conn_tempGeo.close()
conn_cdl.close()