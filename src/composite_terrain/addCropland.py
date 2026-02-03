import os
from os import listdir
from os.path import isfile, join
import sys

import json
import duckdb

import pandas as pd
import numpy as np
from geopy.distance import geodesic
import shapely
from shapely.geometry import Point, Polygon
from scipy.spatial import cKDTree
from datetime import datetime
'''from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
from shapely.geometry.base import BaseGeometry'''
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#from global_functions.utils import safe_round, polygon_filter, fill_nulls
from spatial_utils import haversine
#from global_functions.multispecFunctions import classify_land_cover
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
######################################################################################
conn_tempGeo = duckdb.connect('tempGeo.duckdb')
cursor_tempGeo = conn_tempGeo.cursor()

query_terrainComposite = f"SELECT * FROM terrain_composite"
cursor_tempGeo.execute(query_terrainComposite)

conn_cdl = duckdb.connect('usda_nass_cdl.duckdb')
cursor_cdl = conn_cdl.cursor()

query_cdlData = f"SELECT * FROM cdl_data"
cursor_cdl.execute(query_cdlData)

#cdl_rows = cursor_cdl.fetchall()
cdl_descriptions = [description[0] for description in cursor_cdl.description]
print(cdl_descriptions)
row_count = conn_cdl.execute("SELECT COUNT(*) FROM cdl_data").fetchone()[0]
print(f"CDL data rows: {row_count}")

if row_count == 0:
    print("ERROR: No data in cdl_data table!")
    conn_tempGeo.close()
    conn_cdl.close()
    sys.exit(1)
######################################################################################
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
######################################################################################
cld_ckdTree, cld_headers, cld_rows = db_to_ckdtree("lat", "lon", cursor_cdl)
cdlDataframe = pd.DataFrame(cld_rows, columns=cld_headers)

tempGeo_keys = [description[0] for description in cursor_tempGeo.description]
tempGeo_rows = cursor_tempGeo.fetchall()
######################################################################################
# ckdTree distance threshold in meters
DIST_THRESH = 30
update_count = 0
landCoverValsReplaced = 0
for row in tempGeo_rows:
	rowDict = dict(zip(tempGeo_keys, row))
	geoid = rowDict['geoid']
	lsVal = rowDict.get('ls_land_cover', None)
	query_coords = np.array([rowDict['lon'], rowDict['lat']])
	distance, cpidx = cld_ckdTree.query(query_coords)
	cp_data = cdlDataframe.iloc[cpidx]
	cp_lat, cp_lon = cp_data['lat'], cp_data['lon']
	hDist = haversine(query_coords, [cp_lon, cp_lat])["m"]
	
	if hDist <= DIST_THRESH:
		lcVal, lcLabel = cp_data['lc_val'], cp_data['lc_label']

		# Override with Landsat land cover if lsVal is barren
		if lsVal == "BN":
			lcVal = 65
			landCoverValsReplaced+=1
		# USDA defined 65 AND 131 as barren. Simplify to 65 only
		if lcVal == 131:
			lcVal = 65

		# Update the terrain_composite table
		cursor_tempGeo.execute(
			"UPDATE terrain_composite SET  cl_land_cover = ? WHERE geoid = ?",
			(int(lcVal), geoid)
		)
		update_count += 1
		
		if update_count % 1000 == 0:
			print(f"Updated {update_count} rows...")
			conn_tempGeo.commit()
######################################################################################
print(f"Total land cover values replaced: {landCoverValsReplaced}")

# Final commit
conn_tempGeo.commit()
print(f"Total rows updated: {update_count}")

conn_tempGeo.close()
conn_cdl.close()
######################################################################################
print("addCropland.py completed.")