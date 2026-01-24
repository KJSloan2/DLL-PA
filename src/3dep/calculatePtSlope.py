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
from stats import calc_mean, calc_std, calc_skew, calc_kurt
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
######################################################################################
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

tempGeo_keys = [description[0] for description in cursor_tempGeo.description]
tempGeo_rows = cursor_tempGeo.fetchall()

# ckdTree distance threshold in meters
DIST_THRESH = 30
update_count = 0

###################################################################################
def moore_neighborhood_idxs(row_idx, col_idx, nd=1):
	# Generate the Moore neighborhood (8 surrounding cells)
	neighbors = []
	for dr in range(-nd, nd+1):
		for dc in range(-nd, nd+1):
			if dr == 0 and dc == 0:
				continue  # Skip the center cell
			neighbors.append((row_idx + dr, col_idx + dc))
	return neighbors
###################################################################################
momentsCalculated = 0
ptsProcessed = 0

landCoverValsReplaced = 0


def get_row_by_idx(idx_dict, target_row_idx, target_col_idx):
	return idx_dict.get((target_row_idx, target_col_idx))

idx_row_pos = tempGeo_keys.index('idx_row')
idx_col_pos = tempGeo_keys.index('idx_col')

tempGeo_index = {}
for row in tempGeo_rows:
	key = (row[idx_row_pos], row[idx_col_pos])
	tempGeo_index[key] = dict(zip(tempGeo_keys, row))
	
landCoverValsReplaced = 0
runStats = {}
for i in range(0, 9, 1):
	runStats[i] = 0

slopeStats = {
	"lat":[],
	"lon":[],
	"idx_row":[],
	"idx_col":[],
	"elv_rel":[],
	"slope_skew":[],
	"slope_kurt":[],
	"slope_mean":[],
	"slope_std":[],
	"elv_rel_diffs":[]
	}

for row in tempGeo_rows:
	rowDict = dict(zip(tempGeo_keys, row))
	geoid = rowDict['geoid']
	elvRel = rowDict['elv_rel']
	idxMooreNeighborhoodIdxs = moore_neighborhood_idxs(rowDict['idx_row'], rowDict['idx_col'], nd=1)
	store_elvRelDiffs = {}
	for i, idxs in enumerate(idxMooreNeighborhoodIdxs):
		targetRow = get_row_by_idx(tempGeo_index, idxs[0], idxs[1])
		if targetRow is not None:
			targetRow_elvRel = targetRow['elv_rel']
			targetRow_geoid = targetRow['geoid']
			elvRelDiff = abs(elvRel - targetRow_elvRel)
			store_elvRelDiffs[i] = round((elvRelDiff),1)
	diffs = list(store_elvRelDiffs.values())
	diffsLen = len(diffs)
	runStats[diffsLen]+=1
	if len(diffs) >= 8:
		try:
			diffs_skew = calc_skew(diffs,2)
			diffs_kurt = calc_kurt(diffs,2)
			diffs_mean = calc_mean(diffs,2)
			diffs_std = calc_std(diffs,2)
			moment_elvRelDiffs = [diffs_skew, diffs_kurt, diffs_mean, diffs_std]
			momentsCalculated+=1

			slopeStats["idx_row"].append(rowDict['idx_row'])
			slopeStats["idx_col"].append(rowDict['idx_col'])
			slopeStats["lat"].append(rowDict['lat'])
			slopeStats["lon"].append(rowDict['lon'])
			slopeStats["elv_rel"].append(rowDict['elv_rel'])
			slopeStats["slope_skew"].append(diffs_skew)
			slopeStats["slope_kurt"].append(diffs_kurt)
			slopeStats["slope_mean"].append(diffs_mean)
			slopeStats["slope_std"].append(diffs_std)
			slopeStats["elv_rel_diffs"].append(diffs)
		except Exception as e:
			pass
		
	ptsProcessed+=1

# write slope stats to CSV
slopeStats_df = pd.DataFrame(slopeStats)
slopeStats_df.to_csv(os.path.join(parent_dir, 'output', f'ct_ptSlopes_{locationKey}.csv'), index=False)

momentsCalculated_prct = safe_round((momentsCalculated / ptsProcessed) * 100, 2)
print(momentsCalculated_prct)
for key, val in runStats.items():
	print("Num Neighbors:", key, "Count:", val)