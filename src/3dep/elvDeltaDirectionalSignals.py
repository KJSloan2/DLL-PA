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
from utils import (
	safe_round,
	normalize_linear1
	)
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

tempGeo_keys = [description[0] for description in cursor_tempGeo.description]
tempGeo_rows = cursor_tempGeo.fetchall()


cursor_tempGeo.execute("SELECT MIN(idx_col), MAX(idx_col), MIN(idx_row), MAX(idx_row) FROM terrain_composite")
min_col, max_col, min_row, max_row = cursor_tempGeo.fetchone()

rows = max_row - min_row + 1
cols = max_col - min_col + 1

# Initialize elevation array with NaN values
elv_array = np.full((rows, cols), np.nan)
lat_array = np.full((rows, cols), np.nan)
lon_array = np.full((rows, cols), np.nan)

# Get column indices
idx_row_pos = tempGeo_keys.index('idx_row')
idx_col_pos = tempGeo_keys.index('idx_col')
elv_pos = tempGeo_keys.index('elv')

lat_pos = tempGeo_keys.index('lat')
lon_pos = tempGeo_keys.index('lon')

for row in tempGeo_rows:
	row_idx = row[idx_row_pos] - min_row
	col_idx = row[idx_col_pos] - min_col
	elv_array[row_idx, col_idx] = row[elv_pos]
	lat_array[row_idx, col_idx] = row[lat_pos]
	lon_array[row_idx, col_idx] = row[lon_pos]


def get_moore_neighborhood_vals(d_array, row_idx, col_idx):
	offsets = [
		(-1, -1),  # upper left (NW)
		(-1,  0),  # directly above (N)
		(-1,  1),  # upper right (NE)
		( 0, -1),  # left (W)
		( 0,  1),  # right (E)
		( 1, -1),  # lower left (SW)
		( 1,  0),  # directly below (S)
		( 1,  1)   # lower right (SE)
	]

	neighborhood = []
	for dr, dc in offsets:
		neighbor_row = row_idx + dr
		neighbor_col = col_idx + dc
		neighborhood.append(d_array[neighbor_row, neighbor_col])
	return neighborhood


MOORE_OFFESTS = {
	"NW": (-1, -1),
	"N":  (-1,  0),
	"NE": (-1,  1),
	"W":  ( 0, -1),
	"E":  ( 0,  1),
	"SW": ( 1, -1),
	"S":  ( 1,  0),
	"SE": ( 1,  1)
}

MOORE_OFFESTS_LABELS = list(MOORE_OFFESTS.keys())

elvDelta_signals = {
	"idx_row": [],
	"idx_col": [],
	"center_elv": [],
	"moore_elvs": [],
	"elv_diffs": [],
	"elv_diffs_norm": []
}

def get_neighbor(row_idx, col_idx, direction):
	offset = MOORE_OFFESTS.get(direction)
	if offset:
		neighbor_row = row_idx + offset[0]
		neighbor_col = col_idx + offset[1]
		return elv_array[neighbor_row, neighbor_col]
	return None

with open(os.path.join(
	parent_dir, 'output', 
	f'ct_elvDeltaDirectionalSignals_{locationKey}.csv'), 
	'w', newline='') as csv_output_file:
	
	fieldnames = [
		"idx_row",
		"idx_col",
		"mnc_elv",
		"mnc_lat",
		"mnc_lon",
		"mn_elv_max",
		"mn_elv_max_dir",
		"mn_elv_max_lat",
		"mn_elv_max_lon",
		"mn_elv_min",
		"mn_elv_min_dir",
		"mn_elv_min_lat",
		"mn_elv_min_lon",
	]

	writer = csv.DictWriter(csv_output_file, fieldnames=fieldnames)
	writer.writeheader()

	for i in range(1, rows-1):
		for j in range(1, cols-1):
			center_elv = elv_array[i, j]
			if np.isnan(center_elv):
				continue
			
			center_lat = lat_array[i, j]
			center_lon = lon_array[i, j]

			moore_elvs = get_moore_neighborhood_vals(elv_array, i, j)
			moore_lat = get_moore_neighborhood_vals(lat_array, i, j)
			moore_lon = get_moore_neighborhood_vals(lon_array, i, j)

			elv_diffs = [round((neighbor - center_elv), 2) if not np.isnan(neighbor) else np.nan 
                    for neighbor in moore_elvs]
			
			elv_diffs_norm = normalize_linear1(elv_diffs)

			elv_diffs_abs = [abs(diff) if not np.isnan(diff) else np.nan for diff in elv_diffs]

			elv_diffs_max = max(elv_diffs_abs)
			elv_diffs_min = min(elv_diffs_abs)
			idx_elv_diffs_max = elv_diffs_abs.index(elv_diffs_max)
			idx_elv_diffs_min = elv_diffs_abs.index(elv_diffs_min)

			minElvDiffDirectionLabel = MOORE_OFFESTS_LABELS[idx_elv_diffs_min]
			maxElvDiffDirectionLabel = MOORE_OFFESTS_LABELS[idx_elv_diffs_max]

			idx_neighbor_elvDiff_min = MOORE_OFFESTS[minElvDiffDirectionLabel]
			idx_neighbor_elvDiff_max = MOORE_OFFESTS[maxElvDiffDirectionLabel]

			lat_elvDiff_min = moore_lat[idx_elv_diffs_min]
			lon_elvDiff_min = moore_lon[idx_elv_diffs_min]

			lat_elvDiff_max = moore_lat[idx_elv_diffs_max]
			lon_elvDiff_max = moore_lon[idx_elv_diffs_max]

			'''rowOut = [
				i,
				j,
				center_elv,
				center_lat,
				center_lon,
				elv_diffs_max,
				maxElvDiffDirectionLabel,
				lat_elvDiff_max,
				lon_elvDiff_max,
				elv_diffs_min,
				minElvDiffDirectionLabel,
				lat_elvDiff_min,
				lon_elvDiff_min,
			]'''
			#writer.writerow(rowOut)
			writer.writerow({
				"idx_row": i,
				"idx_col": j,
				"mnc_elv": center_elv,
				"mnc_lat": center_lat,
				"mnc_lon": center_lon,
				"mn_elv_max": elv_diffs_max,
				"mn_elv_max_dir": maxElvDiffDirectionLabel,
				"mn_elv_max_lat": lat_elvDiff_max,
				"mn_elv_max_lon": lon_elvDiff_max,
				"mn_elv_min": elv_diffs_min,
				"mn_elv_min_dir": minElvDiffDirectionLabel,
				"mn_elv_min_lat": lat_elvDiff_min,
				"mn_elv_min_lon": lon_elvDiff_min,
			})	


			
			#print(f"{minElvDiffDirectionLabel}: {elv_diffs_min}, {maxElvDiffDirectionLabel}: {elv_diffs_max}")

			elvDelta_signals["idx_row"].append(i + min_row)
			elvDelta_signals["idx_col"].append(j + min_col)
			elvDelta_signals["center_elv"].append(center_elv)
			elvDelta_signals["moore_elvs"].append(moore_elvs)
			elvDelta_signals["elv_diffs"].append(elv_diffs)
			elvDelta_signals["elv_diffs_norm"].append(elv_diffs_norm)


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
'''momentsCalculated = 0
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
	print("Num Neighbors:", key, "Count:", val)'''