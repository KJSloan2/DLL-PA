import os
from os import listdir
from os.path import isfile, join
import sys

import math
import csv
import json
import sqlite3

import numpy as np
from datetime import datetime
'''from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
from shapely.geometry.base import BaseGeometry'''
################### ###################################################################
APPLY_SPATIAL_FILTER = False
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.utils import (
	safe_round,
	normalize_linear1,
	normalize_symmetric
	)
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
######################################################################################
######################################################################################
TERRAIN_CLASSIFICATIONS = {
	"PTS":{
		"anisotropy": [0.7, 1.0],
		"downhill_fraction": [0.4, 0.6],
	}
}
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
geoid_array = np.full((rows, cols), None)

# Get column indices
idx_row_pos = tempGeo_keys.index('idx_row')
idx_col_pos = tempGeo_keys.index('idx_col')
elv_pos = tempGeo_keys.index('elv_rel')
geoid_pos =  tempGeo_keys.index('geoid')
lat_pos = tempGeo_keys.index('lat')
lon_pos = tempGeo_keys.index('lon')

for row in tempGeo_rows:
	row_idx = row[idx_row_pos] - min_row
	col_idx = row[idx_col_pos] - min_col
	elv_array[row_idx, col_idx] = row[elv_pos]
	lat_array[row_idx, col_idx] = row[lat_pos]
	lon_array[row_idx, col_idx] = row[lon_pos]
	geoid_array[row_idx, col_idx] = row[geoid_pos]

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

moore_ref = {
	"NW": {"angle": 315, "sin": None, "cos": None, "offset": (-1, -1)},
	"N": {"angle": 0, "sin": None, "cos": None, "offset": (-1, 0)},
	"NE": {"angle": 45, "sin": None, "cos": None, "offset": (-1, 1)},
	"W":  {"angle": 270, "sin": None, "cos": None, "offset": (0, -1)},
	"E":  {"angle": 90, "sin": None, "cos": None, "offset": (0, 1)},
	"SW": {"angle": 225, "sin": None, "cos": None, "offset": (1, -1)},
	"S":  {"angle": 180, "sin": None, "cos": None, "offset": (1, 0)}, 
	"SE": {"angle": 135, "sin": None, "cos": None, "offset": (1, 1)}
}

moore_labels = []
cos_components = []
sin_components = []
for key, item in moore_ref.items():
	moore_labels.append(key)
	angle = item["angle"]
	angleRad = angle * (math.pi / 180)
	sin = math.sin(angleRad)
	cos = math.cos(angleRad)
	moore_ref[key]["sin"] = sin
	moore_ref[key]["cos"] = cos
	cos_components.append(cos)
	sin_components.append(sin)

DOM_DIR_IDX = {
    "NW": (-1, 1),
    "N": (0, 1),
    "NE": (1, 1),
    "W": (-1, 0),
    "E": (1, 0),
    "SW": (-1, -1),
    "S": (0, -1),
    "SE": (1, -1)
}

NEIGHBOR_DISTS = [
	math.sqrt(2),  # NW
	1.0,           # N
	math.sqrt(2),  # NE
	1.0,           # W
	1.0,           # E
	math.sqrt(2),  # SW
	1.0,           # S
	math.sqrt(2)   # SE
]

elvDelta_signals = {
	"idx_row": [],
	"idx_col": [],
	"center_elv": [],
	"moore_elvs": [],
	"elv_diffs": [],
	"elv_diffs_norm": []
}

def get_neighbor(row_idx, col_idx, direction, refDict):
	offset = refDict.get(direction)["offset"]
	if offset:
		neighbor_row = row_idx + offset[0]
		neighbor_col = col_idx + offset[1]
		return elv_array[neighbor_row, neighbor_col]
	return None

updates_batch = []

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
		#moore_geoids = get_moore_neighborhood_vals(geoid_array, i, j)
		geoid = geoid_array[i, j]

		# Calculate elevation differences
		elv_diffs_raw = [
			(neighbor - center_elv) if not np.isnan(neighbor) else np.nan
			for neighbor in moore_elvs
		]
		
		
		gradients_signed = [
			(dz / d) if not np.isnan(dz) else np.nan
			for dz, d in zip(elv_diffs_raw, NEIGHBOR_DISTS)
		]

		g_signed = gradients_signed.copy()

		g_signed_norm = normalize_symmetric(g_signed)

		# Unsigned magnitude signal (for anisotropy)
		g_abs = [abs(g) if not np.isnan(g) else np.nan for g in gradients_signed]	

		g_abs_norm = normalize_linear1(g_abs)

		downhill = [g for g in g_signed if g < 0]
		uphill   = [g for g in g_signed if g > 0]

		if len(downhill) >= 2:
			#downhill_sorted = sorted(abs(g) for g in downhill)
			downhill_sorted = sorted([abs(g) for g in downhill], reverse=True)
			Cgap = downhill_sorted[0] - downhill_sorted[1]
			downhill_fraction = len(downhill) / 8
		else:
			Cgap = None
			downhill_fraction = None

		if len(uphill) >= 2:
			#uphill_sorted = sorted(abs(g) for g in uphill)
			uphill_sorted = sorted([abs(g) for g in uphill], reverse=True)
			Cgap_uphill = uphill_sorted[0] - uphill_sorted[1]
			uphill_fraction = len(uphill) / 8
		else:
			Cgap_uphill = None
			uphill_fraction = None
		
		weights = g_abs_norm
	
		Rx = sum(w * cos_i for w, cos_i in zip(weights, cos_components))
		Ry = sum(w * sin_i for w, sin_i in zip(weights, sin_components))

		anisotropy = math.sqrt(Rx**2 + Ry**2) / (sum(weights) + 1e-9)
		
		terrainClassification = None
		if anisotropy > 0:
			dominant_angle = math.atan2(Ry, Rx)
			dominant_angle_deg = math.degrees(dominant_angle)
			bearing = (90 - dominant_angle_deg) % 360
		else:
			dominant_angle = None
			dominant_angle_deg = None
			bearing = None

		if bearing is not None:
			store_diffs = []
			for key, item in moore_ref.items():
				moore_labels.append(key)
				angle = item["angle"]
				store_diffs.append(abs(bearing - angle))
			min_diff = min(store_diffs)
			idx_dominant = store_diffs.index(min_diff)
			dominant_direction = moore_labels[idx_dominant]
			#print("dominant_direction: ", dominant_direction)
			offset = moore_ref[dominant_direction]["offset"]
			dominant_ptb_lat = moore_lat[idx_dominant]
			dominant_ptb_lon = moore_lon[idx_dominant]
			dom_dir_elv = moore_elvs[idx_dominant]
		else:
			dominant_direction = None
			dominant_ptb_lat = None
			dominant_ptb_lon = None
			dom_dir_elv = None
		
		terrainClassification = None
		anisotropy_flag = None
		downhill_flag = None
		CGap_flag = None

		if anisotropy is not None:
			if anisotropy <= 0.35:
				anisotropy_flag = True
			else:
				anisotropy_flag = False
		if downhill_fraction is not None:
			if downhill_fraction >= 0.5:
				downhill_flag = True
			else:
				downhill_flag = False
		if Cgap is not None:
			if Cgap <= 2:
				CGap_flag = True
			else:
				CGap_flag = False

		if anisotropy_flag == True and downhill_flag == True:
			terrainClassification = "DSS"

		# Get the indexes of the dominant point based on direction
		try:
			dd_pt_cidx = j + DOM_DIR_IDX[dominant_direction][1]
			dd_pt_ridx = i + DOM_DIR_IDX[dominant_direction][0]
		except:
			dd_pt_cidx = None
			dd_pt_ridx = None

		updates_batch.append((
			terrainClassification,
			safe_round(anisotropy, 3),
			safe_round(Cgap, 3) if Cgap is not None else None,
			safe_round(downhill_fraction, 3) if downhill_fraction is not None else None,
			safe_round(Cgap_uphill, 3) if Cgap_uphill is not None else None,
			safe_round(uphill_fraction, 3) if uphill_fraction is not None else None,
			safe_round(dominant_angle_deg , 3) if dominant_angle_deg is not None else None,
			dominant_direction,
			dominant_ptb_lat,
			dominant_ptb_lon,
			dom_dir_elv,
			dd_pt_cidx,
			dd_pt_ridx,
			geoid
		))
	
cursor_tempGeo.executemany(
    """UPDATE terrain_composite
    SET terrain_classification = ?,
		anisotropy = ?,
		cgap = ?,
		downhill_fraction = ?,
		cgap_uphill = ?,
		uphill_fraction = ?,
		dom_angle_deg = ?,
		dom_dir = ?,
		dom_ptb_lat = ?,
		dom_ptb_lon = ?,
		dom_dir_elv = ?,
		dd_pt_cidx = ?,
		dd_pt_ridx = ?
    WHERE geoid = ?""",
    updates_batch
)

conn_tempGeo.commit()
print("Done")