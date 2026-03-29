import os
from os import listdir
from os.path import isfile, join
import sys
import subprocess
import cv2

import math
import csv
import json
import sqlite3

import duckdb

import numpy as np
import pandas as pd
from datetime import datetime
from shapely.geometry import Polygon, MultiPolygon, mapping
################### ###################################################################
DIALATION_KERNEL_SIZE = 1
NEIGHBORHOOD_RADIUS = 1
MIN_GROUP_SIZE = 6
DFH_THRESHOLD = 0.75
THRESHOLD_OPERATOR = "gte" 
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.geoFunctions import (
    polygon_area_m2,
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

conn_tempGeo = duckdb.connect('tempGeo.duckdb')

# -------------------------------------------------------------------
# Load terrain_composite using columnar fetch instead of fetchall()
# -------------------------------------------------------------------
print("Loading terrain_composite from DuckDB...")
tempGeo_np = conn_tempGeo.sql("SELECT * FROM terrain_composite").fetchnumpy()
tempGeo_keys = list(tempGeo_np.keys())

if "geoid" not in tempGeo_np or len(tempGeo_np["geoid"]) == 0:
    raise ValueError("terrain_composite is empty or missing required columns.")

n_rows = len(tempGeo_np["geoid"])

print(f"terrain_composite row count: {n_rows}")

# -------------------------------------------------------------------
# Get min / max grid bounds
# -------------------------------------------------------------------
min_col, max_col, min_row, max_row = conn_tempGeo.execute(
    "SELECT MIN(idx_col), MAX(idx_col), MIN(idx_row), MAX(idx_row) FROM terrain_composite"
).fetchone()


rows = int(max_row - min_row + 1)
cols = int(max_col - min_col + 1)

print(f"Grid shape: rows={rows}, cols={cols}")

# -------------------------------------------------------------------
# Initialize terrain arrays
# -------------------------------------------------------------------
elv_array = np.full((rows, cols), np.nan, dtype=np.float64)
lat_array = np.full((rows, cols), np.nan, dtype=np.float64)
lon_array = np.full((rows, cols), np.nan, dtype=np.float64)
dhf_array = np.full((rows, cols), np.nan, dtype=np.float64)
ndvi_array = np.full((rows, cols), np.nan, dtype=np.float64)
ndmi_array = np.full((rows, cols), np.nan, dtype=np.float64)

geoid_array = np.empty((rows, cols), dtype=object)
geoid_array[:] = None

n_rows = len(tempGeo_np["geoid"])
# -------------------------------------------------------------------
# Fill arrays from fetched column arrays
# -------------------------------------------------------------------
valid = (
    ~np.ma.getmaskarray(tempGeo_np["idx_row"]) &
    ~np.ma.getmaskarray(tempGeo_np["idx_col"])
)

idx_row_vals = np.asarray(tempGeo_np["idx_row"][valid], dtype=int)
idx_col_vals = np.asarray(tempGeo_np["idx_col"][valid], dtype=int)
elv_vals = tempGeo_np["elv_rel"][valid]
geoid_vals = tempGeo_np["geoid"][valid]
lat_vals = tempGeo_np["lat"][valid]
lon_vals = tempGeo_np["lon"][valid]
dhf_vals = tempGeo_np["downhill_fraction"][valid]
ndvi_vals = tempGeo_np["ndvi"][valid]
ndmi_vals = tempGeo_np["ndmi"][valid]

n_rows = int(valid.sum())
# Spatial Resolution (SR)
SR = 10
totalRasters = n_rows
toatalAreaSqM = totalRasters * SR**2

for i in range(n_rows):
    row_idx = int(idx_row_vals[i] - min_row)
    col_idx = int(idx_col_vals[i] - min_col)
    dhf_array[row_idx, col_idx] = dhf_vals[i]
    ndvi_array[row_idx, col_idx] = ndvi_vals[i]
    ndmi_array[row_idx, col_idx] = ndmi_vals[i]
    lat_array[row_idx, col_idx] = lat_vals[i]
    lon_array[row_idx, col_idx] = lon_vals[i]

class dhfGroup:
    def __init__(self, r, v, b, g):
        self.range = r
        self.val_array = v
        self.binary_array = b
        self.group_ids = g
        self.buffer_size = 1

'''def dilate_mask(mask, kernel_size=3):
    kernel = np.ones((kernel_size, kernel_size), np.uint8)
    return cv2.dilate(mask, kernel, iterations=1)

def is_within_bounds(i, j, array):
    return 0 <= i < array.shape[0] and 0 <= j < array.shape[1]

def fill_from_index(array_bv, array_gi, start_i, start_j, group_id):
    directions = [(-1, 0), (-1, 1), (0, 1), (1, 1), (1, 0), (1, -1), (0, -1), (-1, -1)]
    if array_bv[start_i, start_j] != 1 or array_gi[start_i, start_j] != 0:
        return
    
    stack = [(start_i, start_j)]
    while stack:
        i, j = stack.pop()
        array_gi[i, j] = group_id
        
        for direction in directions:
            ni, nj = i + direction[0], j + direction[1]
            if is_within_bounds(ni, nj, array_bv) and array_bv[ni, nj] == 1 and array_gi[ni, nj] == 0:
                stack.append((ni, nj))

def assign_group_ids(array_bv, array_gi):
    group_id = 1
    for i in range(array_bv.shape[0]):
        for j in range(array_bv.shape[1]):
            if array_bv[i, j] == 1 and array_gi[i, j] == 0:
                fill_from_index(array_bv, array_gi, i, j, group_id)
                group_id += 1'''


def dilate_mask(mask, kernel_size=1):
    kernel = np.ones((kernel_size, kernel_size), np.uint8)
    return cv2.dilate(mask, kernel, iterations=1)

def is_within_bounds(i, j, array):
    return 0 <= i < array.shape[0] and 0 <= j < array.shape[1]

def get_neighbor_offsets(radius=NEIGHBORHOOD_RADIUS):
    offsets = []
    for di in range(-radius, radius + 1):
        for dj in range(-radius, radius + 1):
            if di == 0 and dj == 0:
                continue
            offsets.append((di, dj))
    return offsets

NEIGHBOR_OFFSETS = get_neighbor_offsets(NEIGHBORHOOD_RADIUS)

def fill_from_index(array_bv, array_gi, start_i, start_j, group_id, radius=NEIGHBORHOOD_RADIUS):
    directions = NEIGHBOR_OFFSETS

    if array_bv[start_i, start_j] != 1 or array_gi[start_i, start_j] != 0:
        return
    
    stack = [(start_i, start_j)]

    while stack:
        i, j = stack.pop()

        if array_gi[i, j] != 0:
            continue

        array_gi[i, j] = group_id
        
        for di, dj in directions:
            ni, nj = i + di, j + dj
            if (
                is_within_bounds(ni, nj, array_bv)
                and array_bv[ni, nj] == 1
                and array_gi[ni, nj] == 0
            ):
                stack.append((ni, nj))

def assign_group_ids(array_bv, array_gi, radius=1):
    group_id = 1
    for i in range(array_bv.shape[0]):
        for j in range(array_bv.shape[1]):
            if array_bv[i, j] == 1 and array_gi[i, j] == 0:
                fill_from_index(array_bv, array_gi, i, j, group_id, radius=radius)
                group_id += 1

fc = {
    "type": "FeatureCollection",
    "features": []
}

filterArgs = [
    {"range": [0, 0.35], "label": "dhf_l"},
    {"range": [0.6, 0.75], "label": "dhf_mh"},
    {"range": [0.751, 0.1], "label": "dhf_h"}
]

for filterArg in filterArgs:

    binary_array = np.zeros_like(dhf_array, dtype=np.int32)
    padded_binary = np.pad(binary_array, pad_width=1, mode='constant', constant_values=0)

    filterRange = filterArg["range"]

    dhfGroup_instance = dhfGroup(
        filterRange, 
        dhf_array, 
        binary_array.astype(np.uint8),
        np.zeros_like(dhf_array, dtype=np.int32)
        )

    mask = (filterRange[0] <= dhf_array) & (dhf_array <= filterRange[1])
    idxR, idxC = np.where(mask)
    dhfGroup_instance.binary_array[idxR, idxC] = 1

    padded_binary = dilate_mask(
        padded_binary.astype(np.uint8) * 255,
        kernel_size=DIALATION_KERNEL_SIZE
    )

    padded_groups = np.pad(dhfGroup_instance.group_ids, pad_width=1, mode='constant', constant_values=0)

    assign_group_ids(dhfGroup_instance.binary_array > 0, padded_groups, radius=NEIGHBORHOOD_RADIUS)

    # Remove padding
    dhfGroup_instance.group_ids = padded_groups[1:-1, 1:-1]

    # Get unique groups
    unique_groups = np.unique(dhfGroup_instance.group_ids)
    unique_groups = unique_groups[unique_groups != 0]

    groupedMask = dhfGroup_instance.group_ids != 0
    idxR, idxC = np.where(groupedMask)


    for group_id in unique_groups:
        group_mask = dhfGroup_instance.group_ids == group_id
        #get size of group_mask
        group_size = np.sum(group_mask)
        if group_size < MIN_GROUP_SIZE:
            continue
        #MIN_GROUP_SIZE
        mask_uint8 = (group_mask).astype(np.uint8) * 255
        
        # Reduce final dilation kernel size
        dilated_mask = dilate_mask(mask_uint8, kernel_size=DIALATION_KERNEL_SIZE)
        
        if np.sum(dilated_mask) > 0:
            contours, hierarchy = cv2.findContours(
                dilated_mask,  # Use dilated mask for contour finding
                cv2.RETR_TREE,
                cv2.CHAIN_APPROX_SIMPLE
            )
            
            if contours:
                try:
                    # Process all contours and their hierarchies
                    polygons = []
                    holes = []
                    mean_ndmi = []
                    mean_ndvi = []
                    
                    # hierarchy format: [Next, Previous, First_Child, Parent]
                    hierarchy = hierarchy[0]  # Remove single-dimensional wrapper
                    
                    for idx, (contour, h) in enumerate(zip(contours, hierarchy)):
                        # Convert contour points to geographic coordinates
                        geo_coords = []
                        pool_ndmi = []
                        pool_ndvi = []
                        for point in contour:
                            try:
                                idx_i = point[0][1]
                                idx_j = point[0][0]

                                coord = (lon_array[idx_i, idx_j], lat_array[idx_i, idx_j])
                                pool_ndmi.append(ndmi_array[idx_i, idx_j])
                                pool_ndvi.append(ndvi_array[idx_i, idx_j])
                                geo_coords.append([coord[0]+0.0002, coord[1]-0.0002])
                            except IndexError:
                                continue

                            '''coord_x = bb_pt1[0] + (x * step_width)
                            coord_y = bb_pt2[1] - (y * abs(step_height))
                            geo_coords.append([coord_x, coord_y])'''
                        
                        # Create polygon from coordinates
                        if len(geo_coords) >= MIN_GROUP_SIZE:
                            geo_coords.append(geo_coords[0])  # close the ring
                            if h[-1] == -1:
                                polygons.append(Polygon(geo_coords))
                            else:
                                holes.append((h[-1], Polygon(geo_coords)))

                        if pool_ndmi:
                            mean_ndmi.append(np.mean(pool_ndmi))
                        #else:
                        #    mean_ndmi.append(np.nan)
                        if pool_ndvi:
                            mean_ndvi.append(np.mean(pool_ndvi))
                    
                    # Associate holes with their parent polygons
                    for parent_idx, hole in holes:
                        if parent_idx < len(polygons):
                            # Get the parent polygon
                            parent = polygons[parent_idx]
                            # Create new polygon with the hole
                            polygons[parent_idx] = Polygon(
                                parent.exterior.coords,
                                [hole.exterior.coords]
                            )
                    
                    if polygons:
                        # Create MultiPolygon from all polygons
                        multi_poly = MultiPolygon(polygons)
                        # Simplify the MultiPolygon
                        simplified = multi_poly.simplify(tolerance=0.000001, preserve_topology=True)
                        
                        areaM2 = polygon_area_m2(simplified)
                        # Create GeoJSON feature
                        fc["features"].append({
                            "type": "Feature",
                            "properties": {
                                "label": filterArg["label"],
                                "range": dhfGroup_instance.range,
                                "group_id": int(group_id),
                                "area": int(areaM2),
                                "mean_ndmi": round(float(np.mean(mean_ndmi)), 2),
                                "mean_ndvi": round(float(np.mean(mean_ndvi)), 2)
                            },
                            "geometry": mapping(simplified)
                        })
                except Exception as e:
                    print(f"Error processing group {group_id}: {e}")
                    continue

with open(os.path.join("output\\3dep", 'dhfGrouped.geojson'), "w", encoding='utf-8') as output_json:
    json.dump(fc, output_json, ensure_ascii=False, indent=2, default=float)


'''df = pd.DataFrame({
    "lat": lat_array[idxR, idxC],
    "lon": lon_array[idxR, idxC],
    "dhf": dhf_array[idxR, idxC],
    "ndvi": ndvi_array[idxR, idxC],
    "ndmi": ndmi_array[idxR, idxC],
    "group_id": dhfGroup_instance.group_ids[idxR, idxC]
}).dropna()
#df = pd.DataFrame({...}).dropna(subset=["lat", "lon", "dhf"])
csv_filename = f'dhfGrouped.csv'
csv_path = os.path.join("output\\3dep", csv_filename)
df.to_csv(csv_path, index=False)'''