import os
from os import listdir
from os.path import isfile, join
import sys
import subprocess

import math
import csv
import json
import sqlite3

import duckdb

import numpy as np
import pandas as pd
from datetime import datetime

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
def calc_moore_tri(center_elv, moore_elvs):
    """
    Calculate Terrain Roughness Index (TRI) from a center elevation array
    and its 8-cell Moore neighborhood elevation stack.

    Formula:
        TRI = sqrt((1 / N) * sum((e_j - e_i)^2))

    Parameters
    ----------
    center_elv : np.ndarray
        2D array of center-cell elevations with shape (rows, cols).
    moore_elvs : np.ndarray
        3D array of neighbor elevations with shape (8, rows, cols),
        where axis 0 contains the 8 Moore neighbors.

    Returns
    -------
    np.ndarray
        2D array of TRI values with shape (rows, cols).
    """
    diff_sq = (moore_elvs - center_elv[np.newaxis, :, :]) ** 2
    tri = np.sqrt(np.nanmean(diff_sq, axis=0))
    return tri
######################################################################################
TERRAIN_CLASSIFICATIONS = {
    "PTS": {
        "anisotropy": [0.7, 1.0],
        "downhill_fraction": [0.4, 0.6],
    }
}
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

stringFields = ['lstf_temporal', 'ndvi_temporal', 'ndmi_temporal', 'ls_land_cover', 'cl_land_cover']
fieldToRetain = [
    'lat', 'lon',
    'lstf', 'lstf_serc', 'lstf_arc', 'lstf_flag',
    'ndvi', 'ndvi_serc', 'ndvi_arc', 'ndvi_flag',
    'ndmi', 'ndmi_serc', 'ndmi_arc', 'ndmi_flag',
    'elv_rel', 'elv',
    'idx_row', 'idx_col',
    'mdrdasp', 'mdrdconc', 'mdrdgrv', 'mdrdunp', 'mdconst', 'mdbldg',
    'lstf_ndvi_corr', 'lstf_ndmi_corr', 'ndvi_ndmi_corr',
    'lstf_ndvi_pval', 'lstf_ndmi_pval', 'ndvi_ndmi_pval',
    'lstf_temporal', 'ndvi_temporal', 'ndmi_temporal',
    'ls_land_cover', 'cl_land_cover'
]

arrays = {}
for field in fieldToRetain:
    if field in tempGeo_keys:
        # Create string array for string fields, float array for numeric fields
        if field in stringFields:
            arrays[field] = np.full((rows, cols), None, dtype=object)
        else:
            arrays[field] = np.full((rows, cols), np.nan, dtype=np.float64)
    else:
        print(f"Warning: Field '{field}' not found in terrain_composite. It will be filled with NaN/None.")
        if field in stringFields:
            arrays[field] = np.full((rows, cols), None, dtype=object)
        else:
            arrays[field] = np.full((rows, cols), np.nan, dtype=np.float64)

geoid_array = np.empty((rows, cols), dtype=object)
geoid_array[:] = None

# -------------------------------------------------------------------
# Fill arrays from fetched column arrays
# -------------------------------------------------------------------
valid = (
    ~np.ma.getmaskarray(tempGeo_np["idx_row"]) &
    ~np.ma.getmaskarray(tempGeo_np["idx_col"])
)
valid_indices = np.where(valid)[0]
idx_row_vals = np.asarray(tempGeo_np["idx_row"][valid], dtype=int)
idx_col_vals = np.asarray(tempGeo_np["idx_col"][valid], dtype=int)

elv_vals = tempGeo_np["elv_rel"]
geoid_vals = tempGeo_np["geoid"]
lat_vals = tempGeo_np["lat"]
lon_vals = tempGeo_np["lon"]

n_valid = int(valid.sum())

print("Populating working arrays...")
for k in range(n_valid):
    row_idx = int(idx_row_vals[k] - min_row)
    col_idx = int(idx_col_vals[k] - min_col)

    elv_array[row_idx, col_idx] = elv_vals[k]
    lat_array[row_idx, col_idx] = lat_vals[k]
    lon_array[row_idx, col_idx] = lon_vals[k]
    geoid_array[row_idx, col_idx] = geoid_vals[k]

    for field, array in arrays.items():
        arrays[field][row_idx, col_idx] = tempGeo_np[field][k]


def get_moore_neighborhood_vals(d_array, row_idx, col_idx):
    offsets = [
        (-1, -1),  # upper left (NW)
        (-1,  0),  # directly above (N)
        (-1,  1),  # upper right (NE)
        (0, -1),   # left (W)
        (0,  1),   # right (E)
        (1, -1),   # lower left (SW)
        (1,  0),   # directly below (S)
        (1,  1)    # lower right (SE)
    ]

    neighborhood = []
    for dr, dc in offsets:
        neighbor_row = row_idx + dr
        neighbor_col = col_idx + dc
        neighborhood.append(d_array[neighbor_row, neighbor_col])
    return neighborhood


moore_ref = {
    "NW": {"angle": 315, "sin": None, "cos": None, "offset": (-1, -1)},
    "N":  {"angle": 0,   "sin": None, "cos": None, "offset": (-1, 0)},
    "NE": {"angle": 45,  "sin": None, "cos": None, "offset": (-1, 1)},
    "W":  {"angle": 270, "sin": None, "cos": None, "offset": (0, -1)},
    "E":  {"angle": 90,  "sin": None, "cos": None, "offset": (0, 1)},
    "SW": {"angle": 225, "sin": None, "cos": None, "offset": (1, -1)},
    "S":  {"angle": 180, "sin": None, "cos": None, "offset": (1, 0)},
    "SE": {"angle": 135, "sin": None, "cos": None, "offset": (1, 1)}
}

moore_labels = list(moore_ref.keys())

cos_components = []
sin_components = []
for key, item in moore_ref.items():
    angle = item["angle"]
    angleRad = angle * (math.pi / 180)
    sin_val = math.sin(angleRad)
    cos_val = math.cos(angleRad)
    moore_ref[key]["sin"] = sin_val
    moore_ref[key]["cos"] = cos_val
    cos_components.append(cos_val)
    sin_components.append(sin_val)

cos_components = np.array(cos_components, dtype=np.float64)
sin_components = np.array(sin_components, dtype=np.float64)

DOM_DIR_IDX = {
    "NW": (-1, 1),
    "N":  (0, 1),
    "NE": (1, 1),
    "W":  (-1, 0),
    "E":  (1, 0),
    "SW": (-1, -1),
    "S":  (0, -1),
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

def process_moore_vectorized(elv_array, lat_array, lon_array, geoid_array):
    """Vectorized processing of all neighborhoods at once."""
    rows, cols = elv_array.shape

    n_points = (rows - 2) * (cols - 2)
    results = {
        'lat': np.full(n_points, np.nan),
        'lon': np.full(n_points, np.nan),
        'lstf': np.full(n_points, np.nan),
        'lstf_serc': np.full(n_points, np.nan),
        'lstf_arc': np.full(n_points, np.nan),
        'lstf_flag': np.full(n_points, np.nan),
        'ndvi': np.full(n_points, np.nan),
        'ndvi_serc': np.full(n_points, np.nan),
        'ndvi_arc': np.full(n_points, np.nan),
        'ndvi_flag': np.full(n_points, np.nan),
        'ndmi': np.full(n_points, np.nan),
        'ndmi_serc': np.full(n_points, np.nan),
        'ndmi_arc': np.full(n_points, np.nan),
        'ndmi_flag': np.full(n_points, np.nan),
        'elv_rel': np.full(n_points, np.nan),
        'elv': np.full(n_points, np.nan),
        'idx_row': np.full(n_points, np.nan),
        'idx_col': np.full(n_points, np.nan),
        'mdrdasp': np.full(n_points, np.nan),
        'mdrdconc': np.full(n_points, np.nan),
        'mdrdgrv': np.full(n_points, np.nan),
        'mdrdunp': np.full(n_points, np.nan),
        'mdconst': np.full(n_points, np.nan),
        'mdbldg': np.full(n_points, np.nan),
        'lstf_ndvi_corr': np.full(n_points, np.nan),
        'lstf_ndmi_corr': np.full(n_points, np.nan),
        'ndvi_ndmi_corr': np.full(n_points, np.nan),
        'lstf_ndvi_pval': np.full(n_points, np.nan),
        'lstf_ndmi_pval': np.full(n_points, np.nan),
        'ndvi_ndmi_pval': np.full(n_points, np.nan),
        'lstf_temporal': np.full(n_points, None, dtype=object),
        'ndvi_temporal': np.full(n_points, None, dtype=object),
        'ndmi_temporal': np.full(n_points, None, dtype=object),
        'ls_land_cover': np.full(n_points, None, dtype=object),
        'cl_land_cover': np.full(n_points, None, dtype=object),
        'anisotropy': np.full(n_points, np.nan),
        'cgap': np.full(n_points, np.nan),
        'downhill_fraction': np.full(n_points, np.nan),
        'cgap_uphill': np.full(n_points, np.nan),
        'uphill_fraction': np.full(n_points, np.nan),
        'dom_angle_deg': np.full(n_points, np.nan),
        'dom_direction': np.full(n_points, None, dtype=object),
        'dom_ptb_lat': np.full(n_points, np.nan),
        'dom_ptb_lon': np.full(n_points, np.nan),
        'dom_dir_elv': np.full(n_points, np.nan),
        'dd_geoid': np.full(n_points, None, dtype=object),
        'geoid': np.full(n_points, None, dtype=object),
        'terrain_classification': np.full(n_points, None, dtype=object),
        'tri': np.full(n_points, None, dtype=object)
    }

    #for field in arrays:
    #    results[field] = 

    neighbors = {
        'NW': elv_array[0:rows-2, 0:cols-2],
        'N':  elv_array[0:rows-2, 1:cols-1],
        'NE': elv_array[0:rows-2, 2:cols],
        'W':  elv_array[1:rows-1, 0:cols-2],
        'E':  elv_array[1:rows-1, 2:cols],
        'SW': elv_array[2:rows,   0:cols-2],
        'S':  elv_array[2:rows,   1:cols-1],
        'SE': elv_array[2:rows,   2:cols]
    }

    neighbors_lat = {
        'NW': lat_array[0:rows-2, 0:cols-2],
        'N':  lat_array[0:rows-2, 1:cols-1],
        'NE': lat_array[0:rows-2, 2:cols],
        'W':  lat_array[1:rows-1, 0:cols-2],
        'E':  lat_array[1:rows-1, 2:cols],
        'SW': lat_array[2:rows,   0:cols-2],
        'S':  lat_array[2:rows,   1:cols-1],
        'SE': lat_array[2:rows,   2:cols]
    }

    neighbors_lon = {
        'NW': lon_array[0:rows-2, 0:cols-2],
        'N':  lon_array[0:rows-2, 1:cols-1],
        'NE': lon_array[0:rows-2, 2:cols],
        'W':  lon_array[1:rows-1, 0:cols-2],
        'E':  lon_array[1:rows-1, 2:cols],
        'SW': lon_array[2:rows,   0:cols-2],
        'S':  lon_array[2:rows,   1:cols-1],
        'SE': lon_array[2:rows,   2:cols]
    }

    center_elv = elv_array[1:rows-1, 1:cols-1]
    center_lat = lat_array[1:rows-1, 1:cols-1]
    center_lon = lon_array[1:rows-1, 1:cols-1]
    center_geoid = geoid_array[1:rows-1, 1:cols-1]

    moore_elvs = np.stack([neighbors[k] for k in moore_labels], axis=0)
    moore_lats = np.stack([neighbors_lat[k] for k in moore_labels], axis=0)
    moore_lons = np.stack([neighbors_lon[k] for k in moore_labels], axis=0)

    elv_diffs = moore_elvs - center_elv[np.newaxis, :, :]

    dists = np.array(NEIGHBOR_DISTS, dtype=np.float64)[:, np.newaxis, np.newaxis]
    gradients = elv_diffs / dists

    g_abs = np.abs(gradients)
    g_min = np.nanmin(g_abs, axis=0, keepdims=True)
    g_max = np.nanmax(g_abs, axis=0, keepdims=True)
    g_range = g_max - g_min
    g_range = np.where(g_range == 0, 1, g_range)

    g_abs_norm = (g_abs - g_min) / g_range
    g_abs_norm = np.nan_to_num(g_abs_norm, nan=0.0)

    cos_comp = cos_components[:, np.newaxis, np.newaxis]
    sin_comp = sin_components[:, np.newaxis, np.newaxis]

    Rx = np.sum(g_abs_norm * cos_comp, axis=0)
    Ry = np.sum(g_abs_norm * sin_comp, axis=0)
    weight_sum = np.sum(g_abs_norm, axis=0) + 1e-9

    anisotropy = np.sqrt(Rx**2 + Ry**2) / weight_sum

    dominant_angle = np.arctan2(Ry, Rx)
    dominant_angle_deg = np.degrees(dominant_angle)
    bearing = (90 - dominant_angle_deg) % 360

    downhill_mask = gradients < 0
    uphill_mask = gradients > 0

    downhill_count = np.sum(downhill_mask & ~np.isnan(gradients), axis=0)
    uphill_count = np.sum(uphill_mask & ~np.isnan(gradients), axis=0)

    downhill_fraction = downhill_count / 8.0
    uphill_fraction = uphill_count / 8.0

    tri = calc_moore_tri(center_elv, moore_elvs)

    angle_values = np.array([moore_ref[k]["angle"] for k in moore_labels], dtype=np.float64)

    idx = 0
    for i in range(rows - 2):
        for j in range(cols - 2):
            if np.isnan(center_elv[i, j]):
                idx += 1
                continue

            anis = anisotropy[i, j]
            dhf = downhill_fraction[i, j]

            bear = bearing[i, j]
            idx_dom = np.argmin(np.abs(bear - angle_values))
            dom_dir = moore_labels[idx_dom]

            grads_valid = gradients[:, i, j][~np.isnan(gradients[:, i, j])]
            dh = grads_valid[grads_valid < 0]
            uh = grads_valid[grads_valid > 0]

            cgap = None
            if len(dh) >= 2:
                dh_sorted = np.sort(np.abs(dh))[::-1]
                cgap = float(dh_sorted[0] - dh_sorted[1])

            cgap_up = None
            if len(uh) >= 2:
                uh_sorted = np.sort(np.abs(uh))[::-1]
                cgap_up = float(uh_sorted[0] - uh_sorted[1])

            results['anisotropy'][idx] = anis
            results['cgap'][idx] = cgap
            results['downhill_fraction'][idx] = dhf
            results['cgap_uphill'][idx] = cgap_up
            results['uphill_fraction'][idx] = uphill_fraction[i, j]
            results['dom_angle_deg'][idx] = dominant_angle_deg[i, j]
            results['dom_direction'][idx] = dom_dir
            results['dom_ptb_lat'][idx] = moore_lats[idx_dom, i, j]
            results['dom_ptb_lon'][idx] = moore_lons[idx_dom, i, j]
            results['dom_dir_elv'][idx] = moore_elvs[idx_dom, i, j]
            results['geoid'][idx] = center_geoid[i, j]
            results['tri'][idx] = tri[i, j]

            for field, array in arrays.items():
                #results[field][idx] = (array[i+1, j+1])[i, j]
                results[field][idx] = array[i, j]

            if dom_dir in DOM_DIR_IDX:
                offset = DOM_DIR_IDX[dom_dir]
                ri = i + 1 + offset[0]
                ci = j + 1 + offset[1]
                results['dd_geoid'][idx] = geoid_array[ri, ci]

            if anis <= 0.35 and dhf >= 0.5:
                results['terrain_classification'][idx] = "DSS"

            idx += 1

    return results


print(f"Processing {rows-2} x {cols-2} = {(rows-2)*(cols-2)} points...")
start_time = datetime.now()

print("process_moore_vectorized...")
results = process_moore_vectorized(elv_array, lat_array, lon_array, geoid_array)

print("Building results dataframe...")

# -------------------------------------------------------------------
# Build results DataFrame with less Python scalar churn
# -------------------------------------------------------------------
results_df = pd.DataFrame({
    'geoid': results['geoid'],
    'lat': results['lat'],
    'lon': results['lon'],
    'lstf': results['lstf'],
    'lstf_serc': results['lstf_serc'],
    'lstf_arc': results['lstf_arc'],
    'lstf_flag': results['lstf_flag'],
    'ndvi': results['ndvi'],
    'ndvi_serc': results['ndvi_serc'],
    'ndvi_arc': results['ndvi_arc'],
    'ndvi_flag': results['ndvi_flag'],
    'ndmi': results['ndmi'],
    'ndmi_serc': results['ndmi_serc'],
    'ndmi_arc': results['ndmi_arc'],
    'ndmi_flag': results['ndmi_flag'],
    'elv_rel': results['elv_rel'],
    'elv': results['elv'],
    'idx_row': results['idx_row'],
    'idx_col': results['idx_col'],
    'mdrdasp': results['mdrdasp'],
    'mdrdconc': results['mdrdconc'],
    'mdrdgrv': results['mdrdgrv'],
    'mdrdunp': results['mdrdunp'],
    'mdconst': results['mdconst'],
    'mdbldg': results['mdbldg'],
    'lstf_ndvi_corr': results['lstf_ndvi_corr'],
    'lstf_ndmi_corr': results['lstf_ndmi_corr'],
    'ndvi_ndmi_corr': results['ndvi_ndmi_corr'],
    'lstf_ndvi_pval': results['lstf_ndvi_pval'],
    'lstf_ndmi_pval': results['lstf_ndmi_pval'],
    'ndvi_ndmi_pval': results['ndvi_ndmi_pval'],
    'lstf_temporal': results['lstf_temporal'],
    'ndvi_temporal': results['ndvi_temporal'],
    'ndmi_temporal': results['ndmi_temporal'],
    'ls_land_cover': results['ls_land_cover'],
    'cl_land_cover': results['cl_land_cover'],
    'terrain_classification': results['terrain_classification'],
    'slope': pd.Series([None] * len(results['geoid']), dtype=object),
    'aspect': pd.Series([None] * len(results['geoid']), dtype=object),
    'tri': pd.Series([None] * len(results['geoid']), dtype=object),
    'tpi': pd.Series([None] * len(results['geoid']), dtype=object),
    'anisotropy': results['anisotropy'],
    'cgap': results['cgap'],
    'downhill_fraction': results['downhill_fraction'],
    'cgap_uphill': results['cgap_uphill'],
    'uphill_fraction': results['uphill_fraction'],
    'dom_angle_deg': results['dom_angle_deg'],
    'dom_dir': results['dom_direction'],
    'dom_ptb_lat': results['dom_ptb_lat'],
    'dom_ptb_lon': results['dom_ptb_lon'],
    'dom_dir_elv': results['dom_dir_elv'],
    'dd_geoid': results['dd_geoid'],
    'tri': results['tri']
})

results_df = results_df.replace({np.nan: None})
results_df = results_df[results_df['geoid'].notna()].copy()

print(f"results_df rows after filtering: {len(results_df)}")

# -------------------------------------------------------------------
# Register results_df and let DuckDB perform the join
# -------------------------------------------------------------------
conn_tempGeo.register("results_temp", results_df)

print("Rebuilding terrain_composite in DuckDB...")

conn_tempGeo.execute("""
    CREATE OR REPLACE TABLE terrain_composite AS
    SELECT
        t.geoid,
        t.lat,
        t.lon,
        t.lstf, t.lstf_serc, t.lstf_arc, t.lstf_flag,
        t.ndvi, t.ndvi_serc, t.ndvi_arc, t.ndvi_flag,
        t.ndmi, t.ndmi_serc, t.ndmi_arc, t.ndmi_flag,
        t.elv_rel, t.elv,
        t.idx_row, t.idx_col,
        t.mdrdasp, t.mdrdconc, t.mdrdgrv, t.mdrdunp, t.mdconst, t.mdbldg,
        t.lstf_ndvi_corr, t.lstf_ndmi_corr, t.ndvi_ndmi_corr,
        t.lstf_ndvi_pval, t.lstf_ndmi_pval, t.ndvi_ndmi_pval,
        t.lstf_temporal, t.ndvi_temporal, t.ndmi_temporal,
        t.ls_land_cover, t.cl_land_cover,
        r.terrain_classification,
        r.slope, r.aspect, r.tri, r.tpi,
        r.anisotropy, r.cgap, r.downhill_fraction,
        r.cgap_uphill, r.uphill_fraction, r.dom_angle_deg,
        r.dom_dir, r.dom_ptb_lat, r.dom_ptb_lon, r.dom_dir_elv, r.dd_geoid
    FROM results_temp t
    INNER JOIN results_temp r
        ON t.geoid = r.geoid
""")

end_time = datetime.now()
print(f"Finished. Total runtime: {end_time - start_time}")

conn_tempGeo.close()

scriptToRun = "terrainMultispecCombine.py"
'''try:
    print("Starting 3DEP-Landsat8 composite processing...")
    script_path = os.path.join(script_dir, scriptToRun)
    #subprocess.run(["python", script_path], check=True)
    subprocess.run([sys.executable, script_path], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running {scriptToRun}: {e}")
except Exception as e:
    print(f"Unexpected error running {scriptToRun}: {e}")'''