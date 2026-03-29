import os
from os import listdir
from os.path import isfile, join
import subprocess

import time
import math
import json

import numpy as np
import rasterio as rio
from rasterio.plot import show
import pandas as pd
import duckdb
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.utils import  json_serialize, to_py_type
from global_functions.resampleFunctions import (
	compress_and_scale_color,
	gaussian_kernel,
	apply_gaussian_kernel
)
from global_functions.rasterFunctions import get_tiff_dimensions
######################################################################################
#resampling_size: The size of the pooling window to average and smooth the data
resampling_size = 1
geoTigffSpatialResolution = 10
GKERNAL_SIZE = 5
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("\\")
idx_src = split_dir.index("src")
#parent_dir = os.path.join(split_dir[idx_src-2], split_dir[idx_src-1])

parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))

print("parent_dir: ", parent_dir)
log_path = os.path.join(script_dir, "..", "..", "resources", "log.json")
log_path = os.path.abspath(log_path) 

with open(log_path, "r", encoding="utf-8") as f:
    logJson = json.load(f)

locationKey = logJson["location_key"]
resolution3Dep = logJson["3dep_resolution"]
######################################################################################
#Color palette for color coding points based on elevation
palette = [
	[58, 226, 55], [181, 226, 46], [214, 226, 31], [255, 247, 5], [255, 214, 17], 
	[255, 182, 19], [255, 139, 19], [255, 110, 8], [255, 80, 13], [255, 0, 0], [222, 1, 1],
	[194, 19, 1], [6, 2, 255], [35, 92, 177], [48, 126, 243], [38, 157, 177],
	[48, 200, 226], [50, 211, 239], [59, 226, 133], [63, 243, 143], [134, 226, 111]
]
#Reverse palette order
palette = palette[::-1]
######################################################################################
elevationData = {"coordinates":[]}
ZSCALE = 0.000008983152841185062
PXL_DIST = 2.7355724505070387
fPath_3depe = os.path.join(os.path.join(script_dir, "..", "..", "data", "3dep", "3DEPe"+str(resolution3Dep)+"_"+locationKey+".tif"))
fPath_3depe = os.path.abspath(fPath_3depe)

with rio.open(fPath_3depe) as src_elevation:
	#Make a subdict to log runtime stats for the oli data'
	start_time_oli = time.time()
	src_width = src_elevation.width
	src_height = src_elevation.height
	print(f"Width: {src_width} pixels")
	print(f"Height: {src_height} pixels")
	
	#Get the bo bounds of the geotif
	src_bounds = src_elevation.bounds

	#Get the boundining box (bb) points and calc the bb width and height
	bb_pt1 = [src_bounds[0],src_bounds[1]]
	bb_pt2 = [src_bounds[2],src_bounds[3]]
	bb_width = bb_pt2[0] - bb_pt1[0]
	bb_height = bb_pt2[1] - bb_pt1[1]
	
	#Calculate the stepsize btwn pixels based on pooling window size
	step_width = bb_width/(src_width/resampling_size)
	step_height = bb_height/(src_height/resampling_size)*-1

	bb_pt3 = [bb_pt1[0],bb_pt2[1]]

	print('step_width: ', step_width, 'step_height: ', step_height)

	pts = []
	b1Elevation = np.array(src_elevation.read(1))

	gaussian = gaussian_kernel(GKERNAL_SIZE, sigma=1)
	elevation_smoothed = np.array(apply_gaussian_kernel(b1Elevation, gaussian, GKERNAL_SIZE))

	width_ft = src_width*PXL_DIST
	height_ft = src_height*PXL_DIST
	print(f"Distance: {width_ft} feet")

fPath_3deps = os.path.join(os.path.join(script_dir, "..", "..", "data", "3dep", "3DEPs"+str(resolution3Dep)+"_"+locationKey+".tif"))
fPath_3deps = os.path.abspath(fPath_3deps)

with rio.open(fPath_3deps) as src_slope:
	b1_slope = src_slope.read(1)

bands_pooled = {"coordinates":[],"elevation":[]}

output_geo = {
	"type": "FeatureCollection", 
	"name": "Landsat 8, LST and NDVI Temportal Analysis",
	"features": []
}

######################################################################################
output = []
coord_y = bb_pt3[1]

slope_smoothed = np.array(b1_slope)

'''pool_b1Elevation = []
for i, (ei, si) in enumerate(zip(elevation_smoothed, slope_smoothed)):
    for j, (ej, sj) in enumerate(zip(ei, si)):
        pool_b1Elevation.append(round(ei[j], 2))'''

#b1Elevation_min = min(pool_b1Elevation)
#b1Elevation_max = max(pool_b1Elevation)

b1Elevation_min = float(elevation_smoothed.min())
b1Elevation_max = float(elevation_smoothed.max())

print("MIN-MAX: ", b1Elevation_min, b1Elevation_max)

total_points = ((src_height - resampling_size - 1) // resampling_size) * \
               ((src_width - resampling_size - 1) // resampling_size)

rowIds = np.arange(total_points)
idx_rows = np.zeros(total_points, dtype=np.int32)
idx_cols = np.zeros(total_points, dtype=np.int32)
lats = np.zeros(total_points, dtype=np.float64)
lons = np.zeros(total_points, dtype=np.float64)
elvs = np.zeros(total_points, dtype=np.float64)
elv_rels = np.zeros(total_points, dtype=np.float64)

idx = 0
coord_y = bb_pt3[1]

for i in range(1, src_height - (resampling_size + 1), resampling_size):
    coord_x = bb_pt3[0]
    for j in range(1, src_width - (resampling_size + 1), resampling_size):
        elevation_window = elevation_smoothed[i:i + resampling_size, j:j + resampling_size]
        elv = np.mean(elevation_window)
        
        coord_x += step_width

        rowIds[idx] = idx
        idx_rows[idx] = j
        idx_cols[idx] = i
        lats[idx] = coord_y
        lons[idx] = coord_x
        elvs[idx] = elv
        elv_rels[idx] = elv - b1Elevation_min
        
        idx += 1
    
    coord_y += step_height
    
df = pd.DataFrame({
    'rowId': rowIds,
    'idx_row': idx_rows,
    'idx_col': idx_cols,
    'lat': lats,
    'lon': lons,
    'elv': np.round(elvs, 2),
    'elv_rel': np.round(elv_rels, 2),
    'slope': 0.0
})

'''for i in range(1, src_height - (resampling_size + 1), resampling_size):
    coord_x = bb_pt3[0]

    for j in range(1, src_width - (resampling_size + 1), resampling_size):
        elevation_window = elevation_smoothed[i:i + resampling_size, j:j + resampling_size]
        elv = round(float(np.mean(elevation_window)), 2)

        coord_x += step_width
        elv_rel = elv - b1Elevation_min

        output.append({
            "rowId": len(output),
            "idx_row": int(j),
            "idx_col": int(i),
            "lat": float(coord_y),
            "lon": float(coord_x),
            "elv_rel": float(elv_rel),
            "elv": float(elv),
            "slope": 0.0
        })

    coord_y += step_height'''

# Optional JSON export
output_dir = os.path.join(parent_dir, "output", "3dep")
os.makedirs(output_dir, exist_ok=True)

with open(
    os.path.join(output_dir, f"{locationKey}_3DEP_terrain.json"),
    "w",
    encoding="utf-8"
) as output_json:
    json.dump(output, output_json, ensure_ascii=False)

# Bulk load into DuckDB
#df = pd.DataFrame(output)

conn = None
max_retries = 3
retry_delay = 2

'''try:
    temp_conn = duckdb.connect("tempGeo.duckdb")
    temp_conn.execute("CHECKPOINT")
    temp_conn.close()
    time.sleep(1)
except:
    pass'''

for attempt in range(max_retries):
    try:
        conn = duckdb.connect("tempGeo.duckdb", read_only=False)
        break
    except Exception as e:
        if attempt < max_retries - 1:
            print(f"Database connection attempt {attempt + 1} failed. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        else:
            print(f"Failed to connect to database after {max_retries} attempts: {e}")
            raise

if conn is None:
    raise RuntimeError("Could not establish database connection")

print(f"DataFrame shape: {df.shape}")
print(f"Total rows to insert: {len(df):,}")

try:
    '''print("Starting database transaction...")
    conn.execute("BEGIN TRANSACTION")
    
    print("Truncating old data...")
    conn.execute("TRUNCATE TABLE three_dep")
    
    print("Registering DataFrame...")
    conn.register("three_dep_df", df)

    print("Inserting new data...")
    conn.execute("""
        INSERT INTO three_dep (
            rowId, idx_row, idx_col,
            lat, lon,
            elv_rel, elv, slope
        )
        SELECT
            rowId, idx_row, idx_col,
            lat, lon,
            elv_rel, elv, slope
        FROM three_dep_df
    """)'''
    
    conn.register("three_dep_df", df)

    conn.execute("""
        CREATE OR REPLACE TABLE three_dep AS
        SELECT
            rowId, idx_row, idx_col,
            lat, lon,
            elv_rel, elv, slope
        FROM three_dep_df
    """)

    #print("Committing transaction...")
    #conn.execute("COMMIT")
    
except KeyboardInterrupt:
    print("\nOperation interrupted by user. Rolling back changes...")
    if conn:
        try:
            conn.execute("ROLLBACK")
        except:
            pass
    raise
except Exception as e:
    print(f"Error during database operation: {e}")
    if conn:
        try:
            conn.execute("ROLLBACK")
        except:
            pass
    raise
finally:
    if conn:
        conn.close()

print("3DEP terrain data processing and loading complete.")
######################################################################################
######################################################################################
'''scriptToRun = "terrainDirectionalSignals.py"
try:
    print("Starting 3DEP-Landsat8 composite processing...")
    script_path = os.path.join(script_dir, scriptToRun)
    #subprocess.run(["python", script_path], check=True)
    subprocess.run([sys.executable, script_path], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running {scriptToRun}: {e}")
except Exception as e:
    print(f"Unexpected error running {scriptToRun}: {e}")'''