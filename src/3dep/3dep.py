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

#logJson = json.load(open(os.path.join('resources', "log.json")))
#backend\data\3dep\PineSprings\3DEPs10m_PineSprings.tif

'''Read parameters to get analysis location, year, etc.
These parameters tell the program what files to read and how to process them'''
'''analysis_parameters = json.load(open("%s%s" % (r"00_resources/","analysis_parameters.json")))
locationKey = analysis_parameters["location_key"]
yearRange = [analysis_parameters["year_start"],analysis_parameters["year_end"]]
analysis_version = analysis_parameters["analysis_version"]
start_time = time.time()'''
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



'''output = []
coord_y = bb_pt3[1]

slope_smoothed = np.array(b1_slope)

pool_b1Elevation = []
for i, (ei, si) in enumerate(zip(elevation_smoothed, slope_smoothed)):
	for j, (ej,sj) in enumerate(zip(ei, si)):
		pool_b1Elevation.append(round((ei[j]),2))

b1Elevation_min = min(pool_b1Elevation)
b1Elevation_max = max(pool_b1Elevation)
print("MIN-MAX: ", b1Elevation_min, b1Elevation_max)

pool_coords = [[],[]]

for i in range(1,src_height-(resampling_size+1),resampling_size):
	coord_x = bb_pt3[0]
	for j in range(1,src_width-(resampling_size+1),resampling_size):
		#bands_output["coordinates"][-1].append([round((coord_x),3),round((coord_y),3)])
		#get elevation data inside the resample window
		elevation_window = elevation_smoothed[i:i + resampling_size, j:j + resampling_size]
		elv = float(np.mean(elevation_window))
		elv = round((elv),2)

		try:
			coord_x+=step_width
			#calculate the reletive elevation
			elv_rel = elv-b1Elevation_min
			elv_scaled = compress_and_scale_color(elv, b1Elevation_min, b1Elevation_max, palette, target_min=0)
			
			output.append({"lon":coord_x, "lat":coord_y, "elv_rel":elv_rel, "elv": elv, "idx_row": j, "idx_col": i})
			pool_coords[0].append(coord_y)
			pool_coords[1].append(coord_x)
			#print(elevation_window)
			#time.sleep(1)
		except Exception as e:
			#print(e)
			#print("elevation_window: ", elevation_window)
			continue 
	coord_y+=step_height
######################################################################################
output_dir = os.path.join(parent_dir, "output", "3dep")
os.makedirs(output_dir, exist_ok=True)

with open(os.path.join(output_dir, locationKey+'_3DEP_terrain.json'), "w", encoding='utf-8') as output_json:
	output_json.write(json.dumps(output, default=json_serialize, ensure_ascii=False))
	
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)
######################################################################################
conn = duckdb.connect('tempGeo.duckdb')
cursor = conn.cursor()

cursor.execute(f"DELETE FROM three_dep")
conn.commit()
for idx, row in enumerate(output):
	#"lon":coord_x, "lat":coord_y, "elv_rel":elv_rel, "elv_real": elv, "idx_row": j, "idx_col": i}
	cursor.execute(
		"""INSERT INTO three_dep (
			rowId, idx_row, idx_col,
			lat, lon,
			elv_rel, elv, slope
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
		(
			idx,
   			to_py_type(output[idx]["idx_row"]), to_py_type(output[idx]["idx_col"]),
			to_py_type(output[idx]["lat"]), to_py_type(output[idx]["lon"]), 
			to_py_type(output[idx]["elv_rel"]), to_py_type(output[idx]["elv"]), 0
		))
	
conn.commit()
conn.close()'''


######################################################################################
output = []
coord_y = bb_pt3[1]

slope_smoothed = np.array(b1_slope)

pool_b1Elevation = []
for i, (ei, si) in enumerate(zip(elevation_smoothed, slope_smoothed)):
    for j, (ej, sj) in enumerate(zip(ei, si)):
        pool_b1Elevation.append(round(ei[j], 2))

b1Elevation_min = min(pool_b1Elevation)
b1Elevation_max = max(pool_b1Elevation)
print("MIN-MAX: ", b1Elevation_min, b1Elevation_max)

for i in range(1, src_height - (resampling_size + 1), resampling_size):
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

    coord_y += step_height

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
df = pd.DataFrame(output)

conn = duckdb.connect("tempGeo.duckdb")

conn.execute("BEGIN TRANSACTION")
conn.execute("DELETE FROM three_dep")
conn.register("three_dep_df", df)

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
""")
conn.execute("COMMIT")

conn.close()
######################################################################################
######################################################################################
try:
    print("Starting 3DEP-Landsat8 composite processing...")
    script_path = os.path.join(script_dir, "3depLs8CompV2.py")
    subprocess.run(["python", script_path], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running 3depLs8CompV2.py: {e}")
except Exception as e:
    print(f"Unexpected error running 3depLs8CompV2.py: {e}")