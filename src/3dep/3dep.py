import subprocess
import os
from os import listdir
from os.path import isfile, join

import time
import math
import json

import numpy as np
import csv
import rasterio as rio
from rasterio.plot import show
import sqlite3
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize, to_py_type
######################################################################################
#resampling_size: The size of the pooling window to avergae and smooth the data
resampling_size = 1
geoTigffSpatialResolution = 10
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
def get_tiff_dimensions(file_path):
	'''Gets the bounds and dimensions of a given geoTiff file'''
	try:
		with rio.open(file_path) as src:
			width = src.width
			height = src.height
		return width, height
	except Exception as e:
		print(f"Error: {e}")
		return None
######################################################################################
def geodetic_to_ecef(lat, lon, h):
	"""Convert geodetic coordinates to ECEF.
	Parameters: lat -- Latitude in degrees; lon -- Longitude in degrees; h -- Elevation in meters
	Returns: x, y, z -- ECEF coordinates in meters"""
	a = 6378137.0  # WGS-84 Earth semimajor axis (meters)
	f = 1 / 298.257223563  # WGS-84 flattening factor
	e2 = 2 * f - f ** 2  # Square of eccentricity
	# Convert latitude and longitude from degrees to radians
	lat_rad = math.radians(lat)
	lon_rad = math.radians(lon)
	# Calculate prime vertical radius of curvature
	N = a / math.sqrt(1 - e2 * math.sin(lat_rad) ** 2)
	# Calculate ECEF coordinates
	x = (N + h) * math.cos(lat_rad) * math.cos(lon_rad)
	y = (N + h) * math.cos(lat_rad) * math.sin(lon_rad)
	z = (N * (1 - e2) + h) * math.sin(lat_rad)
	return x, y, z
######################################################################################
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
def compress_and_scale(value, min_value, max_value, target_min=0, target_max=len(palette)):
	'''Performs min-max normalization to scale elevation values. Rescales normalized value
	to a target value (lenght of color palette) and returns the normalized value and index
	of the color to apply to the pixel'''
	# Step 1: Min-Max Normalization (compress between 0 and 1)
	normalized_value = (value - min_value) / (max_value - min_value)
	# Step 2: Scale to the target range (0 to 21)
	scaled_value = normalized_value * (target_max - target_min) + target_min
	# Step 3: Convert to an integer
	return [int(float(scaled_value)),normalized_value]
######################################################################################
def haversine_meters(pt1, pt2):
	'''Calulates the distance between two geographoic points. Takes curvatrure of 
	the Earth into account.'''
	# Radius of the Earth in meters
	R = 6371000
	# Convert latitude and longitude from degrees to radians'
	lat1, lon1 = pt1[1], pt1[0]
	lat2, lon2 = pt2[1], pt2[0]
	phi1 = math.radians(lat1)
	phi2 = math.radians(lat2)
	delta_phi = math.radians(lat2 - lat1)
	delta_lambda = math.radians(lon2 - lon1)
	# Haversine formula
	a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
	coef_ft = 3.28084
	# Distance in meters
	dist_m = R * c
	dist_ft = dist_m*coef_ft
	dist_ml = round((dist_ft/5280),2)
	return {"ft":dist_ft, "m":dist_m, "ml":dist_ml}
######################################################################################
def gaussian_kernel(size, sigma=1):
	"""
	Creates a gaussian kernal.
	Parameters:
		size (int): Size of the kernel (should be odd).
		sigma (float): Standard deviation of the Gaussian distribution.
	Returns: np.ndarray: 2D array representing the Gaussian kernel.
	"""
	kernel = np.fromfunction(
		lambda x, y: (1/(2*np.pi*sigma**2)) * np.exp(-((x - size//2)**2 + (y - size//2)**2)/(2*sigma**2)),
		(size, size)
	)
	return kernel / np.sum(kernel)
######################################################################################
def apply_gaussian_kernel(data, kernel):
	"""Apply a Gaussian kernel over a 2D array of data using a sliding window.
	Parameters: data (np.ndarray): 2D array of data. kernel (np.ndarray): Gaussian kernel.
	Returns: np.ndarray: Result of applying the Gaussian kernel over the data."""
	# Pad the data
	padded_data = np.pad(data, pad_width=2, mode='constant')
	# Apply the Gaussian filter using a sliding window
	output_data = np.zeros_like(data)
	for y in range(output_data.shape[0]):
		for x in range(output_data.shape[1]):
			window = padded_data[y:y+resampling_size, x:x+resampling_size]
			output_data[y, x] = np.sum(window * kernel)

	return output_data
######################################################################################
elevationData = {"coordinates":[]}
zScaler = 0.000008983152841185062

fPath_3depe = os.path.join(os.path.join(script_dir, "..", "..", "data", "3dep", "3DEPe"+str(resolution3Dep)+"_"+locationKey+".tif"))
fPath_3depe = os.path.abspath(fPath_3depe)

with rio.open(fPath_3depe) as src_elevation:
	#Make a subdict to log runtime stats for the oli data'
	start_time_oli = time.time()
	src_width = src_elevation.width
	src_height = src_elevation.height
	print(f"Width: {src_width} pixels")
	print(f"Height: {src_height} pixels")

	b1_elevation = src_elevation.read(1)
	
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
	b1_elevation = np.array(b1_elevation)
	pxl_dist = 2.7355724505070387
	width_ft = src_width*pxl_dist
	height_ft = src_height*pxl_dist
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

output = []
coord_y = bb_pt3[1]

elevation_smoothed = np.array(b1_elevation)
#slope_smoothed = apply_gaussian_kernel(b1_slope, gaussian)
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
			elv_scaled = compress_and_scale(elv, b1Elevation_min, b1Elevation_max, target_min=0, target_max=len(palette))
			
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
conn = sqlite3.connect('tempGeo.db')
cursor = conn.cursor()

cursor.execute(f"DELETE FROM three_dep")
conn.commit()

for idx, row in enumerate(output):
	#"lon":coord_x, "lat":coord_y, "elv_rel":elv_rel, "elv_real": elv, "idx_row": j, "idx_col": i}
	cursor.execute(
		f'''INSERT INTO three_dep (
			'rowId', 'idx_row', 'idx_col',
			'lat', 'lon',
			'elv_rel', 'elv', 'slope'
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
		(
			idx,
   			to_py_type(output[idx]["idx_row"]), to_py_type(output[idx]["idx_col"]),
			to_py_type(output[idx]["lat"]), to_py_type(output[idx]["lon"]), 
			to_py_type(output[idx]["elv_rel"]), to_py_type(output[idx]["elv"]), 0
		))

conn.commit()
conn.close()
######################################################################################
'''output_path = os.path.join(output_dir, locationKey + "_3DEP_terrain.csv")
with open(output_path, mode="w", newline='', encoding='utf-8') as csvfile:
    fieldnames = ["lon", "lat", "elv_rel", "elv_real", "idx_row", "idx_col"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(output)'''
######################################################################################
try:
    print("Starting 3DEP-Landsat8 composite processing...")
    script_path = os.path.join(script_dir, "3depLs8CompV2.py")
    subprocess.run(["python", script_path], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running 3depLs8CompV2.py: {e}")
except Exception as e:
    print(f"Unexpected error running 3depLs8CompV2.py: {e}")