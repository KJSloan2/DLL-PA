#Import OS modules for file traversing
import os
from os import listdir
from os.path import isfile, join
import sys

#Import time for calculating runtime stats
import time
import math
#Import json for handeling and writing data as JSON
import json
import csv

#Import numpy for statistical calculations
import numpy as np
import pandas as pd

#Import rasterio for handeling geotiffs
import rasterio as rio
from rasterio.plot import show

import cv2
from shapely.geometry import Polygon, MultiPolygon, mapping
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.utils import safe_round, polygon_filter, fill_nulls, get_and_sort_files


from spatial_utils import (
    is_within_bounds,
    haversine
)

from global_functions.multispecFunctions import (
	valid_band_data,
	get_tiff_dimensions
	)

from global_functions.resampleFunctions import (
	gaussian_kernel, 
	apply_gaussian_kernel,
	#window_mean
)

from segmentationTools import (
    fill_from_index,
    assign_group_ids,
    fill_gaps
)

def window_mean(band, i, j, window_size):
	if window_size > 1:
		"""Computes the mean of a windowed section of a raster band."""
		if i + window_size > band.shape[0] or j + window_size > band.shape[1]:
			return 'pass'
		band_window = band[i:i + window_size, j:j + window_size]
		
		# Ignore zeros and NaNs in the mean calculation
		band_window = band_window[(band_window != 0) & ~np.isnan(band_window)]
		return np.mean(band_window) if band_window.size > 1 else 0
	else:
		return band[i][j]
	
######################################################################################
#Define the size of the sliding resampling window and gausian kernal
TARGET_PERIOD = "Q1"
RESAMPLE_WINDOW_SIZE = 1
GUASSIAN_KERNAL_SIZE = 1
######################################################################################
######################################################################################
'''Read parameters to get analysis location, year, etc.
These parameters tell the program what files to read and how to process them'''
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
#idx_src = split_dir.index("src")
#parent_dir = split_dir[idx_src-1]

'''lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))'''

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
logJson["ls8_bounds"] = {}

dataDirectory = os.path.join(parent_dir, 'data', 'landsat')
outputDirectory = os.path.join(parent_dir, 'output', 'landsat')

#publicTerrain_dir = os.path.join(root_dir, "frontend", "public", "landsat")

year_start = logJson["year_start"]
year_end = logJson["year_end"]

yearRange = [year_start, year_end]

logJson["run_stats"]["preprocessing"] = {}
######################################################################################
'''Each tif name will be added to a txt file in the resoueces folder
The txt file will inform other scipts which json files to analyize'''
######################################################################################
if locationKey not in list(listdir(os.path.join(outputDirectory))):
	#folder_path = "%s%s" % (r"output/",locationKey)
	os.mkdir(os.path.join(outputDirectory, locationKey))
	os.mkdir(os.path.join(os.path.join(outputDirectory, locationKey), 'tiles'))
######################################################################################
years = []
files = list(listdir(os.path.join(dataDirectory)))
for fileName in files:
	# file name ex: LS8_DustUpsRanch_2025_Q1.tif
	parts = fileName.split("_")
	period = parts[-1].split(".")[0]
	if period == TARGET_PERIOD:
		year = parts[-2]
		years.append(year)

years_sorted = sorted(years)
publicOutputFileName = None

geoCoordsExported = False
sortedFiles = get_and_sort_files(os.path.join(dataDirectory), "LS8", TARGET_PERIOD, 2)

for f in sortedFiles:
	f_parts = f.split("_")
	tileId = "_".join([f_parts[1],f_parts[2],f_parts[3]])
	locationName = f_parts[1]
	fName = f+".tif"
	fPath =  os.path.join(dataDirectory, fName)
	bands_output = []
	start_time = time.time()
	with rio.open(fPath) as src:
		print("opened: ", fName)
		src_width = src.width
		src_height = src.height

		tirs_downscaleFactor = 10
		oli_downscaleFactor = 10

		ds_height = src_height * oli_downscaleFactor
		ds_width  = src_width  * oli_downscaleFactor

		'''ds_ndvi = np.zeros((src_height*oli_downscaleFactor, src_width*oli_downscaleFactor), dtype=np.float32)
		ds_ndmi = np.zeros((src_height*oli_downscaleFactor, src_width*oli_downscaleFactor), dtype=np.float32)
		ds_lstf = np.zeros((src_height*tirs_downscaleFactor, src_width*tirs_downscaleFactor), dtype=np.float32)'''

		src_bounds = src.bounds

		# Land Surface Temperature Fahrenheit
		lstfOutput = np.zeros((src_height, src_width), dtype=np.float32)
		# Normalized Difference Vegetation Index
		ndviOutput = np.zeros((src_height, src_width), dtype=np.float32)
		# Normalized Difference Moisture Index
		ndmiOutput = np.zeros((src_height, src_width), dtype=np.float32)

		latOutput = np.zeros((src_height, src_width), dtype=np.float32)
		lonOutput = np.zeros((src_height, src_width), dtype=np.float32)
		# Reference geospatial coordinates for each pixel
		coordsOutput = np.empty((src_height, src_width), dtype=object)

		for i in range(src_height):
			for j in range(src_width):
				coordsOutput[i, j] = []

		totalRasters = src.count

		#Get bands
		b5_nir = src.read(5)
		b6_swir1 = src.read(6)
		b4_red = src.read(4)
		b3_green = src.read(3)
		b2_blue = src.read(2)
		b8_lst = src.read(8)

		# Get valid rows, columns, and values for each band
		valid_b5_nir = valid_band_data(b5_nir)
		valid_b4_red = valid_band_data(b4_red)
		valid_b3_green = valid_band_data(b3_green)
		valid_b2_blue = valid_band_data(b2_blue)
		valid_b8_lst = valid_band_data(b8_lst)

		rgb_stack = np.stack((valid_b4_red, valid_b3_green, valid_b2_blue), axis=-1)
		rgb_stack = rgb_stack.astype(np.float32)
		for i in range(3):
			band_min, band_max = np.percentile(rgb_stack[:, :, i], (2, 98))
			rgb_stack[:, :, i] = np.clip((rgb_stack[:, :, i] - band_min) / (band_max - band_min), 0, 1)
		
		red_8bit = rgb_stack[:, :, 0]
		green_8bit = rgb_stack[:, :, 1]
		blue_8bit = rgb_stack[:, :, 2]
		
		gaussian = gaussian_kernel(GUASSIAN_KERNAL_SIZE, sigma=1)

		b4_red_smoothed =  np.array(apply_gaussian_kernel(valid_b4_red, gaussian))
		b3_green_smoothed =  np.array(apply_gaussian_kernel(valid_b3_green, gaussian))
		b2_blue_smoothed =  np.array(apply_gaussian_kernel(valid_b2_blue, gaussian))
		b5_nir_smoothed =  np.array(apply_gaussian_kernel(valid_b5_nir, gaussian))
		lst_smoothed = np.array(apply_gaussian_kernel(valid_b8_lst, gaussian))

		red_8bit_smoothed =  np.array(apply_gaussian_kernel(red_8bit, gaussian))
		green_8bit_smoothed =  np.array(apply_gaussian_kernel(green_8bit, gaussian))
		blue_8bit_smoothed =  np.array(apply_gaussian_kernel(blue_8bit, gaussian))

		bb_pt1 = [src_bounds[0],src_bounds[1]]
		bb_pt2 = [src_bounds[2],src_bounds[3]]
		bb_width = bb_pt2[0] - bb_pt1[0]
		bb_height = bb_pt2[1] - bb_pt1[1]

		logJson["ls8_bounds"][year] = {
			"bb":[[src_bounds[0],src_bounds[1]], 
		 	[src_bounds[2],src_bounds[3]]]
			,"centroid":[(src_bounds[0]+src_bounds[2])/2, (src_bounds[1]+src_bounds[3])/2]
		 }

		step_width = bb_width/(src_width/RESAMPLE_WINDOW_SIZE)
		step_height = bb_height/(src_height/RESAMPLE_WINDOW_SIZE)*-1

		ds_step_width  = (bb_width  / ds_width)*0.98
		ds_step_height = (bb_height / ds_height)*0.98
		
		bb_pt3 = [bb_pt1[0],bb_pt2[1]]
		bb_pt4 = [bb_pt2[0],bb_pt1[1]]

		coord_y = bb_pt3[1]
		for i in range(1,src_height-(RESAMPLE_WINDOW_SIZE+1),RESAMPLE_WINDOW_SIZE):
			coord_x = bb_pt3[0]
			for j in range(1,src_width-(RESAMPLE_WINDOW_SIZE+1),RESAMPLE_WINDOW_SIZE):

				b4_red_windowMean = window_mean(b4_red_smoothed, i, j, RESAMPLE_WINDOW_SIZE)
				b3_green_windowMean = window_mean(b3_green_smoothed, i, j, RESAMPLE_WINDOW_SIZE)
				b2_blue_windowMean = window_mean(b2_blue_smoothed, i, j, RESAMPLE_WINDOW_SIZE)
				b5_nir_windowMean = window_mean(b5_nir_smoothed, i, j, RESAMPLE_WINDOW_SIZE)
				red_8bit_windowMean = window_mean(red_8bit_smoothed, i, j, RESAMPLE_WINDOW_SIZE)
				green_8bit_windowMean = window_mean(green_8bit_smoothed, i, j, RESAMPLE_WINDOW_SIZE)
				blue_8bit_windowMean = window_mean(blue_8bit_smoothed, i, j, RESAMPLE_WINDOW_SIZE)

				lstf = window_mean(lst_smoothed, i, j, RESAMPLE_WINDOW_SIZE)
				lstf = round((lstf),2)
				lstfOutput[i][j] = lstf

				ndvi = float((b5_nir_windowMean - b4_red_windowMean)/(b5_nir_windowMean + b4_red_windowMean))
				ndvi = round((ndvi),2)
				ndviOutput[i][j] = ndvi

				ndmi = float((b5_nir_windowMean - b6_swir1[i][j])/(b5_nir_windowMean + b6_swir1[i][j]))
				ndmi = round((ndmi),2)
				ndmiOutput[i][j] = ndmi

				latOutput[i][j] = coord_x
				lonOutput[i][j] = coord_y
				
				###########################################
				coordsOutput[i][j] = [coord_x, coord_y]
				###########################################
				coord_x+=ds_step_width
			coord_y+=ds_step_height
	src.close()

	ds_ndvi = cv2.resize(ndviOutput, (src_width * oli_downscaleFactor, src_height * oli_downscaleFactor), interpolation=cv2.INTER_LINEAR)
	ds_ndmi = cv2.resize(ndmiOutput, (src_width * oli_downscaleFactor, src_height * oli_downscaleFactor), interpolation=cv2.INTER_LINEAR)
	ds_lstf = cv2.resize(lstfOutput, (src_width * tirs_downscaleFactor, src_height * tirs_downscaleFactor), interpolation=cv2.INTER_LINEAR)

	downscaleKernalSize = 3
	for i in range(1,ds_ndvi.shape[0]-(downscaleKernalSize+1),downscaleKernalSize):
		for j in range(1,ds_ndvi.shape[1]-(downscaleKernalSize+1),downscaleKernalSize):
			ds_ndvi_windowMean = window_mean(ds_ndvi, i, j, downscaleKernalSize)
			ds_ndmi_windowMean = window_mean(ds_ndmi, i, j, downscaleKernalSize)
			ds_lstf_windowMean = window_mean(ds_lstf, i, j, downscaleKernalSize)

			ds_ndvi[i, j] = ds_ndvi_windowMean
			ds_ndmi[i, j] = ds_ndmi_windowMean
			ds_lstf[i, j] = ds_lstf_windowMean

	# Pack ds_{arrays} into pandas DataFrame and export as CSV
	lon_axis = bb_pt3[0] + np.arange(ds_width)  * ds_step_width
	lat_axis = bb_pt3[1] - np.arange(ds_height) * ds_step_height  # descending N→S

	ds_lon, ds_lat = np.meshgrid(lon_axis, lat_axis)

	lens = [ds_lat.shape[0], ds_lat.shape[1], ds_ndvi.shape[0], ds_ndvi.shape[1], ds_ndmi.shape[0], ds_ndmi.shape[1], ds_lstf.shape[0], ds_lstf.shape[1]]

	'''
	Test output
	print("Array shapes: ", lens)

	df = pd.DataFrame({
		'lat': ds_lat.flatten(),
		'lon': ds_lon.flatten(),
		'ndvi': ds_ndvi.flatten(),
		'ndmi': ds_ndmi.flatten(),
		'lstf': ds_lstf.flatten()
	})

	csv_output_path = os.path.join(outputDirectory, locationKey, f"{tileId}_resampled.csv")
	df.to_csv(csv_output_path, index=False)
	'''
	##############################################################################
	# Write data to Numpy arrays
	# Numpy arrays are used in ls8Temporal.py for temporal composite analysis
	npy_dir = os.path.join(outputDirectory, locationKey, 'arrays')
	if not os.path.exists(npy_dir):
		os.makedirs(npy_dir)

	try:
		np.save(os.path.join(npy_dir, f'lstf_{tileId}.npy'), ds_lstf)
		np.save(os.path.join(npy_dir, f'ndvi_{tileId}.npy'), ds_ndvi)
		np.save(os.path.join(npy_dir, f'ndmi_{tileId}.npy'), ds_ndmi)
	except Exception as e:
		print(f"Error saving numpy arrays for tile {tileId}: {e}")
	try:
		if geoCoordsExported == False:
			np.save(os.path.join(npy_dir, f'lat_{locationName}.npy'), ds_lat)
			np.save(os.path.join(npy_dir, f'lon_{locationName}.npy'), ds_lon)
			geoCoordsExported = True
	except NameError:
		pass
	##############################################################################

print("ls8ResampleFull.py, DONE")