#Import OS modules for file traversing
import os
from os import listdir
from os.path import isfile, join

#Import time for calculating runtime stats
import time
import math
#Import json for handeling and writing data as JSON
import json

#Import numpy for statistical calculations
import numpy as np

#Import rasterio for handeling geotiffs
import rasterio as rio
from rasterio.plot import show
######################################################################################
############################### GLOBAL FUNCTIONS #####################################
######################################################################################
'''Read parameters to get analysis location, year, etc.
These parameters tell the program what files to read and how to process them'''
logJson = json.load(open(os.path.join(r"EO", "00_resources", "A0_log.json")))
locationKey = logJson["location_key"]

year_start = logJson["year_start"]
year_end = logJson["year_end"]

yearRange = [year_start, year_end]
analysis_version = logJson["analysis_version"]

logJson["run_stats"]["preprocessing"] = {}

######################################################################################
def normalize_linear_instance(val,d_min,d_max):
	'''Applies linear normalization to compress a given value between zero and one'''
	return round(((val-d_min)/(d_max-d_min)),4)

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
	"""
	Apply a Gaussian kernel over a 2D array of data using a sliding window.
	Parameters: data (np.ndarray): 2D array of data. kernel (np.ndarray): Gaussian kernel.
	Returns: np.ndarray: Result of applying the Gaussian kernel over the data.
	"""
	# Pad the data
	padded_data = np.pad(data, pad_width=2, mode='constant')

	# Apply the Gaussian filter using a sliding window
	output_data = np.zeros_like(data)
	for y in range(output_data.shape[0]):
		for x in range(output_data.shape[1]):
			window = padded_data[y:y+5, x:x+5]
			output_data[y, x] = np.sum(window * kernel)

	return output_data
######################################################################################
def haversine(pt1, pt2):
    # Radius of the Earth in meters
    R = 6371000
    # Convert latitude and longitude from degrees to radians
    lat1, lon1 = pt1[1], pt1[0]
    lat2, lon2 = pt2[1], pt2[0]
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    # Haversine formula
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    # Distance in meters
    dist_m = R * c
    # Convert meters to feet
    coef_ft = 3.28084
    dist_ft = dist_m * coef_ft
    # Convert feet to miles
    dist_ml = round(dist_ft / 5280, 2)
    return {"ft": dist_ft, "m": dist_m, "ml": dist_ml}
######################################################################################
############################### Directory Creation ###################################
######################################################################################
'''Each tif name will be added to a txt file in the resoueces folder
The txt file will inform other scipts which json files to analyize'''
######################################################################################
folders_output = list(listdir(r"EO/02_output/"))
if locationKey not in folders_output:
	folder_path = "%s%s" % (r"EO/02_output/",locationKey)
	os.mkdir(folder_path)
	os.mkdir("%s%s%s%s" % (r"EO/02_output/",locationKey,"/",analysis_version))
elif locationKey in folders_output:
	folder_path = "%s%s" % (r"EO/02_output/",locationKey)
	if analysis_version not in list(listdir(folder_path)):
		os.mkdir("%s%s%s%s" % (r"EO/02_output/",locationKey,"/",analysis_version))
######################################################################################
#logJson["processed_tifs"]["3dep"] = {}
######################################################################################
#Define the size of the sliding resampling window and gausian kernal
resampling_size = 3
kernel_size = 5
######################################################################################
############################### GeoTiff Resampling ###################################
######################################################################################
for year, tiles in logJson["tiles"].items():
	for tileId, tileGeom in tiles.items():
		fName = str(locationKey+"_"+str(year)+"_"+tileId+".tif")
		fPath =  "%s%s%s%s%s%s" % (r"EO/01_data/", locationKey, "/tiles/", str(year), "/geoTiffs/", fName)
		
		#{"value": 0.18721818181818178, "centroid": [-104.1653277, 30.2436288], "vertices": []}
		#bands_output = {"coordinates":[], "lstf":[], "ndvi":[], "rgb":[]}
		bands_output = []
		
		start_time = time.time()
		with rio.open(fPath) as src:
			print("opened: ", fName)

			#Get tif dimensions
			src_width = src.width
			src_height = src.height
			src_bounds = src.bounds

			#Get bands
			b5_nir = src.read(5)
			b4_red = src.read(4)
			b3_green = src.read(3)
			b2_blue = src.read(2)
			b8_lst = src.read(8)

			#Stack RGB bands. Used for RGB to NDVI neaural network training
			rgb_stack = np.stack((b4_red, b3_green, b2_blue), axis=-1)
			rgb_stack = rgb_stack.astype(np.float32)
			for i in range(3):
				band_min, band_max = np.percentile(rgb_stack[:, :, i], (2, 98))
				rgb_stack[:, :, i] = np.clip((rgb_stack[:, :, i] - band_min) / (band_max - band_min), 0, 1)
			
			red_8bit = rgb_stack[:, :, 0]
			green_8bit = rgb_stack[:, :, 1]
			blue_8bit = rgb_stack[:, :, 2]
			
			gaussian = gaussian_kernel(kernel_size, sigma=1)
			lst_smoothed = apply_gaussian_kernel(b8_lst, gaussian)
			lst_smoothed = np.array(lst_smoothed)

			#print(b8_lst)

			b4_red_smoothed =  np.array(apply_gaussian_kernel(b4_red, gaussian))
			b3_green_smoothed =  np.array(apply_gaussian_kernel(b3_green, gaussian))
			b2_blue_smoothed =  np.array(apply_gaussian_kernel(b2_blue, gaussian))
			b5_nir_smoothed =  np.array(apply_gaussian_kernel(b5_nir, gaussian))

			red_8bit_smoothed =  np.array(apply_gaussian_kernel(red_8bit, gaussian))
			green_8bit_smoothed =  np.array(apply_gaussian_kernel(green_8bit, gaussian))
			blue_8bit_smoothed =  np.array(apply_gaussian_kernel(blue_8bit, gaussian))

			bb_pt1 = [src_bounds[0],src_bounds[1]]
			bb_pt2 = [src_bounds[2],src_bounds[3]]
			bb_width = bb_pt2[0] - bb_pt1[0]
			bb_height = bb_pt2[1] - bb_pt1[1]

			step_width = bb_width/(src_width/resampling_size)
			step_height = bb_height/(src_height/resampling_size)*-1

			bb_pt3 = [bb_pt1[0],bb_pt2[1]]
			bb_pt4 = [bb_pt2[0],bb_pt1[1]]

			coord_y = bb_pt3[1]
			for i in range(1,src_height-(resampling_size+1),resampling_size):
				coord_x = bb_pt3[0]
				for j in range(1,src_width-(resampling_size+1),resampling_size):
					if coord_x > -96:
						print(coord_x,coord_y)

					#bands_output["coordinates"][-1].append([round((coord_x),3),round((coord_y),3)])

					lst_window = lst_smoothed[i:i + resampling_size, j:j + resampling_size]
					lstf = float(np.mean(lst_window))
					lsft = round((lstf),2)
					#bands_output["lstf"][-1].append(round((lstf),2))

					def window_mean(band,window_size):
						band_window = band[i:i + window_size, j:j + window_size]
						band_window_mean = np.mean(band_window)
						return band_window_mean

					b4_red_windowMean = window_mean(b4_red_smoothed,resampling_size)
					b3_green_windowMean = window_mean(b3_green_smoothed,resampling_size)
					b2_blue_windowMean = window_mean(b2_blue_smoothed,resampling_size)
					b5_nir_windowMean = window_mean(b5_nir_smoothed,resampling_size)
					red_8bit_windowMean = window_mean(red_8bit_smoothed,resampling_size)
					green_8bit_windowMean = window_mean(green_8bit_smoothed,resampling_size)
					blue_8bit_windowMean = window_mean(blue_8bit_smoothed,resampling_size)

					ndvi = float((b5_nir_windowMean - b4_red_windowMean)/(b5_nir_windowMean + b4_red_windowMean))
					#bands_output["ndvi"][-1].append(round((ndvi),2))
					ndvi = round((ndvi),2)

					mean_rgb = [int(red_8bit_windowMean*256), int(green_8bit_windowMean*256), int(blue_8bit_windowMean*256)]
					#bands_output["rgb"][-1].append(mean_rgb)
					rgb_r = int(red_8bit_windowMean*256)
					rgb_g = int(green_8bit_windowMean*256)
					rgb_b = int(blue_8bit_windowMean*256)

					pxlId = str(i)+"-"+str(j)
					bands_output.append({"id":pxlId, "lstf": lsft, "ndvi": ndvi, "r": rgb_r, "g": rgb_g, "b": rgb_b, "centroid": [round((coord_x),3),round((coord_y),3)], "vertices": []})

					coord_x+=step_width
				coord_y+=step_height

			src.close()
		##############################################################################
		####################### Write Output & Log Runtime Stats #####################
		##############################################################################
		output_fname = locationKey+"_"+year+"_"+tileId+"_"+analysis_version+".json"
		output_path = "%s%s%s%s%s%s" % (r"EO/01_data/",locationKey,"/tiles/",str(year),"/json/",output_fname)
		with open(output_path, "w", encoding='utf-8') as output_json:
			output_json.write(json.dumps(bands_output, ensure_ascii=False))

		end_time = time.time()
		duration = end_time - start_time
		#Update the analysis parameters file with runtime stats
		logJson["run_stats"]["preprocessing"][str(year)+"_"+tileId] = {
			"start_time":start_time,
			"end_time":end_time,
			"duration":duration
			}
		print(str(year)+"_"+tileId)
		
with open("%s%s" % (r"EO/00_resources/",locationKey+"_"+"log.json"), "w", encoding='utf-8') as output_json:
	output_json.write(json.dumps(logJson, indent=2, ensure_ascii=False))