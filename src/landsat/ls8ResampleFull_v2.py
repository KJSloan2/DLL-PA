#Import OS modules for file traversing
import os
from os import listdir
from os.path import isfile, join

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
#Define the size of the sliding resampling window and gausian kernal
resampling_size = 1
gaussianKernelSize = 1
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

dataDirectory = os.path.join(parent_dir, 'data', 'landsat', locationKey)
outputDirectory = os.path.join(parent_dir, 'output', 'landsat')

#publicTerrain_dir = os.path.join(root_dir, "frontend", "public", "landsat")

year_start = logJson["year_start"]
year_end = logJson["year_end"]

yearRange = [year_start, year_end]

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
	Creates a Gaussian kernel.
	Parameters:
		size (int): Size of the kernel (should be odd).
		sigma (float): Standard deviation of the Gaussian distribution.
	Returns:
		np.ndarray: 2D array representing the Gaussian kernel.
	"""
	kernel = np.fromfunction(
		lambda x, y: (1 / (2 * np.pi * sigma ** 2)) * np.exp(
			-((x - size // 2) ** 2 + (y - size // 2) ** 2) / (2 * sigma ** 2)
		),
		(size, size)
	)
	return kernel / np.sum(kernel)
######################################################################################
def apply_gaussian_kernel(data, kernel):
	"""
	Apply a Gaussian kernel over a 2D array of data using a sliding window.
	Parameters:
		data (np.ndarray): 2D array of data.
		kernel (np.ndarray): Gaussian kernel.
	Returns:
		np.ndarray: Result of applying the Gaussian kernel over the data.
	"""
	# Get kernel size
	kernel_size = kernel.shape[0]

	# Pad the data based on kernel size
	padding = kernel_size // 1
	padded_data = np.pad(data, pad_width=padding, mode='constant', constant_values=0)

	# Initialize the output array
	output_data = np.zeros_like(data)

	# Apply the kernel over the image
	for y in range(output_data.shape[0]):
		for x in range(output_data.shape[1]):
			# Extract the window corresponding to the size of the kernel
			window = padded_data[y:y + kernel_size, x:x + kernel_size]
			# Apply the kernel to the window
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
def window_mean(band, i, j, window_size):
	if window_size > 1:
		"""Computes the mean of a windowed section of a raster band."""
		if i + window_size > band.shape[0] or j + window_size > band.shape[1]:
			return 'pass'
		band_window = band[i:i + window_size, j:j + window_size]
		#print(band_window)
		return np.mean(band_window) if band_window.size > 1 else 'pass'
	else:
		#print(band[i][j])
		return band[i][j]
######################################################################################
######################################################################################
'''Each tif name will be added to a txt file in the resoueces folder
The txt file will inform other scipts which json files to analyize'''
######################################################################################
if locationKey not in list(listdir(os.path.join(outputDirectory))):
	#folder_path = "%s%s" % (r"output/",locationKey)
	os.mkdir(os.path.join(outputDirectory, locationKey))
	os.mkdir(os.path.join(os.path.join(outputDirectory, locationKey), 'tiles'))
######################################################################################
######################################################################################
class LSTF_Layer:
    def __init__(self, lstfRange, lstfRangeIdx, lstfArray, binaryArray):
        self.lstfRange = lstfRange
        self.lstfRangeIdx = lstfRangeIdx,
        self.lstf_array = lstfArray
        self.binary_array = binaryArray
        self.group_ids = np.zeros_like(binaryArray)
        self.buffer_size = 0.5

class NDVI_Layer:
    def __init__(self, ndviRange, ndviArray, binaryArray):
        self.ndviRange = ndviRange
        self.ndvi_array = ndviArray
        self.binary_array = binaryArray
        self.group_ids = np.zeros_like(binaryArray)
        self.buffer_size = 0.5

class NDMI_Layer:
    def __init__(self, ndmiRange, ndmiArray, binaryArray):
        self.ndmiRange = ndmiRange
        self.ndmi_array = ndmiArray
        self.binary_array = binaryArray
        self.group_ids = np.zeros_like(binaryArray)
        self.buffer_size = 0.5

######################################################################################
def dilate_mask(mask, kernel_size=3):
    kernel = np.ones((kernel_size, kernel_size), np.uint8)
    return cv2.dilate(mask, kernel, iterations=2)

years = []
files = list(listdir(os.path.join(dataDirectory)))
for fileName in files:
	fileName_split = fileName.split("_")
	year = fileName_split[-1].split(".")[0]
	years.append(year)

years_sorted = sorted(years)


def is_within_bounds(i, j, array):
    return 0 <= i < array.shape[0] and 0 <= j < array.shape[1]

def fill_from_index(array_bv, array_gi, start_i, start_j, group_id):
    directions = [(-1, 0), (1, 0), (0, -1), (0, 1)]
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
                group_id += 1


def fill_gaps(polygons, buffer_size, step_width):
    """Fill small gaps between polygons by applying a buffer."""
    if not polygons:
        return polygons
    
    # Apply buffer to each polygon
    buffered_polygons = [poly.buffer(buffer_size * step_width) for poly in polygons]
    
    # Return the buffered polygons
    return buffered_polygons

publicOutputFileName = None

# Initialize the GeoJSON FeatureCollection to store processed data
fc = {
	"type": "FeatureCollection",
	"features": []
}

for year in years_sorted:
	tileId = locationKey+"_"+str(year)
	print(locationKey)
	fName = 'LS8_'+tileId+".tif"
	fPath =  os.path.join(dataDirectory, fName)
	bands_output = []
	start_time = time.time()
	with rio.open(fPath) as src:
		print("opened: ", fName)
		src_width = src.width
		src_height = src.height
		src_bounds = src.bounds

		# Land Surface Temperature Fahrenheit
		lstfOutput = np.zeros((src_height, src_width), dtype=np.float32)
		# Normalized Difference Vegetation Index
		ndviOutput = np.zeros((src_height, src_width), dtype=np.float32)
		# Normalized Difference Moisture Index
		ndmiOutput = np.zeros((src_height, src_width), dtype=np.float32)
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
		#print('b8_lst', b8_lst.shape)
		#NDMI = (b5_nir - b6_swir1) / (b5_nir + b6_swir1)
		###############################################################################
		lstfLayers = []
		lstfRanges = []
		lstfStep = 5
		lstfOverlap = 1
		for i in range(40, 141, lstfStep):
			layer_range = [i - lstfOverlap, i + lstfStep + lstfOverlap]
			lstfLayer = LSTF_Layer(
				lstfRange=layer_range,
				lstfRangeIdx=i,
				lstfArray=np.zeros((src_height, src_width), dtype=np.float32),
				binaryArray=np.zeros((src_height, src_width), dtype=np.float32)
			)
			lstfLayers.append(lstfLayer)
			lstfRanges.append(layer_range)
		###############################################################################
		ndviLayers = []
		ndviRanges = []
		ndviStep = 0.2
		ndviOverlap = ndviStep
		for i in np.arange(-0.5, 1.2, ndviStep):
			i = float(i) 
			layer_range = [i - ndviOverlap, i + ndviStep + ndviOverlap]
			ndviLayer = NDVI_Layer(
				ndviRange=layer_range,
				ndviArray=np.zeros((src_height, src_width), dtype=np.float32),
				binaryArray=np.zeros((src_height, src_width), dtype=np.float32)
			)
			ndviLayers.append(ndviLayer)
			ndviRanges.append(layer_range)
		###############################################################################
		ndmiLayers = []
		ndmiRanges = []
		ndmiStep = 0.2
		ndmiOverlap = ndmiStep
		for i in np.arange(-0.5, 1.2, ndmiStep):
			i = float(i)
			layer_range = [i - ndmiOverlap, i + ndmiStep + ndmiOverlap]
			ndmiLayer = NDMI_Layer(
				ndmiRange=layer_range,
				ndmiArray=np.zeros((src_height, src_width), dtype=np.float32),
				binaryArray=np.zeros((src_height, src_width), dtype=np.float32)
			)
			ndmiLayers.append(ndmiLayer)
			ndmiRanges.append(layer_range)
		###############################################################################
		def get_valid(band_data):
			"""Replace NaNs with zeros and retain original shape."""
			return np.nan_to_num(band_data, nan=0)

		# Get valid rows, columns, and values for each band
		valid_b5_nir = get_valid(b5_nir)
		valid_b4_red = get_valid(b4_red)
		valid_b3_green = get_valid(b3_green)
		valid_b2_blue = get_valid(b2_blue)
		valid_b8_lst = get_valid(b8_lst)

		rgb_stack = np.stack((valid_b4_red, valid_b3_green, valid_b2_blue), axis=-1)
		rgb_stack = rgb_stack.astype(np.float32)
		for i in range(3):
			band_min, band_max = np.percentile(rgb_stack[:, :, i], (2, 98))
			rgb_stack[:, :, i] = np.clip((rgb_stack[:, :, i] - band_min) / (band_max - band_min), 0, 1)
		
		red_8bit = rgb_stack[:, :, 0]
		green_8bit = rgb_stack[:, :, 1]
		blue_8bit = rgb_stack[:, :, 2]
		
		gaussian = gaussian_kernel(gaussianKernelSize, sigma=1)

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

		step_width = bb_width/(src_width/resampling_size)
		step_height = bb_height/(src_height/resampling_size)*-1

		bb_pt3 = [bb_pt1[0],bb_pt2[1]]
		bb_pt4 = [bb_pt2[0],bb_pt1[1]]

		coord_y = bb_pt3[1]
		for i in range(1,src_height-(resampling_size+1),resampling_size):
			coord_x = bb_pt3[0]
			for j in range(1,src_width-(resampling_size+1),resampling_size):

				b4_red_windowMean = window_mean(b4_red_smoothed, i, j, resampling_size)
				b3_green_windowMean = window_mean(b3_green_smoothed, i, j, resampling_size)
				b2_blue_windowMean = window_mean(b2_blue_smoothed, i, j, resampling_size)
				b5_nir_windowMean = window_mean(b5_nir_smoothed, i, j, resampling_size)
				red_8bit_windowMean = window_mean(red_8bit_smoothed, i, j, resampling_size)
				green_8bit_windowMean = window_mean(green_8bit_smoothed, i, j, resampling_size)
				blue_8bit_windowMean = window_mean(blue_8bit_smoothed, i, j, resampling_size)

				lsft = window_mean(lst_smoothed, i, j, resampling_size)
				lsft = round((lsft),2)

				ndvi = float((b5_nir_windowMean - b4_red_windowMean)/(b5_nir_windowMean + b4_red_windowMean))
				ndvi = round((ndvi),2)

				ndmi = float((b5_nir_windowMean - b6_swir1[i][j])/(b5_nir_windowMean + b6_swir1[i][j]))
				ndmi = round((ndmi),2)
				###########################################
				coordsOutput[i][j] = [coord_x, coord_y]
				###########################################
				for lstfRange, lstfLayer in zip(lstfRanges, lstfLayers):
					lstfOutput[i][j] = lsft
					if lstfRange[0] <= lsft < lstfRange[1]:
						#lsft = normalize_linear_instance(lsft, lstfRange[0], lstfRange[1])
						lsft = round((lsft),2)
						lstfLayer.lstf_array[i][j] = lsft
						lstfLayer.binary_array[i][j] = 1
						break
				###########################################
				for ndviRange, ndviLayer in zip(ndviRanges, ndviLayers):
					ndviOutput[i][j] = ndvi
					if ndviRange[0] <= ndvi < ndviRange[1]:
						ndvi = round((ndvi),2)
						ndviLayer.ndvi_array[i][j] = ndvi
						ndviLayer.binary_array[i][j] = 1
						break
				###########################################
				for ndmiRange, ndmiLayer in zip(ndmiRanges, ndmiLayers):
					ndmiOutput[i][j] = ndmi
					if ndmiRange[0] <= ndmi < ndmiRange[1]:
						ndmi = round((ndmi),2)
						ndmiLayer.ndmi_array[i][j] = ndmi
						ndviLayer.binary_array[i][j] = 1
						break
				###########################################
				coord_x+=step_width
			coord_y+=step_height
	src.close()

	layerSets = {"lstf": lstfLayers, "ndvi": ndviLayers, "ndmi": ndmiLayers}
	for layerSetKey, layerSet in layerSets.items():
		for spectralLayer in layerSet:
			# Reduce padding and dilation
			padded_binary = np.pad(spectralLayer.binary_array, pad_width=1, mode='constant', constant_values=0)
			dilated_binary = dilate_mask(padded_binary.astype(np.uint8) * 255, kernel_size=2)
			padded_groups = np.pad(spectralLayer.group_ids, pad_width=1, mode='constant', constant_values=0)
			# Perform segmentation on dilated mask
			assign_group_ids(dilated_binary > 0, padded_groups)
			
			# Remove padding
			spectralLayer.group_ids = padded_groups[1:-1, 1:-1]
			
			# Get unique groups
			unique_groups = np.unique(spectralLayer.group_ids)
			unique_groups = unique_groups[unique_groups != 0]
			
			for group_id in unique_groups:
				group_mask = spectralLayer.group_ids == group_id
				mask_uint8 = (group_mask).astype(np.uint8) * 255
				
				# Reduce final dilation kernel size
				dilated_mask = dilate_mask(mask_uint8, kernel_size=2)
				
				if np.sum(dilated_mask) > 0:
					contours, hierarchy = cv2.findContours(
						dilated_mask,  # Use dilated mask for contour finding
						cv2.RETR_TREE,
						cv2.CHAIN_APPROX_SIMPLE
					)
					
					if contours:
						# Process all contours and their hierarchies
						polygons = []
						holes = []
						
						# hierarchy format: [Next, Previous, First_Child, Parent]
						hierarchy = hierarchy[0]  # Remove single-dimensional wrapper
						
						for idx, (contour, h) in enumerate(zip(contours, hierarchy)):
							# Convert contour points to geographic coordinates
							geo_coords = []
							for point in contour:
								x = point[0][0]
								y = point[0][1]
								coord_x = bb_pt1[0] + (x * step_width)
								coord_y = bb_pt2[1] - (y * abs(step_height))
								geo_coords.append([coord_x, coord_y])

							# Create polygon from coordinates
							if len(geo_coords) >= 3:
								# If contour has no parent (-1), it's an exterior ring
								if h[3] == -1:
									polygons.append(Polygon(geo_coords))
								# If contour has a parent, it's a hole
								else:
									holes.append((h[3], Polygon(geo_coords)))
						
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
							# Apply buffer to each polygon based on the layer's buffer_size
							buffered_polygons = [poly.buffer(spectralLayer.buffer_size * step_width) for poly in polygons]
							
							# Create MultiPolygon from buffered polygons instead of original ones
							multi_poly = MultiPolygon(buffered_polygons)
							# Simplify the MultiPolygon
							simplified = multi_poly.simplify(tolerance=0.00001, preserve_topology=True)
							
							# Create GeoJSON feature
							spectralLayerPropKey_range = layerSetKey + "Range"
							spectralLayerPropKey_rangeIdx = layerSetKey + "RangeIdx"
							geojsonPropKey_range = layerSetKey + "_range"
							#geojsonPropKey_rangeIdx = layerSetKey + "_range_idx"

							fc["features"].append({
								"type": "Feature",
								"properties": {
									"spectral_type": layerSetKey,
									"year": int(year),
									geojsonPropKey_range: getattr(spectralLayer, spectralLayerPropKey_range),
									#geojsonPropKey_rangeIdx: getattr(spectralLayer, spectralLayerPropKey_rangeIdx),
									"group_id": int(group_id),
									"prct_land": round(((len(contours[0]) / totalRasters) * 100), 2),
									"area": multi_poly.area,
									"hole_count": len(holes)
								},
								"geometry": mapping(simplified)
							})

	##############################################################################
	# Write data to Numpy arrays
	# Numpy arrays are used in ls8Temporal.py for temporal composite analysis
	npy_dir = os.path.join(outputDirectory, locationKey, 'arrays')
	if not os.path.exists(npy_dir):
		os.makedirs(npy_dir)

	# Write lstf and ndvi arrays for each year
	np.save(os.path.join(npy_dir, f'lstf_{tileId}.npy'), lstfOutput)
	np.save(os.path.join(npy_dir, f'ndvi_{tileId}.npy'), ndviOutput)
	np.save(os.path.join(npy_dir, f'ndmi_{tileId}.npy'), ndmiOutput)

	# Write coordinate reference array for the most recent year
	if year == years_sorted[-1]:
		np.save(os.path.join(npy_dir, f'geo_{locationKey}.npy'), coordsOutput)
	##############################################################################
	####################### Write Output & Log Runtime Stats #####################
	##############################################################################
	publicOutputFileName = f'{locationKey}_ls8.geojson'

	#output_path = "%s%s%s%s%s%s" % (r"data/",locationKey,"/tiles/",str(year),"/json/",publicOutputFileName)
	output_path = os.path.join(outputDirectory, locationKey, 'tiles', f'LS8_{tileId}.geojson')
	with open(output_path, "w", encoding='utf-8') as output_json:
		json.dump(fc, output_json, ensure_ascii=False, indent=2, default=float)
	
	'''with open(os.path.join(publicTerrain_dir, publicOutputFileName), "w", encoding='utf-8') as output_json:
		json.dump(fc, output_json, ensure_ascii=False, indent=2, default=float)'''

	end_time = time.time()
	duration = end_time - start_time
	#Update the analysis parameters file with runtime stats
	logJson["run_stats"]["preprocessing"][str(year)+"_"+tileId] = {
		"start_time":start_time,
		"end_time":end_time,
		"duration":duration
		}
		
logJson["ls8_bounds"][str(year)]["step_width"] = step_width
logJson["ls8_bounds"][str(year)]["step_height"] = step_height
logJson["ls8_bounds"][str(year)]["resampling_size"] = resampling_size

with open(os.path.join(parent_dir, 'resources', "log.json"), "w", encoding='utf-8') as json_log:
	json_log.write(json.dumps(logJson, indent=2, ensure_ascii=False))

'''siteFileMap[locationKey]["landsat"] = "./landsat/"+publicOutputFileName 
with open(os.path.join(lib_dir, "siteFileMap.json"), "w", encoding='utf-8') as json_siteFileMap:
	json_siteFileMap.write(json.dumps(siteFileMap, indent=2, ensure_ascii=False))'''

print("ls8ResampleFull.py, DONE")