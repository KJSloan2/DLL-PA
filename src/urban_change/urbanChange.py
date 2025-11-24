import os
from os import listdir
from os.path import isfile, join

import time
import math
import json

import numpy as np

import rasterio as rio
from rasterio.plot import show

import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

from scipy.spatial import ConvexHull
from shapely.geometry import Polygon
from shapely.ops import transform
import pyproj

######################################################################################
proj_wgs84 = pyproj.CRS("EPSG:4326")  # WGS84
proj_meters = pyproj.CRS("EPSG:3857")

project = pyproj.Transformer.from_crs(proj_wgs84, proj_meters, always_xy=True).transform
######################################################################################
resampling_size = 1
# Define the location ID and start and end years of the analysis period
locationId = 'SGF'
yearStart = 2021
yearEnd = 2024
######################################################################################
'''script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
idx_src = split_dir.index("src")
parent_dir = split_dir[idx_src-1]

logJson = json.load(open(os.path.join('resources', "log.json")))'''

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
	return [int(scaled_value),normalized_value]
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
# Make the file names of the UCA tif files to process
filesToProcess = []
for year in range(yearStart, yearEnd, 1):
	fileName = 'UCA_'+locationId+'_'+str(year)+'-'+str(year+1)
	filesToProcess.append(fileName)

# Cycle over the list of UCA files to process and process each file
for ucaFileName in filesToProcess:
	with rio.open("%s%s" % (r'eo_v3/data/urban_change/',str(ucaFileName+'.tif'))) as src_uc:
		b1_mask = src_uc.read(1)
		src_width = src_uc.width
		src_height = src_uc.height
		src_bounds = src_uc.bounds

		#Get the boundining box (bb) points and calc the bb width and height
		bb_pt1 = [src_bounds[0],src_bounds[1]]
		bb_pt2 = [src_bounds[2],src_bounds[3]]
		bb_pt3 = [bb_pt1[0], bb_pt2[1]]
		bb_pt4 = [bb_pt2[0], bb_pt1[1]]
		bb_width = bb_pt2[0] - bb_pt1[0]
		bb_height = bb_pt2[1] - bb_pt1[1]

		bb_polygon = Polygon([bb_pt3, bb_pt2, bb_pt4, bb_pt1, bb_pt3])
		#bb_area = round((bb_polygon.area),2)

		bb_polygon_projected = transform(project, bb_polygon)
		bb_area_sq_meters = bb_polygon_projected.area
		bb_area = bb_area_sq_meters * 3.861e-7
		bb_area = round((bb_area),2)
		
		b1_mask = np.array(b1_mask)
		pxl_dist = 2.7355724505070387
		width_ft = src_width*pxl_dist
		height_ft = src_height*pxl_dist

		print('bb_area: ', bb_area)
		print(src_width, src_height)
		print(width_ft, height_ft)
		print(bb_pt3, bb_pt2, bb_pt4, bb_pt1, bb_pt3)

		#Calculate the stepsize btwn pixels based on pooling window size
		step_width = bb_width/(src_width/resampling_size)
		step_height = bb_height/(src_height/resampling_size)*-1

		print('step_width: ', step_width, 'step_height: ', step_height)

		pxl_dist = 2.7355724505070387
		width_ft = src_width*pxl_dist
		height_ft = src_height*pxl_dist
		print(f"Distance: {width_ft} feet")

		output = []

		idx_notZero = np.where(b1_mask == 1.0)
		print(len(idx_notZero[0]))

		mask_groupIds = np.zeros((src_height, src_width))
		mask_binary = np.zeros((src_height, src_width))
		for idx_i, idx_j in zip(idx_notZero[0], idx_notZero[1]):
			mask_binary[idx_i][idx_j] = 1
			#mask_groupIds[idx_i][idx_j] = 0

		######################################################################################
		######################################################################################
		# Perform CA-based flood-filled segmentation to segment pixel masks into individual groups.
		max_rowIdx = max(idx_notZero[0])
		max_columnIdx = max(idx_notZero[1])
		def get_moore_neighborhood(array, i, j):
			# Get the dimensions of the array
			rows, cols = array.shape
			# Define the boundaries for the 3x3 neighborhood, ensuring we don't go out of bounds
			row_start = max(0, i - 1)
			row_end = min(max_rowIdx, i + 2)  # i+2 because Python slicing is exclusive at the end
			col_start = max(0, j - 1)
			col_end = min(max_columnIdx, j + 2)  # j+2 because Python slicing is exclusive at the end
			# Extract the neighborhood
			neighborhood = array[row_start:row_end, col_start:col_end]
			return neighborhood
		######################################################################################
		def is_within_bounds(i, j, array):
			return 0 <= i < array.shape[0] and 0 <= j < array.shape[1]

		# Define a list of possible movements (up, down, left, right)
		directions = [(-1, 0), (1, 0), (0, -1), (0, 1)]
		######################################################################################
		# Start the process at the given index and group with a group ID
		def fill_from_index(array_bv, array_gi, start_i, start_j, group_id):
			directions = [(-1, 0), (1, 0), (0, -1), (0, 1)]
			# If the starting point is not 1 or has already been assigned a group, return early
			if array_bv[start_i, start_j] != 1 or array_gi[start_i, start_j] != 0:
				return
			# Initialize a stack for the positions to check (flood fill style)
			stack = [(start_i, start_j)]
			while stack:
				# Pop a position from the stack
				i, j = stack.pop()
				# Assign the current position to the group ID in array_gi
				array_gi[i, j] = group_id
				# Check all 4 possible adjacent cells
				for direction in directions:
					ni, nj = i + direction[0], j + direction[1]
					# Check if the new position is within bounds, has value 1, and hasn't been assigned a group ID
					if is_within_bounds(ni, nj, array_bv) and array_bv[ni, nj] == 1 and array_gi[ni, nj] == 0:
						# Add the new position to the stack for further processing
						stack.append((ni, nj))
		######################################################################################
		# Function to find and assign group IDs to all connected components in array_bv
		def assign_group_ids(array_bv, array_gi):
			group_id = 1  # Start group ID from 1
			# Loop through all the elements in the array
			for i in range(array_bv.shape[0]):
				for j in range(array_bv.shape[1]):
					# If the current position has value 1 and hasn't been assigned a group ID
					if array_bv[i, j] == 1 and array_gi[i, j] == 0:
						# Fill the connected component starting from this position with the current group ID
						fill_from_index(array_bv, array_gi, i, j, group_id)
						group_id += 1  # Increment the group ID for the next connected component
		######################################################################################
		######################################################################################
		assign_group_ids(mask_binary, mask_groupIds)
		mask_binary = np.pad(mask_binary, pad_width=1, mode='constant', constant_values=0)
		mask_groupIds = np.pad(mask_groupIds, pad_width=1, mode='constant', constant_values=0)
		
		output = {"type": "FeatureCollection", "features": []}

		unique_groupIds = sorted(set(mask_groupIds.flatten()))

		hull_points_dict = {}

		for groupId in unique_groupIds:
			groupPoints = []
			idx_groupId = np.where(mask_groupIds == groupId)
			for idx_i, idx_j in zip(idx_groupId[0], idx_groupId[1]):
				cx = bb_pt3[1] + ((step_width * idx_i)*-1)
				cy = bb_pt3[0] + ((step_height * idx_j)*-1)
				groupPoints.append((cx, cy))

			# Convert to numpy array
			groupPoints = np.array(groupPoints)

			# Check for variability in both x and y dimensions
			if np.ptp(groupPoints[:, 0]) > 0 and np.ptp(groupPoints[:, 1]) > 0:
				hull = ConvexHull(groupPoints)
				hull_points = groupPoints[hull.vertices]  # Get hull vertices
				hull_points_dict[groupId] = hull_points  # Store hull points for each group

		# Print or access the points in `hull_points_dict`
		for groupId, points in hull_points_dict.items():
			polygon_points = []
			for pt in points:
				polygon_points.append([pt[1], pt[0]])
			polygon_points.append([points[0][1],points[0][0]])
			
			group_polygon = Polygon(polygon_points)
			group_polygon_projected = transform(project, group_polygon)
			group_area_sq_meters = group_polygon_projected.area
			# Convert area to square miles (1 square meter = 3.861e-7 square miles)
			group_area = group_area_sq_meters * 3.861e-7

			######################################################################################
			def calc_group_centroid(points):
				"""
				Calculate the mean latitude and longitude of a list of points.
				Args:
					points (list of lists): List of points, where each point is [lat, lon].
				Returns:
					tuple: Mean latitude and mean longitude as (mean_lat, mean_lon).
				"""
				if not points:
					raise ValueError("The points list is empty.")
				# Separate latitudes and longitudes
				latitudes = [point[1] for point in points]
				longitudes = [point[0] for point in points]
				# Calculate means
				mean_lat = sum(latitudes) / len(latitudes)
				mean_lon = sum(longitudes) / len(longitudes)
				return mean_lat, mean_lon
			######################################################################################
			groupCentroid = calc_group_centroid(polygon_points)
			if group_area <= 10:
				group_area = round((group_area),4)
				#print('GROUP AREA: ', group_area)
				feature = {
					"type": "Feature",
					"geometry": {
						"type": "Polygon",
						"coordinates": [polygon_points]
					},
					"properties": {
						"group_id": groupId,
						'area': group_area,
						'centroid':groupCentroid
					}
				}

				output["features"].append(feature)

			#print(f"Convex Hull points for group {groupId}:")
			#print(points)

		#with open(os.path.join(r'output\urban_change\\', str(ucaFileName+'.json')), "w", encoding='utf-8') as output_geojson:
		#	output_geojson.write(json.dumps(output, ensure_ascii=False))
		with open(os.path.join(r'eo_v3/output/urban_change/', str(ucaFileName+'.geojson')), "w", encoding='utf-8') as output_json:
			output_json.write(json.dumps(output, ensure_ascii=False))

print('DONE')
