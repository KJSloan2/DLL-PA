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
proj_wgs84 = pyproj.CRS("EPSG:4326")
proj_meters = pyproj.CRS("EPSG:3857")
project = pyproj.Transformer.from_crs(proj_wgs84, proj_meters, always_xy=True).transform
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
def apply_gaussian_kernel(data, kernel, resampling_size):
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
def get_moore_neighborhood(mask, array, i, j):
    # Perform CA-based flood-filled segmentation to segment pixel masks into individual groups.
    idx_notZero = np.where(mask == 1.0)
    max_rowIdx = max(idx_notZero[0])
    max_columnIdx = max(idx_notZero[1])
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