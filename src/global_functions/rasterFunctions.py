
import numpy as np
import math
import fiona
import rasterio as rio
#######################################################################################
# GEOTIFF HANDLING
#######################################################################################
def read_tiff(file_path):
	'''Reads a geoTiff file and returns the dataset'''
	try:
		src = rio.open(file_path)
		return src
	except Exception as e:
		print(f"Error: {e}")
		return None
#######################################################################################
def get_tiff_dimensions(src):
	'''Gets the bounds and dimensions of a given geoTiff file'''
	try:
		width = src.width
		height = src.height
		return width, height
	except Exception as e:
		print(f"Error: {e}")
		return None
######################################################################################
def make_tiff_bb(bounds):
	bb_pt1 = [bounds[0],bounds[1]]
	bb_pt2 = [bounds[2],bounds[3]]
	bb_width = bb_pt2[0] - bb_pt1[0]
	bb_height = bb_pt2[1] - bb_pt1[1]
	return {"bb": [bb_pt1, bb_pt2], "w": bb_width, "h": bb_height }
######################################################################################
def get_tiff_bounds(src):
	'''Gets the bounds and dimensions of a given geoTiff file'''
	try:
		bounds = src.bounds
		return bounds
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
	padding = kernel_size // 2
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