import numpy as np
import rasterio as rio
######################################################################################
def classify_land_cover(ndvi, ndmi):
	"""
	Classify a pixel using NDVI + NDMI ranges.
	Returns:
		"UN"  -> Unclassified
		"DW"  -> Deep Water
		"SW"  -> Shallow Water
		"WS"  -> Wet Soil
		"V"   -> Vegetation
		"BN"  -> Barren
	"""
	# "Unclassified"
	landCoverClassification = "UN"
	# --- Deep Water ---
	# NDVI: -1.0 to -0.05 | NDMI: -1.0 to -0.4
	if -1.0 <= ndvi <= -0.05 and -1.0 <= ndmi <= -0.4:
		landCoverClassification = "DW"
		
	# --- Shallow Water ---
	# NDVI: -0.05 to 0.05 | NDMI: -0.4 to -0.2
	if -0.05 < ndvi <= 0.05 and -0.4 < ndmi <= -0.2:
		landCoverClassification = "SW"

	# --- Wet Soil ---
	# NDVI: 0 to 0.2 | NDMI: -0.2 to 0.1
	if 0.0 < ndvi <= 0.2 and -0.2 < ndmi <= 0.1:
		landCoverClassification = "WS"

	# --- Vegetation ---
	# NDVI: 0.2 to 0.9 | NDMI: 0.1 to 0.7
	if 0.2 < ndvi <= 0.9 and 0.1 < ndmi <= 0.7:
		landCoverClassification = "V"

	# --- Barren ---
	# NDVI: 0 to 0.3 | NDMI: -0.1 to 0.2 
	if 0.0 <= ndvi <= 0.3 and -0.1 <= ndmi <= 0.2:
		landCoverClassification = "BN"
		
	return landCoverClassification

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
def valid_band_data(band_data):
    """Replace NaNs with zeros and retain original shape."""
    return np.nan_to_num(band_data, nan=0)