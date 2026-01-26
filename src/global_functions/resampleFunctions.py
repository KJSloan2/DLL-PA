import math
import numpy as np
######################################################################################
def compress_and_scale_color(value, min_value, max_value, ref_palette, target_min=0):
	'''Performs min-max normalization to scale elevation values. Rescales normalized value
	to a target value (lenght of color palette) and returns the normalized value and index
	of the color to apply to the pixel'''
	target_max=len(ref_palette)
	# Step 1: Min-Max Normalization (compress between 0 and 1)
	normalized_value = (value - min_value) / (max_value - min_value)
	# Step 2: Scale to the target range (0 to 21)
	scaled_value = normalized_value * (target_max - target_min) + target_min
	# Step 3: Convert to an integer
	return [int(float(scaled_value)),normalized_value]
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
def apply_gaussian_kernel(data, kernel, resampling_size=5):
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
