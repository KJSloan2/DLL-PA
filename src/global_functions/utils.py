import os
import numpy as np
import fiona
import math
from types import NoneType
from typing import Tuple
from pathlib import Path
import re
from shapely.geometry import MultiLineString, Polygon, Point, mapping
######################################################################################
def hex_to_rgb(hex_color, scale255=False):
    """
    Convert hex color to RGB tuple.
    
    Args:
        hex_color: Hex color string (e.g., '#FF5733' or 'FF5733')
        scale255: If True, return 0-255 range; if False, return 0-1 range for matplotlib
    
    Returns:
        tuple: RGB values as (r, g, b)
    """
    if hex_color is None:
        return (0, 0, 0)  # Black for unknown values
    hex_color = hex_color.lstrip('#')
    
    if scale255:
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    else:
        return tuple(int(hex_color[i:i+2], 16)/255.0 for i in (0, 2, 4))
######################################################################################
def calc_yoy_change(data):
	storePrctDeltas = []
	storeDelta = []
	for i in range(1, len(data)):
		try:
			v1 = data[i - 1]
			v2 = data[i]
			delta = v2 - v1
			# Use absolute value to handle negative denominators correctly
			if v1 != 0:
				prctDelta = (delta / abs(v1)) * 100
			else:
				prctDelta = 0  # or float('inf') or None, depending on how you want to handle div by 0
			storeDelta.append(delta)
			storePrctDeltas.append(prctDelta)
		except:
			continue
	return {"deltas": storeDelta, "prct_deltas": storePrctDeltas}
######################################################################################
def is_nan(value):
	return np.isnan(value)

def is_infinity(value):
	return np.isinf(value)

def check_vals(vals):
	returnVals = []
	for val in vals:
		if is_nan(val):
			val = 0
		elif is_infinity(val):
			val = 0
		returnVals.append(val)
	return returnVals
######################################################################################
def fill_nulls(value, replacement):
	"""
	Return the value if it's not None or empty string, otherwise return default.
	
	Args:
		value: The value to check
		default: The replacement value (default is 0)
	
	Returns:
		The original value or the default replacement
	"""
	if value is None or value == "":
		return replacement
	return value
######################################################################################
def calc_slope(y_values):
	n = len(y_values)
	x_values = list(range(n))
	
	sum_x = sum(x_values)
	sum_y = sum(y_values)
	sum_xy = sum(x * y for x, y in zip(x_values, y_values))
	sum_x_squared = sum(x ** 2 for x in x_values)
	
	numerator = n * sum_xy - sum_x * sum_y
	denominator = n * sum_x_squared - sum_x ** 2
	
	if denominator == 0:
		return float('inf')
	
	slope = numerator / denominator
	return slope
######################################################################################
def json_serialize(obj):
	if isinstance(obj, (np.integer,)):
		return int(obj)
	elif isinstance(obj, (np.floating,)):
		return float(obj)
	elif isinstance(obj, (np.ndarray,)):
		return obj.tolist()
	else:
		raise TypeError(f"Type {type(obj)} not serializable")
######################################################################################
def flatten_array(x):
	flattened = []
	for i in x:
		if isinstance(i, (np.ndarray, list)):
			if len(i) > 0:
				flattened.append(i[0])
			else:
				flattened.append(0)  # Or float('nan') depending on how you want to handle it
		else:
			flattened.append(i)
	return flattened
######################################################################################
def make_new_geojson_feature(feature, newProperties=None):
	newFeature = {
		"type": "Feature",
		"properties":{},
		"geometry":{
			"type":feature["geometry"]["type"],
			"coordinates":feature["geometry"]["coordinates"],
		}
	}
	for propKey, propVal in feature["properties"].items():
		newFeature["properties"][propKey] = propVal
	if newProperties:
		for propKey, propVal in newProperties.items():
			if propKey not in newFeature["properties"]:
				newFeature["properties"][propKey] = propVal

	return newFeature
######################################################################################
def make_fc(srcPath, properties_to_get, return_reference_list, feature_id_property_key):
	new_fc = {
		"type": "FeatureCollection",
		"crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4269"} },
		"features": []
	}
	storeFeatureIds = []
	with fiona.open(srcPath, mode="r") as src:
		for feature in src:
			properties = feature["properties"]
			featureId = properties[feature_id_property_key]
			storeFeatureIds.append(str(featureId))

			newFeature = {
				"type": "Feature",
				"properties": {feature_id_property_key:featureId},
				"geometry": {
					"type": feature["geometry"]["type"],
					"coordinates": feature["geometry"]["coordinates"]
				}
			}
			if properties_to_get is not None:
				for prop in properties_to_get:
					newFeature["properties"][prop] = feature["properties"][prop]
			else:
				for propKey, propVal in properties.items():
					if propKey != feature_id_property_key:
						newFeature["properties"][propKey] = propVal
			new_fc["features"].append(newFeature)
		src.close()
		if return_reference_list:
			return {"fc": new_fc, "ref": storeFeatureIds}
		else:
			return new_fc
######################################################################################
def get_files(directory):
	"""
	Returns a list of files in the given folder path.
	
	Args:
		path (str): The folder path to search.

	Returns:
		List[str]: A list of file names.
	"""
	return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
######################################################################################
def get_directories(path):
	"""
	Returns a list of directories in the given folder path.
	
	Args:
		path (str): The folder path to search.

	Returns:
		List[str]: A list of directory names.
	"""
	try:
		return [name for name in os.listdir(path)
				if os.path.isdir(os.path.join(path, name))]
	except FileNotFoundError:
		print(f"Error: The path '{path}' does not exist.")
		return []
	except Exception as e:
		print(f"Error: {e}")
		return []
######################################################################################
def get_workspace_paths():
	script_dir = os.path.dirname(os.path.abspath(__file__))
	split_dir = str(script_dir).split("\\")
	workspace_folder = split_dir[(split_dir.index("src"))-1]
	parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
	root_dir = os.path.abspath(os.path.join(parent_dir, "..",))

	workspace_dir = os.path.join(parent_dir, workspace_folder)
	return {
		"script": script_dir,
		"parent": parent_dir,
		"workspace": workspace_dir,
		"root": root_dir
	}
######################################################################################
def to_py_type(val):
	"""Convert NumPy types to native Python types for SQLite"""
	if isinstance(val, (np.integer, np.int64, np.int32)):
		return int(val)
	elif isinstance(val, (np.floating, np.float64, np.float32)):
		if np.isnan(val):
			return None  # Store NaN as NULL in SQLite
		return float(val)
	elif isinstance(val, np.ndarray):
		return val.item()  # Extract scalar from 0-d array
	return val
######################################################################################
def safe_round(value, decimals=1):
	"""Safely round a value, returning None if value is None"""
	return round(value, decimals) if value is not None else None
######################################################################################
def polygon_filter(lon, lat, polygon):
	"""Check if a point falls within a polygon
	
	Args:
		lon: Longitude of the point
		lat: Latitude of the point
		polygon: A Shapely Polygon object
		
	Returns:
		bool: True if point is within polygon, False otherwise
	"""
	point = Point(lon, lat)
	return polygon.contains(point)
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
def normalize_linear1(d):
	"""
	Normalize a list of numbers to a 0-1 scale.
	Args:
		d (list of float): The list of numbers to normalize.
	"""
	min_d = min(d)
	max_d = max(d)
	range_d = max_d - min_d
	if range_d == 0:
		return [0 for _ in d]
	normalized = [(val - min_d) / range_d for val in d]
	return normalized
######################################################################################
def normalize_symmetric(d):
    """
    Normalize a list of signed values to [-1, 1] by max absolute value.
    """
    max_abs = max(abs(val) for val in d if not np.isnan(val))
    if max_abs == 0:
        return [0 for _ in d]
    return [(val / max_abs) if not np.isnan(val) else np.nan for val in d]
######################################################################################
def moore_neighborhood_idxs(row_idx, col_idx, nd=1):
	# Generate the Moore neighborhood (8 surrounding cells)
	neighbors = []
	for dr in range(-nd, nd+1):
		for dc in range(-nd, nd+1):
			if dr == 0 and dc == 0:
				continue  # Skip the center cell
			neighbors.append((row_idx + dr, col_idx + dc))
	return neighbors
################################################################################
def ensure_folder(directory: str, folder_name: str):
    folder_path = Path(directory) / folder_name
    folder_path.mkdir(parents=True, exist_ok=True)
    print(f"Ensured folder exists: {folder_path}")
    return folder_path
######################################################################################
def is_valid_wgs84(coord_str: str) -> Tuple[bool, str]:
    """
    Check if a string is a valid WGS84 coordinate pair (latitude, longitude).
    Args:
        coord_str (str): A string like "40.7128, -74.0060"
    Returns:
        (bool, str): (True, "") if valid; (False, error_message) if not.
    """
    # Match "lat, lon" with optional spaces, decimals, and special characters
    pattern = r'^\s*([-+]?\d+(\.\d+)?)\s*,\s*([-+]?\d+(\.\d+)?)\s*$'
    match = re.match(pattern, coord_str)
    if not match:
        #"NOT VALID WGS 84 FORMAT"
        return False
    try:
        lat = float(match.group(1))
        lon = float(match.group(3))
    except ValueError:
        # "Values could not be converted to floats."
        return False

    if not (-90 <= lat <= 90):
        #f"Latitude {lat} is out of range (-90 to 90)."
        return False
    if not (-180 <= lon <= 180):
        # f"Longitude {lon} is out of range (-180 to 180)."
        return False
    return True
######################################################################################
######################################################################################
def prep_coords(coords):
    """
    Parse coordinate string into (lat, lon) tuple.
    
    Args:
        coords: String in format "lat, lon" or "(lat, lon)"
        
    Returns:
        Tuple of (lat, lon) as floats
    """
    # Remove outer parentheses if present
    coords_str = coords.strip()
    if coords_str.startswith('(') and coords_str.endswith(')'):
        coords_str = coords_str[1:-1]
    
    # Split by comma
    parts = coords_str.split(",")
    
    if len(parts) != 2:
        raise ValueError(f"Invalid coordinate format: {coords}. Expected 'lat, lon'")
    
    try:
        lat = float(parts[0].strip())
        lon = float(parts[1].strip())
    except ValueError as e:
        raise ValueError(f"Could not parse coordinates '{coords}': {e}")
    
    # Validate WGS84 ranges
    if not (-90 <= lat <= 90):
        raise ValueError(f"Latitude {lat} is out of valid range (-90 to 90)")
    if not (-180 <= lon <= 180):
        raise ValueError(f"Longitude {lon} is out of valid range (-180 to 180)")
    
    return (lat, lon)
######################################################################################
