import os
import numpy as np
import fiona
import math
from shapely.geometry import MultiLineString, Polygon, Point, mapping
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
def get_project_paths():
	script_dir = os.path.dirname(os.path.abspath(__file__))
	split_dir = str(script_dir).split("/")
	parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
	root_dir = os.path.abspath(os.path.join(parent_dir, "..",))
	return {
		"script": script_dir,
		"parent": parent_dir,
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