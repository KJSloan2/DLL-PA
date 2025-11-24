'''
    Script to get OSM data using Overpy and save it as GeoJSON files.
    Uses the get_osm_overpy function from osmGetTools.py.
'''
import os
import sys
import json
from osmGetTools import get_osm_overpy
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import get_project_paths
from spatial_utils import haversine
######################################################################################
# Make this process into a reusable function to use across the codebase
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
ls8Bounds = logJson["ls8_bounds"]

ls8Bounds_key0 = next(iter(ls8Bounds))
ls8Bounds_item0 = ls8Bounds[ls8Bounds_key0]
bb_corner = ls8Bounds_item0["bb"][0]
ls8Bounds_item0_centroid = ls8Bounds_item0["centroid"]

# Calculate search radius as chord len from centroid to bounding box corner (in miles)
searchRadius = haversine(bb_corner, ls8Bounds_item0_centroid)["ml"]

# Format coordinates for function call
osmCoordinates = [ls8Bounds_item0_centroid[1], ls8Bounds_item0_centroid[0]]
######################################################################################
get_osm_overpy(locationKey, osmCoordinates, searchRadius,  ["highway", "building"], parent_dir)
######################################################################################