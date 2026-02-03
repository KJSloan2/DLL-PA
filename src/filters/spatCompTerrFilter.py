import os
import sys
import json
import math
from typing import List, Tuple
import sqlite3
import shapely.geometry
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#from utils import  json_serialize, make_new_geojson_feature    
from spatial_utils import haversine, mpolygon_yeild_pts, polygon_filter
#polygon_filter(lon, lat, polygon)

filterPolygonFileName = "aoi_polygon_2025-12-09.geojson"
filterPolygonFile_path = os.path.join(r"data", "spatial_filters", filterPolygonFileName)
with open(filterPolygonFile_path) as f:
    filterPolygonJson = json.load(f)
filterPolygonCoords = filterPolygonJson["features"][0]["geometry"]["coordinates"]
filterPolygon = shapely.geometry.shape(filterPolygonJson["features"][0]["geometry"])

compositeTerrainFile_path = os.path.join(r"data", "spatial_filters", "composite_terrain_2025-12-09.geojson")
######################################################################################