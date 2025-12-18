from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
from shapely.geometry.base import BaseGeometry
import json
import os
import sqlite3
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
from spatial_utils import calc_group_centroid, haversine
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
conn = sqlite3.connect('ref.db')
cursor = conn.cursor()
siteName = "MildredLakeWI"
siteRadius = 1
# Query only the row you need
cursor.execute("SELECT COORDS FROM site_info WHERE NAME = ?", (siteName,))
result = cursor.fetchone()
query_point = None
query_pt = None
if result:
    # Parse the coordinate string into a tuple of floats
    # Remove parentheses and split by comma
    coord_str = result[0].strip("() ")
    coords = tuple(float(x.strip()) for x in coord_str.split(","))
    print(coords)
    lon, lat = coords
    query_pt = (lon, lat) 
    #query_point = Point(lon, lat)
else:
    print("COORDS not found in env_config")
######################################################################################
hydro_fc = {"type": "FeatureCollection","features": []}
if query_pt:
    cursor.execute("SELECT * FROM dir_ref WHERE DIR_NAME = ?", ("USCB_HYDROGRAPHY_TIGER",))
    row = cursor.fetchone()
    column_names = [description[0] for description in cursor.description]
    if row:
        row_dict = dict(zip(column_names, row))
        tiger_dirPath = row_dict["DIR_PATH"]
        hydroTiger_fName = "tl_2025_55085_areawater.geojson"
        hydroTiger_fPath = os.path.join(tiger_dirPath, hydroTiger_fName)
        with open(hydroTiger_fPath, "r") as hydroTiger_geojson_file:
            hydroTiger_geojson = json.load(hydroTiger_geojson_file)
            for feature in hydroTiger_geojson["features"]:
                feature_properties = feature["properties"]
                feature_name = feature_properties["FULLNAME"]
                
                centroid = calc_group_centroid(feature["geometry"]["coordinates"][0])
                hDist = haversine(query_pt, (centroid[1], centroid[0]))["ml"]
                if hDist <= siteRadius:
                    hydro_fc["features"].append(feature)
    
    ##################################################################################
if len(hydro_fc["features"]) > 0:
    cursor.execute("UPDATE site_info SET HAS_HYDRO_FEATURES = ? WHERE NAME = ?", ("True", siteName))
    conn.commit()

output_fName = siteName+"_hydro.geojson"
output_path = os.path.join(parent_dir, "data", "hydro", output_fName)
with open(output_path, "w") as f:
    json.dump(hydro_fc, f, indent=1)
print("HYDRO DONE")
#gridRefGeoJson_path = os.path.join(parent_dir, "resources", "grid.geojson")