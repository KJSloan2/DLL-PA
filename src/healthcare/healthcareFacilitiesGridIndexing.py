import sqlite3
import os
import json
from shapely.geometry import Point, shape
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
gridRef = json.load(open(os.path.join(parent_dir, "resources", "grid.geojson")))

tables = ["hospitals"]
for gridFeature in gridRef["features"]:
    for tableName in tables:
        gridFeature["properties"][tableName] = []
######################################################################################
db = os.path.join(parent_dir, "data", "healthcare_sites.db")

# Connect to the SQLite database
conn = sqlite3.connect(db)
cursor = conn.cursor()
#f"Latitude: {latitude}, Longitude: {longitude}"
######################################################################################
fc = {
    "type": "FeatureCollection",
    "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4269" } },
    "features": []
}

for gridFeature in gridRef["features"]:
    updatedFeature = {
        "type": "Feature",
        "geometry": gridFeature["geometry"],
        "properties": {}
    }
    for propKey, propVal in gridFeature["properties"].items():
        updatedFeature["properties"][propKey] = propVal
    for tableName in tables:
        updatedFeature["properties"][tableName] = []
    fc["features"].append(updatedFeature)
######################################################################################
for tableName in tables:
    query = f"SELECT lat, lon, object_id FROM {tableName}"
    cursor.execute(query)

    for i, row in enumerate(cursor.fetchall()):
        lat, lon, object_id = row
        if None not in (lat, lon):
            queryPoint = Point(lon, lat)
            featureId_inside = None
            for i, feature in enumerate(fc["features"]):
                feature_shape = shape(feature["geometry"])
                if feature_shape.contains(queryPoint):
                    featureId_inside = feature["properties"]["id"]
                    feature["properties"][tableName].append(object_id)
                    print(object_id)
                    break
        else:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
            print(f"Row {i} in table {tableName} has None values for latitude or longitude.")
conn.close()

output_geojson_path = os.path.join(parent_dir, "resources", "healthcareFacilities_gridRef.geojson")
with open(output_geojson_path, "w", encoding='utf-8') as geojson_gridRef:
	geojson_gridRef.write(json.dumps(fc, indent=1, default=json_serialize))
######################################################################################
print("healthcareFacilitiesGridIndexing.py completed successfully.")