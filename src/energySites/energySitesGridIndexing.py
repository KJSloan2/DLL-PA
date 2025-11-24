import sqlite3
import os
import json
from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
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
filterGeomJson = json.load(open(os.path.join(parent_dir, 'resources', "grid.geojson")))
gridRef = json.load(open(os.path.join(parent_dir, "resources", "gridRef.geojson")))

tables = ["agricultural_digesters", "bea_bioenergy_sites","petroleum_refineries"]
for gridFeature in gridRef["features"]:
    for tableName in tables:
        gridFeature["properties"][tableName] = []
######################################################################################
db = os.path.join(parent_dir, "data", "energy_sites.db")

# Connect to the SQLite database
conn = sqlite3.connect(db)
cursor = conn.cursor()
#f"Latitude: {latitude}, Longitude: {longitude}"

for tableName in tables:
    query = f"SELECT latitude, longitude FROM {tableName}"
    cursor.execute(query)

    for i, row in enumerate(cursor.fetchall()):
        latitude, longitude = row
        if None not in (latitude, longitude):
            queryPoint = Point(longitude, latitude)
            featureId_inside = None
            for gridFeature in gridRef["features"]:
                feature_shape = shape(gridFeature["geometry"])
                if feature_shape.contains(queryPoint):
                    featureId_inside = gridFeature["properties"]["id"]
                    gridFeature["properties"][tableName].append(i)
                    break
conn.close()

with open(os.path.join(parent_dir, "resources", "gridRef.geojson"), "w", encoding='utf-8') as geojson_gridRef:
	geojson_gridRef.write(json.dumps(gridRef, indent=1, default=json_serialize))
######################################################################################