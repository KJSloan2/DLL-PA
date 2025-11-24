from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
import geopandas as gpd
import json
import fiona
import os
from collections import Counter
import numpy as np
from rtree import index

######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
######################################################################################
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

resources_dir = os.path.join(parent_dir, "resources")
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
######################################################################################
filterGeomJson = json.load(open(os.path.join(parent_dir, 'resources', "grid.geojson")))

fc = {
    "type": "FeatureCollection",
    "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4269" } },
    "features": []
}

propsToReplace = ["hail_idx", "wind_idx", "torn_idx"]

for feature in filterGeomJson["features"]:
    newFeature = {
        "type": "Feature",
        "geometry": feature["geometry"],
        "properties": {}
    }

    for propKey, propVal in feature["properties"].items():
        if propKey in propsToReplace:
            newFeature["properties"][propKey] = []
        else:
            newFeature["properties"][propKey] = propVal
    
    for propKey in propsToReplace:
        if propKey not in newFeature["properties"]:
            # Initialize the property with an empty list if it doesn't exist
            newFeature["properties"][propKey] = []

    fc["features"].append(newFeature)
######################################################################################
files = {
    "hail": "1955-2024-hail-aspath.geojson",
    "wind": "1955-2024-wind-aspath.geojson",
    "torn": "1950-2024-torn-aspath.geojson"
    }

filter_index = index.Index()
for idx, filterFeature in enumerate(fc["features"]):
    filterShape = shape(filterFeature["geometry"])
    filter_index.insert(idx, filterShape.bounds)

for fileKey, fileName in files.items():
    with fiona.open(os.path.join(parent_dir, "data", "weather", fileName), mode="r") as src:
        for i, feature in enumerate(src):
            yr = int(feature["properties"]["yr"])
            if yr >= 2000:
                geom = shape(feature["geometry"])
                possible_matches = list(filter_index.intersection(geom.bounds))
                for idx in possible_matches:
                    filterShape = shape(fc["features"][idx]["geometry"])
                    if geom.intersects(filterShape):
                        fc["features"][idx]["properties"][f"{fileKey}_idx"].append(i)


output_geojson_path = os.path.join(parent_dir, "resources", "gridRef.geojson")
# Write the `fc` dictionary to a GeoJSON file
with open(output_geojson_path, "w") as geojson_file:
    try:
        # Use json.dump with the json_serialize function to handle serialization
        json.dump(fc, geojson_file, default=json_serialize)
        print(f"GeoJSON file successfully written to: {output_geojson_path}")
    except TypeError as e:
        print(f"Error serializing GeoJSON data: {e}")
