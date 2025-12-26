import os
import sys
import json
import math
from typing import List, Tuple
import sqlite3
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import get_files, json_serialize
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationId = logJson["location_key"]
######################################################################################
dir_path = os.path.join("data", "osm")
files = get_files(dir_path)
######################################################################################
acceptedGeomTypes = {
    "building": ["Polygon", "MultiPolygon"],
    "highway": ["LineString", "MultiLineString", "Polygon", "MultiPolygon"],
    "construction": ["LineString", "MultiLineString", "Polygon", "MultiPolygon"]
}

fc = {
    "type": "FeatureCollection",
    "features": []
}
processedFiles = []
for fName in files:
    if fName.endswith(".geojson"):
        parse_fName = fName.split("_")
        if parse_fName[0] == locationId:
            osmCategory = parse_fName[1].split(".")[0]
            fPath = os.path.join(parent_dir, dir_path, fName)
            geoJson = json.load(open(fPath))
            if "features" in geoJson:
                features = geoJson["features"]
                for feature in features:
                    props = feature["properties"]
                    geom = feature["geometry"]
                    geom_type = geom["type"]
                    coords = geom["coordinates"]
                    if geom_type in acceptedGeomTypes[osmCategory]:
                        newFeature = {
                            "type": "Feature",
                            "properties": {
                                "osm_category": osmCategory,
                                **props
                            },
                            "geometry": {
                                "type": geom_type,
                                "coordinates": coords
                            }
                        }
                        fc["features"].append(newFeature)
            processedFiles.append(fPath)

######################################################################################
jsonDump_success = False
try:
    output_fName = locationId+"_osm.json"
    output_path = os.path.join(parent_dir, "data", "osm", output_fName)
    with open(output_path, "w", encoding='utf-8') as output_json:
        json.dump(fc, output_json, indent=1, default=json_serialize,ensure_ascii=False)
    jsonDump_success = True
except Exception as e:
    print(f"Error writing combined OSM GeoJSON: {e}")
######################################################################################
# If the json write was successful, remove the individual files
if jsonDump_success:
    for fPath in processedFiles:
        try:
            os.remove(fPath)
        except Exception as e:
            print(f"Error removing file {fPath}: {e}")
print("DONE")
######################################################################################
