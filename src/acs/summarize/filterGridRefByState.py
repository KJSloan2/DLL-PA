from shapely.geometry import shape
#from shapely.geometry.base import BaseGeometry
#from shapely.ops import unary_union
#import geopandas as gpd
import json
import os
from collections import Counter
import numpy as np

######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils import  json_serialize
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

gridRefGeoJson_path = os.path.join(root_dir, "resources", "grid.geojson")

######################################################################################
with open(gridRefGeoJson_path, "r") as gridRefGeoJson_file:
    gridRefGeoJson = json.load(gridRefGeoJson_file)

stateRefGeoJson_path = os.path.join(root_dir, "resources", "reference", "tl_2024_us_state.geojson")
with open(stateRefGeoJson_path, "r") as stateRefGeoJson_file:
    stateRefGeoJson = json.load(stateRefGeoJson_file)


state_dict = {
    "01": "ALABAMA", "02": "ALASKA", "04": "ARIZONA", "05": "ARKANSAS",
    "06": "CALIFORNIA", "08": "COLORADO", "09": "CONNECTICUT", "10": "DELAWARE",
    "11": "DISTRICT OF COLUMBIA", "12": "FLORIDA", "13": "GEORGIA", "15": "HAWAII",
    "16": "IDAHO", "17": "ILLINOIS", "18": "INDIANA", "19": "IOWA",
    "20": "KANSAS", "21": "KENTUCKY", "22": "LOUISIANA", "23": "MAINE",
    "24": "MARYLAND", "25": "MASSACHUSETTS", "26": "MICHIGAN", "27": "MINNESOTA",
    "28": "MISSISSIPPI", "29": "MISSOURI", "30": "MONTANA", "31": "NEBRASKA",
    "32": "NEVADA", "33": "NEW HAMPSHIRE", "34": "NEW JERSEY", "35": "NEW MEXICO",
    "36": "NEW YORK", "37": "NORTH CAROLINA", "38": "NORTH DAKOTA", "39": "OHIO",
    "40": "OKLAHOMA", "41": "OREGON", "42": "PENNSYLVANIA", "44": "RHODE ISLAND",
    "45": "SOUTH CAROLINA", "46": "SOUTH DAKOTA", "47": "TENNESSEE", "48": "TEXAS",
    "49": "UTAH", "50": "VERMONT", "51": "VIRGINIA", "53": "WASHINGTON",
    "54": "WEST VIRGINIA", "55": "WISCONSIN", "56": "WYOMING"
}

selectedStateName = "Texas"

for key, val in state_dict.items():
    if val == selectedStateName.upper():
        selectedStateCode = key
        break

selectedStateFeature = None
for feature in stateRefGeoJson["features"]:
    if feature["properties"]["NAME"] == selectedStateName:
        selectedStateFeature = feature


placeGeoJson_path = os.path.join(root_dir, "resources", "reference", "tl_2024_"+selectedStateCode+"_place.geojson")
with open(placeGeoJson_path, "r") as placeGeoJson_file:
    placeGeoJson = json.load(placeGeoJson_file)


filteredGridFeatures = {
    "type": "FeatureCollection",
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}},
    "features": []
}

for gridFeature in gridRefGeoJson["features"]:
    gridGeom = shape(gridFeature["geometry"])
    gridId = gridFeature["properties"]["id"]
    stateGeom = shape(selectedStateFeature["geometry"])
    overlapArea = gridGeom.intersection(stateGeom).area
    if overlapArea > 0:
        filteredGridFeatures["features"].append(gridFeature)
        print(overlapArea)

with open(os.path.join(root_dir, "resources", "reference", "gridRef_"+selectedStateCode+".geojson"), "w", encoding='utf-8') as json_output:
    json_output.write(json.dumps(filteredGridFeatures, indent=1, default=json_serialize, ensure_ascii=False))

print("DONE")