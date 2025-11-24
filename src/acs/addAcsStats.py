from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
import geopandas as gpd
import json
import os
from collections import Counter
import numpy as np

######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
from utils import make_fc, make_new_geojson_feature
from utils import calc_yoy_change
from utils import flatten_array
from utils import make_fc
from utils import check_vals
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

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

selectedStateName = "Texas"
#gridRefGeoJson_path = os.path.join(parent_dir, "resources", "reference", "gridRef_"+selectedStateName+".geojson")
uscbPlaceGridRef_path = os.path.join(parent_dir, "resources", "reference", "gridRef_"+selectedStateCode+".geojson")
gridStats_path = os.path.join(parent_dir, "data", "composite", "gridStats.geojson")
acsSummary_path = os.path.join(parent_dir, "data", "acs", "acsCompositeStats_uscbPlace.geojson")
######################################################################################
with open(uscbPlaceGridRef_path, "r") as uscbPlaceGridRef_file:
    uscbPlaceGridRef = json.load(uscbPlaceGridRef_file)

with open(uscbPlaceGridRef_path, "r") as gridStats_file:
    gridStats = json.load(gridStats_file)


makeNewFc = make_fc(gridStats_path, None, True, "id")
fc = makeNewFc["fc"]
gridStatsCellIdRef = makeNewFc["ref"]
    
with open(acsSummary_path, "r") as acsSummary_file:
    acsSummary = json.load(acsSummary_file)

# Reference list of place Geoids - used for getting index of acsFeature
uscbPlaceRef = []
for feature in acsSummary["features"]:
    uscbPlaceRef.append(feature["properties"]["geoid"])

propKeys2Get = [
    "s0101_c01_001e", 
    "dp03_0022e",
    "dp03_0023e",
    "dp03_0024e",
    "dp03_0025e",
    "dp03_0032e",
    "dp03_0033e",
    "dp03_0034e",
    "dp03_0035e",
    "dp03_0036e",
    "dp03_0037e",
    "dp03_0038e",
    "dp03_0039e",
    "dp03_0040e",
    "dp03_0041e",
    "dp03_0042e",
    "dp03_0043e",
    "dp03_0044e",
    "dp03_0045e"
]

#
for i, val in enumerate(propKeys2Get):
    propKeys2Get[i] = val.lower()
    print(val.lower())

for feature in uscbPlaceGridRef["features"]:
    if len(feature["properties"]["uscb_geoid"]) != 0:
        for i, geoid in enumerate(feature["properties"]["uscb_geoid"]):
            if geoid in uscbPlaceRef:
                prctOverlap = feature["properties"]["prct_overlap"][i]*0.01
                idx_geoid = uscbPlaceRef.index(geoid)
                acsFeature = acsSummary["features"][idx_geoid]
                if feature["properties"]["id"] in gridStatsCellIdRef:
                    gridCell_idx = gridStatsCellIdRef.index(feature["properties"]["id"])
                    for propKey, propVals in acsFeature["properties"].items():
                        if propKey in propKeys2Get:
                            if propKey not in  fc["features"][gridCell_idx]["properties"]:
                                fc["features"][gridCell_idx]["properties"][propKey] = list(map(lambda x: round((x * prctOverlap),2), propVals))
                            else:
                                for j, val in enumerate(propVals):
                                    fc["features"][gridCell_idx]["properties"][propKey][j] += val*prctOverlap
                                    
for i, feature in enumerate(fc["features"]):
    prop_keys = list(feature["properties"].keys())
    for propKey in prop_keys:
        if propKey in propKeys2Get:
            propVals = feature["properties"][propKey]
            ##########################################################################  
            estimates = propVals
            meanPrctDelta, meanDelta, sepc, serc = 0, 0, 0, 0
            try:
                if len(estimates) > 1:
                    yoyDeltas = calc_yoy_change(estimates)
                    # Filter out None values before calculations
                    valid_deltas = [d for d in yoyDeltas["deltas"] if d is not None]
                    valid_prct_deltas = [d for d in yoyDeltas["prct_deltas"] if d is not None]
                    
                    if valid_deltas and valid_prct_deltas:  # Check if lists are not empty
                        seDeltas = calc_yoy_change([estimates[0], estimates[-1]])
                        sepc = seDeltas["prct_deltas"][0] if seDeltas["prct_deltas"][0] is not None else 0
                        serc = seDeltas["deltas"][0] if seDeltas["deltas"][0] is not None else 0
                        meanPrctDelta = np.mean(flatten_array(valid_prct_deltas))
                        meanDelta = np.mean(flatten_array(valid_deltas))
                    
                values = check_vals([meanPrctDelta, meanDelta, sepc, serc]) 

                fc["features"][i]["properties"][propKey+"_apc"] = round((values[0]),2)
                fc["features"][i]["properties"][propKey+"_arc"] = round((values[1]),2)
                fc["features"][i]["properties"][propKey+"_sepc"] = round((values[2]),2)
                fc["features"][i]["properties"][propKey+"_serc"] = values[3]
            except Exception as e:
                print(f"Error processing {propKey}: {e}")
            ##########################################################################
output_path = os.path.join(parent_dir, "data", "composite", "test.geojson")
with open(output_path, "w", encoding='utf-8') as json_output:
    json_output.write(json.dumps(fc, indent=1, default=json_serialize, ensure_ascii=False))

print("DONE")
