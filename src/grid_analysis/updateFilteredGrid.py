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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils import  json_serialize
from utils import make_fc, make_new_geojson_feature
from utils import calc_yoy_change
from utils import flatten_array
from utils import check_vals
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

public_dir = os.path.join(root_dir, "frontend", "public")

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
#gridRefGeoJson_path = os.path.join(parent_dir, "resources", "reference", "gridRef_"+selectedStateName+".geojson")
gridRefGeoJson_path = os.path.join(parent_dir, "resources", "reference", "gridRef_"+selectedStateCode+".geojson")
placeGeoJson_path = os.path.join(parent_dir, "resources", "reference", "tl_2024_"+selectedStateCode+"_place.geojson")
#acsSummary_path: acs data aggragated and summarized by USCB place
acsSummary_path = os.path.join(parent_dir, "data", "acs", "acsCompositeStats_uscbPlace.geojson")
#
ewGridStatsGeoJson_path = os.path.join(parent_dir, "data", "composite", "gridStats.geojson")
hcGridRefGeoJson_path = os.path.join(parent_dir, "resources", "healthcareFacilities_gridRef.geojson")
######################################################################################
# gridRefGeoJson: grid areas by state - contains extreme weather reference indexes
with open(ewGridStatsGeoJson_path, "r") as gridRefGeoJson_file:
    gridRefGeoJson = json.load(gridRefGeoJson_file)

with open(placeGeoJson_path, "r") as placeGeoJson_file:
    placeGeoJson = json.load(placeGeoJson_file)

with open(hcGridRefGeoJson_path, "r") as hcGridRefGeoJson_file:
    hcGridRefGeoJson = json.load(hcGridRefGeoJson_file)

######################################################################################
refIds_gridStats = []
with open(ewGridStatsGeoJson_path, "r") as gridStatsGeoJson_file:
    gridStatsGeoJson = json.load(gridStatsGeoJson_file)
for feature in gridStatsGeoJson["features"]:
    refIds_gridStats.append(feature["properties"]["id"])


with open(acsSummary_path, "r") as acsSummary_file:
    acsSummary = json.load(acsSummary_file)

uscbPlaceRef = []
for feature in acsSummary["features"]:
    uscbPlaceRef.append(feature["properties"]["geoid"])

extremeWeatherPropsToGet = [
    #Wind
    "wi_mag_mean_t", "wi_inj_mean_t", "wi_fat_mean_t", 
    "wi_loss_mean_t", "wi_closs_mean_t", "wi_len_mean_t", "wi_wid_mean_t",
    "ta_mag_mean",
    "wi_inj_sum", "wi_fat_sum", "wi_loss_sum", "wi_closs_sum",
    #Tornados
    "ta_mag_mean_t", "ta_inj_mean_t", "ta_fat_mean_t", 
    "ta_loss_mean_t", "ta_closs_mean_t", "ta_len_mean_t", "ta_wid_mean_t",
    "ta_inj_sum", "ta_fat_sum", "ta_loss_sum", "ta_closs_sum",
    #Hail
    "ha_mag_mean_t", "ha_inj_mean_t", "ha_fat_mean_t", 
    "ha_loss_mean_t", "ha_closs_mean_t",
    "ha_inj_sum", "ha_fat_sum", "ha_loss_sum", "ha_closs_sum"
]

acsPropsToGet = [
    "s0101_c01_001e",
    "dp03_0001e",
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

hcPropsToGet = [
        "helipads", "trauma", "hospital_objectids",
        "hosp_genacute", "hosp_crit", "hosp_mil",
        "hosp_rehab", "hosp_lt", "hosp_psyc",
        "hosp_child", "hosp_spec", "hosp_women",
        "hosp_chronic"
]

fc = {
    "type": "FeatureCollection",
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}},
    "features": []
}
######################################################################################
count = 0
overlaps_found = 0
for gridFeature in gridRefGeoJson["features"]:
    try:
        gridGeom = shape(gridFeature["geometry"])
        grid_id = gridFeature["properties"]["id"]
        updatedGridFeature = make_new_geojson_feature(gridFeature, newProperties={"uscb_geoid": [], "prct_overlap": []})
       
        #refIds_gridStats: list of grid IDs from gridStatsGeoJson
        ewGridStatsFeature_idx = refIds_gridStats.index(grid_id) if grid_id in refIds_gridStats else None
        if ewGridStatsFeature_idx is not None:
            for propKey in extremeWeatherPropsToGet:
                #print(gridStatsGeoJson["features"][gridStatsFeature_idx]["properties"].keys())
                if propKey in list(gridStatsGeoJson["features"][ewGridStatsFeature_idx]["properties"].keys()):
                    updatedGridFeature["properties"][propKey] = gridStatsGeoJson["features"][ewGridStatsFeature_idx]["properties"][propKey]
        
        hcGridStatsFeature_idx = refIds_gridStats.index(grid_id) if grid_id in refIds_gridStats else None
        for propKey in hcPropsToGet:
            if propKey in list(hcGridRefGeoJson["features"][ewGridStatsFeature_idx]["properties"].keys()):
                updatedGridFeature["properties"][propKey] = hcGridRefGeoJson["features"][ewGridStatsFeature_idx]["properties"][propKey]
            ##########################################################################                  
        for placeFeature in placeGeoJson["features"]:
            try:
                # Use shape() for place geometry too
                placeGeom = shape(placeFeature["geometry"])
                place_name = placeFeature["properties"]["NAME"]
                uscb_geoid = placeFeature["properties"]["GEOID"]
                
                count += 1

                if gridGeom.intersects(placeGeom):
                    overlapArea = gridGeom.intersection(placeGeom).area
                    if overlapArea > 0:
                        # Calculate percentage of grid covered
                        prct_overlap = (overlapArea / gridGeom.area) * 100
                        if prct_overlap > 1:
                            updatedGridFeature["properties"]["uscb_geoid"].append(uscb_geoid)
                            updatedGridFeature["properties"]["prct_overlap"].append(prct_overlap)

                            if uscb_geoid in uscbPlaceRef:
                                idx_geoid = uscbPlaceRef.index(uscb_geoid)
                                acsFeature = acsSummary["features"][idx_geoid]
                                
                                for acsPropKey in acsPropsToGet:
                                    if acsPropKey in list(acsFeature["properties"].keys()):
                                        if acsPropKey not in list(updatedGridFeature["properties"].keys()):
                                            updatedGridFeature["properties"][acsPropKey] = list(map(lambda x: round((x * prct_overlap),2), acsFeature["properties"][acsPropKey]))
                                        else:
                                            for j, val in enumerate(acsFeature["properties"][acsPropKey]):
                                                updatedGridFeature["properties"][acsPropKey][j] += val*prct_overlap


                            print(f"Overlap #{overlaps_found}: Grid {grid_id} with {uscb_geoid}: {prct_overlap:.2f}%")
                        overlaps_found += 1
                
            except Exception as e:
                print(f"Error processing place feature: {e}")
                continue
        
        fc["features"].append(updatedGridFeature)

    except Exception as e:
        print(f"Error processing grid feature: {e}")
        continue

for i, feature in enumerate(fc["features"]):
    try:
        prop_keys = list(feature["properties"].keys())
        for propKey in prop_keys:
            if propKey in acsPropsToGet:
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
    except Exception as e:
        print(f"Error processing feature properties: {e}")
        continue

output_path = os.path.join(public_dir, "gridRef_"+selectedStateCode+".geojson")
with open(output_path, "w", encoding='utf-8') as json_output:
    json_output.write(json.dumps(fc, indent=1, default=json_serialize, ensure_ascii=False))

print(f"Completed. Processed {count} comparisons, found {overlaps_found} overlaps.")