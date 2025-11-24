import os
import time
import json
from collections import Counter

import numpy as np

import fiona
from shapely.geometry import MultiLineString, Point, mapping

######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
from spatial_utils import create_circle
######################################################################################
start_time = time.time()
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]

lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))
siteCoordinates = siteFileMap[locationKey]["coordinates"]
######################################################################################
weatherFilterRegionId = logJson["weather_filter_region"]

weatherFilterRegionsJson = os.path.join(parent_dir, "resources", "gridRef.geojson")
with open(weatherFilterRegionsJson, "r") as file:
    weather_filter_regions = json.load(file)
 
weatherEventProps = {
    "om": {"type": int, "calculations": ["mean", "min", "max", "std"]},
    "mo": {"type": int, "calculations": ["mode"]},
    "dy": {"type": int, "calculations": ["mode"]},
    "tz": {"type": int, "calculations": ["mean", "min", "max", "std"]},
    "stn": {"type": int, "calculations": ["mean", "min", "max", "std"]},
    "mag": {"type": int, "calculations": ["mean", "min", "max", "std"]},
    "inj": {"type": int, "calculations": ["sum", "mean"]},
    "fat": {"type": int, "calculations": ["sum", "mean"]},
    "loss": {"type": int, "calculations": ["sum", "mean", "min", "max", "std"]},
    "closs": {"type": int, "calculations": ["sum", "mean", "min", "max", "std"]},
    "len": {"type": float, "calculations": ["mean", "min", "max", "std"]},
    "wid": {"type": float, "calculations": ["mean", "min", "max", "std"]},
    "fc": {"type": int, "calculations": ["mean", "min", "max", "std"]},
}
#"mt": {"type": str, "calculations": [None]},
#Temp disable mt for wind and torn
eventTypes = {
    "wind": {
        "file": "1955-2024-wind-aspath.geojson",
        "props": ["om", "mo", "dy", "tz", "stn", "mag", "inj", "fat", "loss", "closs", "len", "wid"],
        "prefix": "wi_"
    },
    "torn": {
        "file": "1950-2024-torn-aspath.geojson",
        "props": ["om", "mo", "dy", "tz", "stn", "mag", "inj", "fat", "loss", "closs", "len", "wid", "fc"],
        "prefix": "ta_"
    },
    "hail": {
        "file": "1955-2024-hail-aspath.geojson",
        "props": ["om", "mo", "dy", "tz", "stn", "mag", "inj", "fat", "loss", "closs"],
        "prefix": "ha_"
    }
}

for eventType, eventProps in eventTypes.items():
    filterIndexes = None
    for feature in weather_filter_regions["features"]:
        if feature["properties"]["id"] == weatherFilterRegionId:
            filterIndexes = feature["properties"][eventType+"_idx"]
            break

    pathsGeoJsonName = eventProps["file"]
    propKeyPrefix = eventProps["prefix"]
    geojson_path = os.path.join(parent_dir, "data", "weather", pathsGeoJsonName)

    featuresFiltered = []
    if filterIndexes is not None:
        with fiona.open(geojson_path, 'r') as src:
            featuresFiltered = [src[i] for i in filterIndexes]
    ######################################################################################
    fc = {
        "type": "FeatureCollection",
        "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4269" } },
        "features": []
    }

    intersects = 0
    if len(featuresFiltered) != 0:
        for i in range(1, 20, 5):
            radius = int(i)
            circle_geom, circle_feature = create_circle(siteCoordinates[1], siteCoordinates[0], radius, resolution=64)
            
            propMetrics = {}
            for key in eventProps["props"]:
                propMetrics[key] = weatherEventProps[key]

            circle_feature["properties"] = {
                propKeyPrefix+"radius_miles": radius,
                propKeyPrefix+"year":[], propKeyPrefix+"count":[],
            }
            
            for propKey, item in propMetrics.items(): 
                for calcType in item["calculations"]:
                    if calcType is not None:
                        circle_feature["properties"][propKeyPrefix+propKey+"_"+calcType] = []
                    else:
                        circle_feature["properties"][propKeyPrefix+propKey] = []

            events_by_year = {}

            for feature in featuresFiltered:
                properties = feature["properties"]
                geometry = feature["geometry"]
                yr = properties["yr"]

                # Normalize to MultiLineString for consistent handling
                if geometry["type"] == "MultiLineString":
                    multi_line = MultiLineString(geometry["coordinates"])
                elif geometry["type"] == "LineString":
                    multi_line = MultiLineString([geometry["coordinates"]])
                else:
                    continue  # Skip other geometry types

                if multi_line.intersects(circle_geom):
                    if yr not in events_by_year:
                        events_by_year[yr] = {"count": 1}
                        for key, propData in propMetrics.items():
                            events_by_year[yr][key] = [properties[key]]
                    else:
                        events_by_year[yr]["count"] += 1
                        for key, propData in propMetrics.items():
                            events_by_year[yr][key].append(properties[key])
                                        
            yrKeysSorted = sorted(events_by_year.keys())                     
            for yrKey in yrKeysSorted:
                circle_feature["properties"][propKeyPrefix+"year"].append(yrKey)
                circle_feature["properties"][propKeyPrefix+"count"].append(events_by_year[yrKey]["count"])
                for propKey, propData in propMetrics.items():
                    for calcType in propData["calculations"]:
                        data = events_by_year[yrKey][propKey]
                        if calcType == None:
                            for val in data:
                                circle_feature["properties"][propKeyPrefix+propKey].append(val)
                        elif calcType == "mean":
                            mean = np.mean(data)
                            circle_feature["properties"][propKeyPrefix+propKey+"_"+"mean"].append(round((mean),2))
                        elif calcType == "mode":
                            counter = Counter(data)
                            mode_value, mode_count = counter.most_common(1)[0]
                            circle_feature["properties"][propKeyPrefix+propKey+"_"+"mode"].append(mode_count)
                        elif calcType == "std":
                            std = np.std(data)
                            circle_feature["properties"][propKeyPrefix+propKey+"_"+"std"].append(round((std),2))
                        elif calcType == "min":
                            circle_feature["properties"][propKeyPrefix+propKey+"_"+"min"].append(min(data))
                        elif calcType == "max":
                            circle_feature["properties"][propKeyPrefix+propKey+"_"+"max"].append(max(data))
                        elif calcType == "sum":
                            circle_feature["properties"][propKeyPrefix+propKey+"_"+"sum"].append(sum(data))
                        elif calcType == "none":
                            circle_feature["properties"][propKeyPrefix+propKey].append(data)
            
            del circle_feature["geometry"]
            fc["features"].append(circle_feature)

    outputFileName = locationKey+"_site"+eventType+".json"
    siteFileMap[locationKey][eventType] = "./weather/"+outputFileName
    with open(os.path.join(lib_dir, "siteFileMap.json"), "w", encoding='utf-8') as json_siteFileMap:
        json_siteFileMap.write(json.dumps(siteFileMap, indent=2, ensure_ascii=False))
    ######################################################################################
    output_path = os.path.join("frontend", "public", 'weather', outputFileName)
    with open(output_path, "w", encoding='utf-8') as output_geojson:
        output_geojson.write(json.dumps(fc, indent=1, default=json_serialize, ensure_ascii=False))
    ######################################################################################
    end_time = time.time()
    duration = end_time - start_time
    logJson["run_stats"]["preprocessing"][eventType+"_site_analysis"] = {
                "start_time":start_time,
                "end_time":end_time,
                "duration":duration
                }

with open(os.path.join(parent_dir, 'resources', "log.json"), "w", encoding='utf-8') as json_log:
    json_log.write(json.dumps(logJson, indent=2, ensure_ascii=False))

print("DONE")
