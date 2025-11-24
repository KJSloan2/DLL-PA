import os
import time
import json
from collections import Counter
import numpy as np
import time
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
######################################################################################
start_time = time.time()
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
gridRefGeoJson = json.load(open(os.path.join(parent_dir, 'resources', "gridRef.geojson")))
######################################################################################
weatherEventProps = {
    "yr": {"type": int, "calculations": ["mode"]},
    #"mag": {"type": int, "calculations": ["mean", "min", "max", "std"]},
    "mag": {"type": int, "calculations": ["mean"]},
    "inj": {"type": int, "calculations": ["sum", "mean"]},
    "fat": {"type": int, "calculations": ["sum", "mean"]},
    "loss": {"type": int, "calculations": ["sum", "mean"]},
    #"loss": {"type": int, "calculations": ["sum", "mean", "min", "max", "std"]},
    "closs": {"type": int, "calculations": ["sum", "mean"]},
    #"closs": {"type": int, "calculations": ["sum", "mean", "min", "max", "std"]},
    "len": {"type": float, "calculations": ["mean"]},
    #"len": {"type": float, "calculations": ["mean", "min", "max", "std"]},
    "wid": {"type": float, "calculations": ["mean"]},
    #"wid": {"type": float, "calculations": ["mean", "min", "max", "std"]},
    "fc": {"type": int, "calculations": ["mean"]},
    #"fc": {"type": int, "calculations": ["mean", "min", "max", "std"]},
}

eventTypes = {
    "wind": {
        "file": "1955-2024-wind-aspath.geojson",
        "props": ["yr", "mag", "inj", "fat", "loss", "closs", "len", "wid"],
        "prefix": "wa_"
    },
    "torn": {
        "file": "1950-2024-torn-aspath.geojson",
        "props": ["yr", "mag", "inj", "fat", "loss", "closs", "len", "wid", "fc"],
        "prefix": "ta_"
    },
    "hail": {
        "file": "1955-2024-hail-aspath.geojson",
        "props": ["yr", "mag", "inj", "fat", "loss", "closs"],
        "prefix": "ha_"
    }
}

props =  {}
for eventType, eventProps in eventTypes.items():
    prefix = eventProps["prefix"]
    for propKey in eventProps["props"]:
        for calcType in weatherEventProps[propKey]["calculations"]:
            props[prefix+propKey+"_"+calcType] = []

######################################################################################
fc = {
    "type": "FeatureCollection",
    "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4269" } },
    "features": []
}
refFeatureIds = []
for eventType, eventProps in eventTypes.items():
    prefix = eventProps["prefix"]
    print(prefix)
    weatherEventDataGeoJson = json.load(open(os.path.join(parent_dir, "data", "weather", eventProps["file"])))
    weatherEventFeatures = weatherEventDataGeoJson["features"]
    for feature in gridRefGeoJson["features"]:
        feature_id = feature["properties"]["id"]
        if feature_id not in refFeatureIds:
            refFeatureIds.append(feature_id)
            ##########################################################################
            weatherFeature = {
                "type": "Feature",
                "properties": {"id": feature_id},
                "geometry": {
                    "type": feature["geometry"]["type"],
                    "coordinates": feature["geometry"]["coordinates"]
                }
            }
            for key, val in props.items():
                weatherFeature["properties"][key] = []
            ##########################################################################
        else:
            weatherFeature = fc["features"][refFeatureIds.index(feature_id)]
        poolEventData = {}
        for prop in eventProps["props"]:
            poolEventData[prop] = []

        weatherEventType_idx = feature["properties"][eventType+"_idx"]
        for idx in weatherEventType_idx:
            event = weatherEventFeatures[idx]
            for prop in eventProps["props"]:
                propVal = event["properties"][prop]
                if eventType == "torn" and prop == "loss":
                    tornLossRef = [
                        {"range": [0, 1], "fscale": 0, "loss_dollars":0.00005},
                        {"range": [2, 3], "fscale": 1, "loss_dollars":0.005},
                        {"range": [4, 5], "fscale": 2, "loss_dollars":0.5},
                        {"range": [6, 7], "fscale": 3, "loss_dollars":50},
                        {"range": [8, 9], "fscale": 4, "loss_dollars":500},
                    ]
                    for lossRef in tornLossRef:
                        if int(propVal) in lossRef["range"]:
                            poolEventData[prop].append(lossRef["loss_dollars"])
                            break
                poolEventData[prop].append(event["properties"][prop])
            
        propDataAnnualized = {}

        if len(poolEventData["yr"]) > 1:
            # Sort year keys and calculate annualized properties
            yrKeysSorted = sorted(set(poolEventData["yr"]))
            weatherFeature["properties"][prefix+"yr"] = yrKeysSorted

            for yrKey in yrKeysSorted:
                idx_yrKey = [i for i, y in enumerate(poolEventData["yr"]) if y == yrKey]
                props_without_yr = [prop for prop in eventProps["props"] if prop != "yr"]
                for propKey in props_without_yr:
                    propData = list(map(lambda idx: poolEventData[propKey][idx], idx_yrKey))

                    if propKey not in propDataAnnualized:
                        propDataAnnualized[propKey] = propData
                    else:
                        propDataAnnualized[propKey] = propDataAnnualized[propKey] + propData

                    for calcType in weatherEventProps[propKey]["calculations"]:
                        if calcType == None:
                            for val in propData:
                                weatherFeature["properties"][prefix+propKey].append(val)
                        elif calcType == "mean":
                            mean = np.mean(propData)
                            weatherFeature["properties"][prefix+propKey+"_"+"mean"].append(round((mean),2))
                        elif calcType == "mode":
                            counter = Counter(propData)
                            mode_value, mode_count = counter.most_common(1)[0]
                            weatherFeature["properties"][prefix+propKey+"_"+"mode"].append(mode_count)
                        elif calcType == "std":
                            std = np.std(propData)
                            weatherFeature["properties"][prefix+propKey+"_"+"std"].append(round((std),2))
                        elif calcType == "min":
                            weatherFeature["properties"][prefix+propKey+"_"+"min"].append(min(propData))
                        elif calcType == "max":
                            weatherFeature["properties"][prefix+propKey+"_"+"max"].append(max(propData))
                        elif calcType == "sum":
                            weatherFeature["properties"][prefix+propKey+"_"+"sum"].append(sum(propData))

            for propKey, propData in propDataAnnualized.items():
                for calcType in weatherEventProps[propKey]["calculations"]:
                    if calcType == None:
                        for val in propData:
                            weatherFeature["properties"][prefix+propKey] = val
                    elif calcType == "mean":
                        mean = np.mean(propData)
                        weatherFeature["properties"][prefix+propKey+"_mean_t"] = round((mean),2)
                    elif calcType == "mode":
                        counter = Counter(propData)
                        mode_value, mode_count = counter.most_common(1)[0]
                        weatherFeature["properties"][prefix+propKey+"_mode_t"] = mode_count
                    elif calcType == "std":
                        std = np.std(propData)
                        weatherFeature["properties"][prefix+propKey+"_std_t"] = round((std),2)
                    elif calcType == "min":
                        weatherFeature["properties"][prefix+propKey+"_min_t"] = min(propData)
                    elif calcType == "max":
                        weatherFeature["properties"][prefix+propKey+"_max_t"] = max(propData)
                    elif calcType == "sum":
                        weatherFeature["properties"][prefix+propKey+"_sum_t"]  = sum(propData)
        fc["features"].append(weatherFeature)

with open(os.path.join(parent_dir, "data", "weather", "extremeWeather_gridStats.geojson"), "w", encoding='utf-8') as json_log:
    json_log.write(json.dumps(fc, indent=1, default=json_serialize, ensure_ascii=False))

print("DONE")
