import json
import os
import shutil
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))

publicOsm_dir = os.path.join(root_dir, "frontend", "public", "osm")

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]

filePath = os.path.join(parent_dir, 'data', 'osm', str(locationKey)+"_OSM_building.geojson")
######################################################################################
osmBuildingsJson = json.load(open(filePath,"r", encoding='utf-8'))

fc = {
    "type": "FeatureCollection",
    "features": []
}

for feature in osmBuildingsJson["features"]:
    if feature["geometry"]["type"] in ["Polygon", "MultiPolygon"]:
        fc["features"].append(feature)

with open(filePath, "w", encoding='utf-8') as output_json:
    output_json.write(json.dumps(fc, ensure_ascii=False))

siteFileMap[locationKey]["structures"] = "./osm/"+str(locationKey)+"_OSM_building.geojson"
with open(os.path.join(lib_dir, "siteFileMap.json"), "w", encoding='utf-8') as json_siteFileMap:
	json_siteFileMap.write(json.dumps(siteFileMap, indent=2, ensure_ascii=False))
     
shutil.copy(filePath, publicOsm_dir)

print("DONE")