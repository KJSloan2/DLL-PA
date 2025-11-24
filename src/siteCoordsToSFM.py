import os
import json

######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]

lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))

public_dir = os.path.join(root_dir, "frontend", "public", "acs")
######################################################################################
poolCoords = [[],[]]
for year, bounds in logJson["ls8_bounds"].items():
    poolCoords[0].append(bounds["centroid"][1])
    poolCoords[1].append(bounds["centroid"][0])
    
# Calculate the centroid of the polygon
centroid_x = sum(poolCoords[0]) / len(poolCoords[0])
centroid_y = sum(poolCoords[1]) / len(poolCoords[1])
# Add the site coordinates to the siteFileMap

logJson["site_centroid"] = [centroid_x, centroid_y]
with open(os.path.join(parent_dir, 'resources', "log.json"), "w", encoding='utf-8') as json_log:
	json_log.write(json.dumps(logJson, indent=2, ensure_ascii=False))
######################################################################################
siteFileMap[locationKey]["coordinates"] = [centroid_x, centroid_y]
######################################################################################
with open(os.path.join(lib_dir, "siteFileMap.json"), "w", encoding='utf-8') as json_siteFileMap:
	json_siteFileMap.write(json.dumps(siteFileMap, indent=2, ensure_ascii=False))
######################################################################################
print("sitrCoordsToSFM.py: DONE")