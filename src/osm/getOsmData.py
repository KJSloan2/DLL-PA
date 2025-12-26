import os
import sys
import json
import subprocess
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize, to_py_type
from osmUtils import get_osm_overpy_bbox, osm_req_bbox
from spatial_utils import complete_bbox
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

'''lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))'''

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationId = logJson["location_key"]

dataDirectory = os.path.join(parent_dir, 'data', 'osm')
outputDirectory = os.path.join(parent_dir, 'output', 'osm')
######################################################################################
# get  years and convert to int for sorting
analysisYears = sorted(list(map(int, logJson["ls8_bounds"].keys())))
# get bb at earliet year, convert back to str for dict access
bb_ls = logJson["ls8_bounds"][str(analysisYears[0])]["bb"]
bbox = complete_bbox((bb_ls[0][0], bb_ls[0][1]), (bb_ls[1][0], bb_ls[1][1]))

west_lon = min(bbox["pt_sw"][0], bbox["pt_ne"][0])
east_lon = max(bbox["pt_sw"][0], bbox["pt_ne"][0])
south_lat = min(bbox["pt_sw"][1], bbox["pt_ne"][1])
north_lat = max(bbox["pt_sw"][1], bbox["pt_ne"][1])
bbStr = f"{south_lat},{west_lon},{north_lat},{east_lon}"

print(bbStr)
bb_geoJsom = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [bbox["pt_sw"][0], bbox["pt_sw"][1]],
                    [bbox["pt_nw"][0], bbox["pt_nw"][1]],
                    [bbox["pt_ne"][0], bbox["pt_ne"][1]],
                    [bbox["pt_se"][0], bbox["pt_se"][1]],
                    [bbox["pt_sw"][0], bbox["pt_sw"][1]],
                    ]]}
        },
    ]
}

outputPath = os.path.join(parent_dir, 'data', 'osm', "TEST_bbox.geojson")
with open(outputPath, "w", encoding='utf-8') as output_json:
    output_json.write(json.dumps(bb_geoJsom, ensure_ascii=False))

######################################################################################
osm_req_bbox(locationId, bbStr, ["building", "highway", "construction"], dataDirectory)
######################################################################################