import os
import sys
import json
import subprocess
import sqlite3
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.utils import  json_serialize, to_py_type
from osmUtils import get_osm_overpy_bbox, osm_req_bbox
from spatial_utils import complete_bbox
from global_functions.sqlite_utils import get_table_info
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

dataDirectory = os.path.join(parent_dir, 'data', 'osm')
outputDirectory = os.path.join(parent_dir, 'output', 'osm')
######################################################################################
DB_NAME = "runtime.db"
DB_TABLE = "site_info"

def str_to_coords(coord_str):
    coord_str = coord_str.strip("() ")
    coords = tuple(float(x.strip()) for x in coord_str.split(","))
    return coords

siteInfo = get_table_info(DB_NAME, DB_TABLE, ["NAME", "AOI_BB_PT_SW", "AOI_BB_PT_NE", "AOI_CENTROID", "STATE_FIPS", "COUNTY_FIPS"])
siteName = siteInfo['NAME']
aoi_bb_pt_sw = str_to_coords(siteInfo["AOI_BB_PT_SW"])
aoi_bb_pt_ne = str_to_coords(siteInfo["AOI_BB_PT_NE"])
aoi_centroid = str_to_coords(siteInfo["AOI_CENTROID"])
stateFips = siteInfo["STATE_FIPS"]
countyFips = siteInfo["COUNTY_FIPS"]

bbox = complete_bbox((aoi_bb_pt_sw[0], aoi_bb_pt_sw[1]), (aoi_bb_pt_ne[0], aoi_bb_pt_ne[1]))

west_lon = min(bbox["pt_sw"][0], bbox["pt_ne"][0])
east_lon = max(bbox["pt_sw"][0], bbox["pt_ne"][0])
south_lat = min(bbox["pt_sw"][1], bbox["pt_ne"][1])
north_lat = max(bbox["pt_sw"][1], bbox["pt_ne"][1])
bbStr = f"{south_lat},{west_lon},{north_lat},{east_lon}"

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
osm_req_bbox(siteName, bbStr, ["building", "highway", "construction"], dataDirectory)
######################################################################################