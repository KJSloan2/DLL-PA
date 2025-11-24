import os
import numpy as np
import json
import csv
import rasterio as rio

######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import get_directories, get_files
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, "resources","log.json")))
######################################################################################
data_dir = r"C:\Users\Kjslo\Documents\data\GEE_Landsat"

subDirs = get_directories(data_dir)

fc = {
    "type": "FeatureCollection",
    "features": []
}

for subDir in subDirs:
    files = get_files(os.path.join(data_dir, subDir))
    for f in files:
        if f.endswith(".tif"):
            file_path = os.path.join(data_dir, subDir, f)
            f_split = f.split("_")
            locationKey = f_split[1]

            with rio.open(file_path) as src:

                src_width = src.width
                src_height = src.height
                src_bounds = src.bounds

                bb_pt1 = [src_bounds[0],src_bounds[1]]
                bb_pt2 = [src_bounds[2],src_bounds[3]]
                bb_width = bb_pt2[0] - bb_pt1[0]
                bb_height = bb_pt2[1] - bb_pt1[1]

                bb_pt3 = [bb_pt1[0],bb_pt2[1]]
                bb_pt4 = [bb_pt2[0],bb_pt1[1]]

                obj = {
                    "type": "Feature",
                    "properties": {
                        "site_name": locationKey,
                        "filename": f,
                        "file_dir": subDir,
                        "src_width": src_width,
                        "src_height": src_height,
                        "src_bounds": src_bounds,
                        "bb_width": bb_width,
                        "bb_height": bb_height,
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            bb_pt1,
                            [bb_pt2[0], bb_pt1[1]],
                            bb_pt2,
                            [bb_pt1[0], bb_pt2[1]],
                            bb_pt1
                        ]]
                    }
                }
                fc["features"].append(obj)
#C:\Users\Kjslo\Documents\local_dev\dynamic_lands_lab\backend\data\tile_reference
#backend\data\tile_reference
with open(r"backend\data\tile_reference", encoding='utf-8') as output_json:
	json.dump(fc, output_json, indent=1, ensure_ascii=False)