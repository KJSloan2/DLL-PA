import os
from os import listdir
from os.path import isfile, join

import json
import pandas as pd
import numpy as np
from geopy.distance import geodesic
from shapely.geometry import Point, Polygon
from scipy.spatial import cKDTree

from datetime import datetime
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
from utils import make_new_geojson_feature
######################################################################################
def list_files(directory):
	return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]

publicTerrain_dir = os.path.join(root_dir, "frontend", "public", "terrain")
lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))

public_dir = os.path.join(root_dir, "frontend", "public", "acs")
######################################################################################
######################################################################################
threeDepData_fName = locationKey+'_3DEP_terrain.csv'
threeDepData_path = os.path.join(
	parent_dir, 'output', '3dep', threeDepData_fName)
threeDepData = pd.read_csv(threeDepData_path)

######################################################################################
ls8Files = list_files(os.path.join(parent_dir, 'output', 'landsat', locationKey, 'tiles'))
storeLs8FileYears = []
for f in ls8Files:
	spit_f = f.split('_')
	spit_f = spit_f[-1].split('.')
	ext = spit_f[-1]
	if ext == "csv":
		fYear = spit_f[0]
		print("fYear: ", fYear)
		storeLs8FileYears.append(int(fYear))
# Use the most recent year available in the Landsat data
year = max(storeLs8FileYears)

step_width = logJson["ls8_bounds"][str(year)]["step_width"]
step_height = logJson["ls8_bounds"][str(year)]["step_height"]

landsatData_fName = 'LS8_'+locationKey+'_'+str(year)+'.csv'
landsatData_path = os.path.join(
	parent_dir, 'output', 'landsat', locationKey, 'tiles', landsatData_fName)
landsatData = pd.read_csv(landsatData_path)
######################################################################################
fc = {
	"type": "FeatureCollection",
	"features": []
}
landsatPolygons = []
landsatPoints = []
######################################################################################
compositeTerrain = {
	"lstf":[], "ndvi":[],
	"lat":[], "lon":[],
	"elv_rel":[], "elv_real":[],
}

for idx, row in landsatData.iterrows():
	lstf = row['lstf']
	ndvi = row['ndvi']
	#-98.93307920759433,30.40357062255362
	pt = Point(row['longitude'], row['latitude'])
	landsatPoints.append(pt)

landsatCkdTreePoints = np.array([[p.x, p.y] for p in landsatPoints])
landsatCkdTree = cKDTree(landsatCkdTreePoints)
######################################################################################

#"lon":coord_x, "lat":coord_y, "elv_rel":elv_rel, "elv_real": elv
for idx, threeDepPt in threeDepData.iterrows():
    query_coords = np.array([threeDepPt['lon'], threeDepPt['lat']])
    distance, index = landsatCkdTree.query(query_coords)
    ls8_cp = landsatData.iloc[index]

    compositeTerrain["elv_rel"].append(float(threeDepPt["elv_rel"]))
    compositeTerrain["elv_real"].append(float(threeDepPt["elv_real"]))
    compositeTerrain["lat"].append(float(threeDepPt["lat"]))
    compositeTerrain["lon"].append(float(threeDepPt["lon"]))
    compositeTerrain["lstf"].append(float(ls8_cp["lstf"]))
    compositeTerrain["ndvi"].append(float(ls8_cp["ndvi"]))

######################################################################################
outputFileName = locationKey+'_3depLs8Comp.csv'
outputPath = os.path.join(publicTerrain_dir, outputFileName)

df = pd.DataFrame({
	"lstf": compositeTerrain["lstf"],
	"ndvi": compositeTerrain["ndvi"],
	"lat": compositeTerrain["lat"],
	"lon": compositeTerrain["lon"],
	"elv_rel": compositeTerrain["elv_rel"],
	"elv_real": compositeTerrain["elv_real"]
})

df.to_csv(outputPath, index=False)
######################################################################################
siteFileMap[locationKey]["terrain"] = "./terrain/"+outputFileName 
with open(os.path.join(lib_dir, "siteFileMap.json"), "w", encoding='utf-8') as json_siteFileMap:
	json_siteFileMap.write(json.dumps(siteFileMap, indent=2, ensure_ascii=False))
print("terrainComposite.py DONE")
######################################################################################