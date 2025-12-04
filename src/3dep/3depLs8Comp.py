import os
from os import listdir
from os.path import isfile, join

import json

import os
import json
import sqlite3

import pandas as pd
import numpy as np
from geopy.distance import geodesic
from shapely.geometry import Point, Polygon
from scipy.spatial import cKDTree
import csv
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

publicTerrain_dir = os.path.join(root_dir, "output", "terrain")
#lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
#siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))

#public_dir = os.path.join(root_dir, "frontend", "public", "acs")
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
	if ext == "geojson":
		fYear = spit_f[0]
		print("fYear: ", fYear)
		storeLs8FileYears.append(int(fYear))
# Use the most recent year available in the Landsat data
year = max(storeLs8FileYears)

step_width = logJson["ls8_bounds"][str(year)]["step_width"]
step_height = logJson["ls8_bounds"][str(year)]["step_height"]

landsatData_fName = "ls8_"+locationKey+"_temporal.csv"
landsatData_path = os.path.join(
	parent_dir, 'output', 'landsat', locationKey, 'arrays', landsatData_fName)
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
	"lat":[], "lon":[],
	"lstf":[], 
	"lstf_serc":[],
	"lstf_arc":[],
	"ndvi":[],
	"ndvi_serc":[],
	"ndvi_arc":[],
	"ndmi":[],
	"ndmi_serc":[],
	"ndmi_arc":[],
	"elv_rel":[], "elv_real":[],
	"idx_row":[], "idx_col":[]
}

poolData = {
	"lstf_arc_neg": [], "lstf_arc_pos": [],
	"lstf_serc_neg": [],"lstf_serc_pos": [],
	"ndvi_arc_neg": [], "ndvi_arc_pos": [],
	"ndvi_serc_neg": [], "ndvi_serc_pos": [],
	"ndmi_arc_neg": [], "ndmi_arc_pos": [],
	"ndmi_serc_neg": [], "ndmi_serc_pos": [],
}

def append_pool(metricKey, value, pool=poolData):
	if value < 0:
		pool[metricKey+"_arc_neg"].append(abs(value))
	elif value > 0:
		pool[metricKey+"_arc_pos"].append(value)


if threeDepData.empty:
	print("No 3DEP data found for the location, using Landsat data only.")
	for idx, row in landsatData.iterrows():
		compositeTerrain["lat"].append(float(row["lat"]))
		compositeTerrain["lon"].append(float(row["lon"]))
		compositeTerrain["lstf"].append(float(row["lstf"]))
		compositeTerrain["lstf_serc"].append(float(row["lstf_total_change"]))
		compositeTerrain["lstf_arc"].append(float(row["lstf_mean_yoy_change"]))
		compositeTerrain["ndvi"].append(float(row["ndvi"]))
		compositeTerrain["ndvi_serc"].append(float(row["ndvi_total_change"]))
		compositeTerrain["ndvi_arc"].append(float(row["ndvi_mean_yoy_change"]))
		compositeTerrain["ndmi"].append(float(row["ndmi"]))
		compositeTerrain["ndmi_serc"].append(float(row["ndmi_total_change"]))
		compositeTerrain["ndmi_arc"].append(float(row["ndmi_mean_yoy_change"]))
		compositeTerrain["elv_rel"].append(1)
		compositeTerrain["elv_real"].append(1)
		compositeTerrain["idx_row"].append(row["idx_row"])
		compositeTerrain["idx_col"].append(row["idx_col"])
else:
	for idx, row in landsatData.iterrows():
		#lon,lat,lstf,lstf_total_change,lstf_mean_yoy_change,ndvi,ndvi_total_change,ndvi_mean_yoy_change
		lstf = row["lstf"]
		lstf_serc = row["lstf_total_change"]
		lstf_arc = row["lstf_mean_yoy_change"]

		ndvi = row["ndvi"]
		ndvi_serc = row["ndvi_total_change"]
		ndvi_arc = row["ndvi_mean_yoy_change"]

		ndmi = row["ndmi"]
		ndmi_serc = row["ndmi_total_change"]
		ndmi_arc = row["ndmi_mean_yoy_change"]

		append_pool("lstf", lstf_arc)
		append_pool("ndvi", ndvi_arc)
		append_pool("ndmi", ndmi_arc)

		#-98.93307920759433,30.40357062255362
		pt = Point(row['lon'], row['lat'])
		landsatPoints.append(pt)

	landsatCkdTreePoints = np.array([[p.x, p.y] for p in landsatPoints])
	landsatCkdTree = cKDTree(landsatCkdTreePoints)
	######################################################################################
	for idx, threeDepPt in threeDepData.iterrows():
		query_coords = np.array([threeDepPt['lon'], threeDepPt['lat']])
		distance, index = landsatCkdTree.query(query_coords)
		ls8_cp = landsatData.iloc[index]
		# lstf
		lstfSerc = ls8_cp["lstf_total_change"]
		lstfArc = ls8_cp["lstf_mean_yoy_change"]
		# ndvi
		ndviSerc = ls8_cp["ndvi_total_change"]
		ndviArc = ls8_cp["ndvi_mean_yoy_change"]
		# ndmi
		ndmiSerc = ls8_cp["ndmi_total_change"]
		ndmiArc = ls8_cp["ndmi_mean_yoy_change"]

		compositeTerrain["elv_rel"].append(float(threeDepPt["elv_rel"]))
		compositeTerrain["elv_real"].append(float(threeDepPt["elv_real"]))
		compositeTerrain["lat"].append(float(threeDepPt["lat"]))
		compositeTerrain["lon"].append(float(threeDepPt["lon"]))
		compositeTerrain["lstf"].append(float(ls8_cp["lstf"]))
		compositeTerrain["lstf_serc"].append(float(lstfSerc))
		compositeTerrain["lstf_arc"].append(float(lstfArc))
		compositeTerrain["ndvi"].append(float(ls8_cp["ndvi"]))
		compositeTerrain["ndvi_serc"].append(float(ndviSerc))
		compositeTerrain["ndvi_arc"].append(float(ndviArc))
		compositeTerrain["ndmi"].append(float(ls8_cp["ndmi"]))
		compositeTerrain["ndmi_serc"].append(float(ndmiSerc))
		compositeTerrain["ndmi_arc"].append(float(ndmiArc))
		compositeTerrain["idx_row"].append(int(threeDepPt["idx_row"]))
		compositeTerrain["idx_col"].append(int(threeDepPt["idx_col"]))

######################################################################################
outputFileName = locationKey+'_3depLs8Comp.csv'
outputPath = os.path.join(publicTerrain_dir, outputFileName)

ranges = {}
rangeKeys = ["lstf", "ndvi", "ndmi"]
for key in rangeKeys:
	ranges[key+"_min"] = min(compositeTerrain[key])
	ranges[key+"_max"] = max(compositeTerrain[key])
	try:
		ranges[key+"ArcPos_min"] = min(poolData[key+"_arc_pos"])
		ranges[key+"ArcPos_max"] = max(poolData[key+"_arc_pos"])
	except ValueError:
		ranges[key+"ArcPos_min"] = 0
		ranges[key+"ArcPos_max"] = 0
	try:
		ranges[key+"ArcNeg_min"] = min(poolData[key+"_arc_neg"])
		ranges[key+"ArcNeg_max"] = max(poolData[key+"_arc_neg"])
	except ValueError:
		ranges[key+"ArcNeg_min"] = 0
		ranges[key+"ArcNeg_max"] = 0

poolData.clear()

def create_flag(metricKey, value):
    if value > 0:
        rangeKey_min = metricKey + "ArcPos_min"
        rangeKey_max = metricKey + "ArcPos_max"
        flag_prefix = "pos"
    elif value < 0:
        rangeKey_min = metricKey + "ArcNeg_min"
        rangeKey_max = metricKey + "ArcNeg_max"
        flag_prefix = "neg"
    else:
        return None  # or "neutral", depending on your logic

    range_min = ranges[rangeKey_min]
    range_max = ranges[rangeKey_max]

    try:
        normalized_value = (abs(value) - range_min) / (range_max - range_min)
    except ZeroDivisionError:
        normalized_value = 0.5

    if normalized_value >= 0.75:
        return f"{flag_prefix}_h"
    elif 0.5 <= normalized_value < 0.75:
        return f"{flag_prefix}_mh"
    elif 0.25 <= normalized_value < 0.5:
        return f"{flag_prefix}_ml"
    elif normalized_value < 0.25:
        return f"{flag_prefix}_l"

#--------------------------------------------------------------------------------------
# Make storage bins for flags
compositeTerrain["lstf_flag"] = []
compositeTerrain["ndvi_flag"] = []
compositeTerrain["ndmi_flag"] = []

for i in range(len(compositeTerrain["lstf"])):
	lstfArc = compositeTerrain["lstf_arc"][i]
	ndviArc = compositeTerrain["ndvi_arc"][i]
	ndmiArc = compositeTerrain["ndmi_arc"][i]

	compositeTerrain["lstf_flag"].append(create_flag("lstf", lstfArc))
	compositeTerrain["ndvi_flag"].append(create_flag("ndvi", ndviArc))
	compositeTerrain["ndmi_flag"].append(create_flag("ndmi", ndmiArc))

#--------------------------------------------------------------------------------------
df = pd.DataFrame({
    "lstf": compositeTerrain["lstf"],
    "lstf_serc": compositeTerrain["lstf_serc"],
    "lstf_arc": compositeTerrain["lstf_arc"],
    "lstf_flag": compositeTerrain["lstf_flag"],
    "ndvi": compositeTerrain["ndvi"],
    "ndvi_serc": compositeTerrain["ndvi_serc"],
    "ndvi_arc": compositeTerrain["ndvi_arc"],
    "ndvi_flag": compositeTerrain["ndvi_flag"],
	"ndmi": compositeTerrain["ndmi"],
	"ndmi_serc": compositeTerrain["ndmi_serc"],
	"ndmi_arc": compositeTerrain["ndmi_arc"],
	"ndmi_flag": compositeTerrain["ndmi_flag"],
    "lat": compositeTerrain["lat"],
    "lon": compositeTerrain["lon"],
    "elv_rel": compositeTerrain["elv_rel"],
    "elv_real": compositeTerrain["elv_real"],
	"idx_row": compositeTerrain["idx_row"],
	"idx_col": compositeTerrain["idx_col"]
})

######################################################################################
conn = sqlite3.connect('tempGeo.db')
cursor = conn.cursor()

cursor.execute("DELETE FROM terrainComposite")
conn.commit()

# cursor = conn.cursor()

'''for osmFturCategory in ["highway", "building", "construction"]:
	osmFturPts = []
	osmFturRows = []
	for row in cursor.fetchall():
		# row structure: geoid, lat, lon, surface
		geoid, lat, lon, surface = row[0], row[1], row[2], row[3]
		osmFturPts.append((lon, lat))
		osmFturRows.append(row)

	osmFturCKDTree = cKDTree(osmFturPts)'''
######################################################################################
#write to csv
with open(os.path.join(r"output\3dep\test.csv"), mode='w', newline='', encoding='utf-8') as file:
	writer = csv.writer(file)
	
	writer.writerow([
		"geoid", "lat", "lon", "lstf", "lstf_serc", "lstf_arc",
		"lstf_flag", "ndvi", "ndvi_serc", " ndvi_arc", "ndvi_flag",
		"ndmi", "ndmi_serc", "ndmi_arc", "ndmi_flag",
		"elv_rel", "elv", "idx_row", "idx_col",
		"mdrdasp", "mdrdconc", "mdrdgrv", "mdrdunp", "mdconst", "mdbldg"
		])
	
	distance_by_srf = {
		"asphalt":{"dist": [], "mean_dist": None},
		"concrete":{"dist": [], "mean_dist": None},
		"gravel":{"dist": [], "mean_dist": None},
		"unpaved":{"dist": [], "mean_dist": None},
		"building":{"dist": [], "mean_dist": None},
		"construction":{"dist": [], "mean_dist": None},
	}

	# Build CKDTrees for all OSM feature categories first
	osmFeatureTrees = {}
	for osmFturCategory in ["highway", "building", "construction"]:
		query = f"SELECT * FROM {osmFturCategory}_srf"
		cursor.execute(query)
		
		osmFturPts = []
		osmFturRows = []
		for row in cursor.fetchall():
			# row structure: geoid, lat, lon, surface
			geoid, lat, lon, srf = row[0], row[1], row[2], row[3]
			osmFturPts.append((lon, lat))
			osmFturRows.append(row)
		
		if osmFturPts:
			osmFeatureTrees[osmFturCategory] = {
				'tree': cKDTree(osmFturPts),
				'points': osmFturPts,
				'rows': osmFturRows
			}

	# Now process each terrain point once
	for idx, dfRow in df.iterrows():
		terrainPt = np.array([dfRow['lon'], dfRow['lat']])
		
		# Reset distances for this terrain point
		for srfKey in distance_by_srf.keys():
			distance_by_srf[srfKey]["dist"] = []
			distance_by_srf[srfKey]["mean_dist"] = None
		
		# Query all OSM feature categories for this terrain point
		for osmFturCategory, treeData in osmFeatureTrees.items():
			osmFturCKDTree = treeData['tree']
			osmFturPts = treeData['points']
			osmFturRows = treeData['rows']
			
			indices = osmFturCKDTree.query_ball_point(terrainPt, r=0.005)
			
			for index in indices:
				osmFtur_row = osmFturRows[index]
				geoid, lat, lon, srf = osmFtur_row
				
				distance = np.linalg.norm(terrainPt - np.array(osmFturPts[index]))
				
				if distance < 0.005:  # Approx ~500 meters
					distanceFt = (distance * 100000) / 3.28  # Convert to feet
					
					# Map surface types to distance_by_srf keys
					if srf in distance_by_srf:
						distance_by_srf[srf]["dist"].append(distanceFt)
					elif osmFturCategory == "building":
						distance_by_srf["building"]["dist"].append(distanceFt)
					elif osmFturCategory == "construction":
						distance_by_srf["construction"]["dist"].append(distanceFt)
		
		# Calculate mean distances for this terrain point
		for srfKey, obj in distance_by_srf.items():
			if obj["dist"]:
				avg_distance = sum(obj["dist"]) / len(obj["dist"])
				distance_by_srf[srfKey]["mean_dist"] = avg_distance
		
		# Insert into database once per terrain point
		try:
			cursor.execute(
				'''INSERT INTO terrainComposite (
				geoid, lat, lon, lstf, lstf_serc, lstf_arc,
				lstf_flag, ndvi, ndvi_serc, ndvi_arc, ndvi_flag,
				ndmi, ndmi_serc, ndmi_arc, ndmi_flag,
				elv_rel, elv, idx_row, idx_col, 
				mdrdasp, mdrdconc, mdrdgrv, mdrdunp, mdconst, mdbldg) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
				(idx, dfRow['lat'], dfRow['lon'],
				dfRow['lstf'], dfRow['lstf_serc'], dfRow['lstf_arc'],
				dfRow['lstf_flag'], dfRow['ndvi'], dfRow['ndvi_serc'],
				dfRow['ndvi_arc'], dfRow['ndvi_flag'], dfRow['ndmi'],
				dfRow['ndmi_serc'], dfRow['ndmi_arc'], dfRow['ndmi_flag'],
				dfRow['elv_rel'], dfRow['elv_real'], dfRow['idx_row'],
				dfRow['idx_col'], distance_by_srf["asphalt"]["mean_dist"],
				distance_by_srf["concrete"]["mean_dist"], distance_by_srf["gravel"]["mean_dist"],
				distance_by_srf["unpaved"]["mean_dist"], distance_by_srf["construction"]["mean_dist"], 
				distance_by_srf["building"]["mean_dist"])
			)
		except Exception as e:
			print(f"Error inserting row {idx}: {e}")
		
		# Write to CSV once per terrain point
		writer.writerow([
			idx, dfRow['lat'], dfRow['lon'],
			dfRow['lstf'], dfRow['lstf_serc'], dfRow['lstf_arc'], dfRow['lstf_flag'], 
			dfRow['ndvi'], dfRow['ndvi_serc'], dfRow['ndvi_arc'], dfRow['ndvi_flag'], 
			dfRow['ndmi'], dfRow['ndmi_serc'], dfRow['ndmi_arc'], dfRow['ndmi_flag'],
			dfRow['elv_rel'], dfRow['elv_real'], 
			dfRow['idx_row'], dfRow['idx_col'], 
			distance_by_srf["asphalt"]["mean_dist"],
			distance_by_srf["concrete"]["mean_dist"], 
			distance_by_srf["gravel"]["mean_dist"],
			distance_by_srf["unpaved"]["mean_dist"], 
			distance_by_srf["construction"]["mean_dist"],
			distance_by_srf["building"]["mean_dist"]
		])

conn.commit()
conn.close()
print("Composite data saved to database.")