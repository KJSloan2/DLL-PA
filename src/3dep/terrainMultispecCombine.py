import os
from os import listdir
from os.path import isfile, join
import sys

import csv
import json
import sqlite3
import duckdb
import pyarrow as pa
import polars

import pandas as pd
import numpy as np
from geopy.distance import geodesic
import shapely
from shapely.geometry import Point, Polygon
from scipy.spatial import cKDTree
from datetime import datetime

import subprocess

'''from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
from shapely.geometry.base import BaseGeometry'''
################### ###################################################################
APPLY_SPATIAL_FILTER = False
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.utils import safe_round, polygon_filter, fill_nulls
from spatial_utils import pt_in_geom, nearby_feature
from global_functions.multispecFunctions import classify_land_cover
from global_functions.sqlite_utils import get_table_info
from databases.terrain_composite.duckDb_create_terrainComposite import create_duckDb_terrainComposite
######################################################################################
DB_NAME = "runtime.db"
DB_TABLE = "site_info"

siteInfo = get_table_info(DB_NAME, DB_TABLE, ["NAME", "AOI_BB_PT_SW", "AOI_BB_PT_NE", "AOI_CENTROID", "STATE_FIPS", "COUNTY_FIPS"])
siteName = siteInfo['NAME']
aoi_bb_pt_sw = siteInfo["AOI_BB_PT_SW"]
aoi_bb_pt_ne = siteInfo["AOI_BB_PT_NE"]
aoi_centroid = siteInfo["AOI_CENTROID"]
stateFips = siteInfo["STATE_FIPS"]
countyFips = siteInfo["COUNTY_FIPS"]
######################################################################################
def list_files(directory):
	return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
######################################################################################
if APPLY_SPATIAL_FILTER:
	filterPolygonFileName = "aoi_polygon_2025-12-09.geojson"
	filterPolygonFile_path = os.path.join(r"data", "spatial_filters", filterPolygonFileName)
	with open(filterPolygonFile_path) as f:
		filterPolygonJson = json.load(f)
	filterPolygonCoords = filterPolygonJson["features"][0]["geometry"]["coordinates"]
	filterPolygon = shapely.geometry.shape(filterPolygonJson["features"][0]["geometry"])
######################################################################################
conn = duckdb.connect('tempGeo.duckdb')
cursor = conn.cursor()

#cursor.execute(f"DELETE FROM spectral_temporal")
query = f"SELECT * FROM spectral_temporal"
cursor.execute(query)

rows_spectralTemporal = cursor.fetchall()
columnNames_spectralTemporal = [description[0] for description in cursor.description]
multiSpecData = pd.DataFrame(rows_spectralTemporal, columns=columnNames_spectralTemporal)
######################################################################################
query = f"SELECT * FROM three_dep"
cursor.execute(query)
rows_threeDep = cursor.fetchall()
columnNames_threeDep = [description[0] for description in cursor.description]
threeDepData = pd.DataFrame(rows_threeDep, columns=columnNames_threeDep)
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
print(f"Site name: {siteName}")
conn_refDb = sqlite3.connect('runtime.db')
cursor_refDb = conn_refDb.cursor()
cursor_refDb.execute("SELECT HAS_HYDRO_FEATURES FROM site_info WHERE NAME = ?", (siteName,))
result_siteInfo = cursor_refDb.fetchone()
hasHydroFeatures = result_siteInfo[0]

print(f"Has hydro TIGER features: {hasHydroFeatures}")

hydroFeatureGeom = []
ptsInHydroFeatures = 0
if hasHydroFeatures:
	hydroFeatures_fName = siteName+"_hydro.geojson"
	hydroFeatures_geojson_path = os.path.join("data", "hydro", hydroFeatures_fName)
	with open(hydroFeatures_geojson_path, "r") as hydroFeatures_geojson_file:
		hydroFeatures_geojson = json.load(hydroFeatures_geojson_file)
		for feature in hydroFeatures_geojson["features"]:
			feature_geometry = feature["geometry"]
			geometry_type = feature_geometry["type"]
			if geometry_type == "Polygon":
				hydroFeatureGeom.append(Polygon(feature_geometry["coordinates"][0]))
				ptsInHydroFeatures+=1
print(f"Pts in hydro features: {ptsInHydroFeatures}")

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
	"lstf":[], "lstf_serc":[], "lstf_arc":[],
	"ndvi":[], "ndvi_serc":[], "ndvi_arc":[],
	"ndmi":[], "ndmi_serc":[], "ndmi_arc":[],
	"elv_rel":[], "elv":[],
	"idx_row":[], "idx_col":[],
	"lstf_ndvi_corr":[], "lstf_ndmi_corr":[], "ndvi_ndmi_corr":[],
	"lstf_ndvi_pval":[], "lstf_ndmi_pval":[], "ndvi_ndmi_pval":[],
	"lstf_temporal":[], "ndvi_temporal":[], "ndmi_temporal":[]
}

poolData = {
	"lstf_arc_neg": [], "lstf_arc_pos": [],
	"lstf_serc_neg": [],"lstf_serc_pos": [],
	"ndvi_arc_neg": [], "ndvi_arc_pos": [],
	"ndvi_serc_neg": [], "ndvi_serc_pos": [],
	"ndmi_arc_neg": [], "ndmi_arc_pos": [],
	"ndmi_serc_neg": [], "ndmi_serc_pos": [],
	"lstf_ndvi_corr":[], "lstf_ndmi_corr":[], "ndvi_ndmi_corr":[],
	"lstf_ndvi_pval":[], "lstf_ndmi_pval":[], "ndvi_ndmi_pval":[]
}

def append_pool(metricKey, value, pool=poolData):
	if value < 0:
		pool[metricKey+"_arc_neg"].append(abs(value))
	elif value > 0:
		pool[metricKey+"_arc_pos"].append(value)

if threeDepData.empty:
	print("No 3DEP data found for the location, using Landsat data only.")
	for idx, row in multiSpecData.iterrows():
		compositeTerrain["lat"].append(float(row["lat"]))
		compositeTerrain["lon"].append(float(row["lon"]))
		compositeTerrain["lstf"].append(float(row["lstf"]))
		compositeTerrain["lstf_serc"].append(float(row["lstf_serc"]))
		compositeTerrain["lstf_arc"].append(float(row["lstf_arc"]))
		compositeTerrain["ndvi"].append(float(row["ndvi"]))
		compositeTerrain["ndvi_serc"].append(float(row["ndvi_serc"]))
		compositeTerrain["ndvi_arc"].append(float(row["ndvi_arc"]))
		compositeTerrain["ndmi"].append(float(row["ndmi"]))
		compositeTerrain["ndmi_serc"].append(float(row["ndmi_serc"]))
		compositeTerrain["ndmi_arc"].append(float(row["ndmi_arc"]))
		compositeTerrain["elv_rel"].append(1)
		compositeTerrain["elv"].append(1)
		compositeTerrain["idx_row"].append(row["idx_row"])
		compositeTerrain["idx_col"].append(row["idx_col"])
		compositeTerrain["lstf_ndvi_corr"].append(float(row["lstf_ndvi_corr"]))
		compositeTerrain["lstf_ndmi_corr"].append(float(row["lstf_ndmi_corr"]))
		compositeTerrain["ndvi_ndmi_corr"].append(float(row["ndvi_ndmi_corr"]))
		compositeTerrain["lstf_ndvi_pval"].append(float(row["lstf_ndvi_pval"]))
		compositeTerrain["lstf_ndmi_pval"].append(float(row["lstf_ndmi_pval"]))
		compositeTerrain["ndvi_ndmi_pval"].append(float(row["ndvi_ndmi_pval"]))
		# Convert from string to list of floats
		lstf_temporal = list(map(lambda x: round(float(x), 1), row["lstf_temporal"].split(',')))
		ndvi_temporal = list(map(lambda x: round(float(x), 1), row["ndvi_temporal"].split(',')))
		ndmi_temporal = list(map(lambda x: round(float(x), 1), row["ndmi_temporal"].split(',')))
		compositeTerrain["lstf_temporal"].append(lstf_temporal)
		compositeTerrain["ndvi_temporal"].append(ndvi_temporal)
		compositeTerrain["ndmi_temporal"].append(ndmi_temporal)
else:
	print("Begin 3DEP + Landsat composite processing")
	for idx, row in multiSpecData.iterrows():
		#lon,lat,lstf,lstf_serc,lstf_arc,ndvi,ndvi_serc,ndvi_arc
		lstf = row["lstf"]
		lstf_serc = row["lstf_serc"]
		lstf_arc = row["lstf_arc"]

		ndvi = row["ndvi"]
		ndvi_serc = row["ndvi_serc"]
		ndvi_arc = row["ndvi_arc"]

		ndmi = row["ndmi"]
		ndmi_serc = row["ndmi_serc"]
		ndmi_arc = row["ndmi_arc"]

		append_pool("lstf", lstf_arc)
		append_pool("ndvi", ndvi_arc)
		append_pool("ndmi", ndmi_arc)

		#-98.93307920759433,30.40357062255362
		pt = Point(row['lat'], row['lon'])
		landsatPoints.append(pt)

	print("Construct landsatCkdTree")
	landsatCkdTreePoints = np.array([[p.x, p.y] for p in landsatPoints])
	landsatCkdTree = cKDTree(landsatCkdTreePoints)
	print("Construct landsatCkdTree complete")
	######################################################################################
	for idx, threeDepPt in threeDepData.iterrows():
		addPoint = True
		lat, lon = threeDepPt['lat'], threeDepPt['lon']
		if APPLY_SPATIAL_FILTER:
			addPoint = polygon_filter(lon, lat, filterPolygon)
		if addPoint == True:
			query_coords = np.array([threeDepPt['lon'], threeDepPt['lat']])
			distance, index = landsatCkdTree.query(query_coords)
			msd_cp = multiSpecData.iloc[index]
			# lstf
			lstfSerc = msd_cp["lstf_serc"]
			lstfArc = msd_cp["lstf_arc"]
			# ndvi
			ndviSerc = msd_cp["ndvi_serc"]
			ndviArc = msd_cp["ndvi_arc"]
			# ndmi
			ndmiSerc = msd_cp["ndmi_serc"]
			ndmiArc = msd_cp["ndmi_arc"]

			compositeTerrain["elv_rel"].append(float(threeDepPt["elv_rel"]))
			compositeTerrain["elv"].append(float(threeDepPt["elv"]))
			compositeTerrain["lat"].append(float(threeDepPt["lat"]))
			compositeTerrain["lon"].append(float(threeDepPt["lon"]))
			compositeTerrain["lstf"].append(float(msd_cp["lstf"]))
			compositeTerrain["lstf_serc"].append(float(lstfSerc))
			compositeTerrain["lstf_arc"].append(float(lstfArc))
			compositeTerrain["ndvi"].append(float(msd_cp["ndvi"]))
			compositeTerrain["ndvi_serc"].append(float(ndviSerc))
			compositeTerrain["ndvi_arc"].append(float(ndviArc))
			compositeTerrain["ndmi"].append(float(msd_cp["ndmi"]))
			compositeTerrain["ndmi_serc"].append(float(ndmiSerc))
			compositeTerrain["ndmi_arc"].append(float(ndmiArc))
			compositeTerrain["idx_row"].append(int(threeDepPt["idx_row"]))
			compositeTerrain["idx_col"].append(int(threeDepPt["idx_col"]))
			compositeTerrain["lstf_ndvi_corr"].append(msd_cp["lstf_ndvi_corr"])
			compositeTerrain["lstf_ndmi_corr"].append(msd_cp["lstf_ndmi_corr"])
			compositeTerrain["ndvi_ndmi_corr"].append(msd_cp["ndvi_ndmi_corr"])
			compositeTerrain["lstf_ndvi_pval"].append(msd_cp["lstf_ndvi_pval"])
			compositeTerrain["lstf_ndmi_pval"].append(msd_cp["lstf_ndmi_pval"])
			compositeTerrain["ndvi_ndmi_pval"].append(msd_cp["ndvi_ndmi_pval"])

			# Convert from string to list of floats
			lstf_temporal = list(map(lambda x: round(float(x), 1), msd_cp["lstf_temporal"].split(',')))
			ndvi_temporal = list(map(lambda x: round(float(x), 1), msd_cp["ndvi_temporal"].split(',')))
			ndmi_temporal = list(map(lambda x: round(float(x), 1), msd_cp["ndmi_temporal"].split(',')))
			compositeTerrain["lstf_temporal"].append(lstf_temporal)
			compositeTerrain["ndvi_temporal"].append(ndvi_temporal)
			compositeTerrain["ndmi_temporal"].append(ndmi_temporal)
	
	print(f"{len(compositeTerrain['lat'])} Composite points added")

######################################################################################\
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
	flagMultiplier = 0
	if value > 0:
		rangeKey_min = metricKey + "ArcPos_min"
		rangeKey_max = metricKey + "ArcPos_max"
		flagMultiplier = 1
	elif value < 0:
		rangeKey_min = metricKey + "ArcNeg_min"
		rangeKey_max = metricKey + "ArcNeg_max"
		flagMultiplier = -1
	else:
		return None

	range_min = ranges[rangeKey_min]
	range_max = ranges[rangeKey_max]

	try:
		normalized_value = (abs(value) - range_min) / (range_max - range_min)
	except ZeroDivisionError:
		normalized_value = 0.5

	if normalized_value >= 0.75:
		return 4*flagMultiplier
	elif 0.5 <= normalized_value < 0.75:
		return 3*flagMultiplier
	elif 0.25 <= normalized_value < 0.5:
		return 2*flagMultiplier
	elif normalized_value < 0.25:
		return 1*flagMultiplier
	else:
		return 0

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
df_compositeTerrain = pd.DataFrame({
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
	"elv": compositeTerrain["elv"],
	"idx_row": compositeTerrain["idx_row"],
	"idx_col": compositeTerrain["idx_col"],
	"lstf_ndvi_corr": compositeTerrain["lstf_ndvi_corr"],
	"lstf_ndmi_corr": compositeTerrain["lstf_ndmi_corr"],
	"ndvi_ndmi_corr": compositeTerrain["ndvi_ndmi_corr"],
	"lstf_ndvi_pval": compositeTerrain["lstf_ndvi_pval"],
	"lstf_ndmi_pval": compositeTerrain["lstf_ndmi_pval"],
	"ndvi_ndmi_pval": compositeTerrain["ndvi_ndmi_pval"],
	"lstf_temporal": compositeTerrain["lstf_temporal"],
	"ndvi_temporal": compositeTerrain["ndvi_temporal"],
	"ndmi_temporal": compositeTerrain["ndmi_temporal"]
})
print("df_compositeTerrain created with ", len(df_compositeTerrain), " rows")
######################################################################################
def get_distance_value(mean_dist):
	return mean_dist if mean_dist is not None else 0
######################################################################################
#cursor.execute("DELETE FROM terrain_composite")
#conn.commit()

#create_duckDb_terrainComposite(conn)

conn.execute("DROP TABLE IF EXISTS terrain_composite")

create_duckDb_terrainComposite(conn)

print("Recreate terrain_composite")
######################################################################################
MAX_DIST_FT = 300
MIN_DIST_FT = 10

distance_by_srf = {
	"asphalt":{"dist": [], "mean_dist": None},
	"concrete":{"dist": [], "mean_dist": None},
	"gravel":{"dist": [], "mean_dist": None},
	"unpaved":{"dist": [], "mean_dist": None},
	"building":{"dist": [], "mean_dist": None},
	"construction":{"dist": [], "mean_dist": None},
}

# Build CKDTrees for all OSM feature categories first
'''osmFeatureTrees = {}
for osmCategory in ["highway", "building", "construction"]:
	query = f"SELECT * FROM {osmCategory}_srf"
	cursor.execute(query)
	
	osmFturPts = []
	osmFturRows = []
	for row in cursor.fetchall():
		# row structure: geoid, lat, lon, surface
		geoid, lat, lon, srf = row[0], row[1], row[2], row[3]
		osmFturPts.append((lon, lat))
		osmFturRows.append(row)
	print(f"Construct CKDTree for {osmCategory}: {len(osmFturPts)} points")
	if osmFturPts:
		osmFeatureTrees[osmCategory] = {
			"tree": cKDTree(osmFturPts),
			"points": osmFturPts,
			"rows": osmFturRows
		}'''

osmFeatureTrees = {}
for osmCategory in ["highway", "building", "construction"]:
	print(f"Querying {osmCategory}_srf for CKDTree construction")
	query = f"""
		SELECT geoid, lat, lon, srf
		FROM {osmCategory}_srf
		WHERE lon IS NOT NULL AND lat IS NOT NULL
	"""

	pl_df = conn.execute(query).pl()

	if pl_df.height == 0:
		print(f"Construct CKDTree for {osmCategory}: 0 points")
		continue

	pts = pl_df.select(["lon", "lat"]).to_numpy()
	rows = pl_df.rows()

	print(f"Construct CKDTree for {osmCategory}: {len(pts)} points")

	osmFeatureTrees[osmCategory] = {
		"tree": cKDTree(pts),
		"points": pts,
		"rows": rows
	}
print("Finished building CKDTrees for OSM features.")
######################################################################################
output = []
# Now process each terrain point once
guid = 0
for idx, dfRow in df_compositeTerrain.iterrows():
	#ctLat, ctLon = dfRow['lat'], dfRow['lon']
	terrainPt = np.array([dfRow['lon'], dfRow['lat']])

	# Reset distances for this terrain point
	for srfKey in distance_by_srf.keys():
		distance_by_srf[srfKey]["dist"] = []
		distance_by_srf[srfKey]["mean_dist"] = 1000
	
	# Query all OSM feature categories for this terrain point
	for osmCategory, treeData in osmFeatureTrees.items():
		osmFturCKDTree = treeData["tree"]
		osmFturPts = treeData["points"]
		osmFturRows = treeData["rows"]
		
		indices = osmFturCKDTree.query_ball_point(terrainPt, r=0.002)
		
		for index in indices:
			osmFtur_row = osmFturRows[index]
			geoid, lat, lon, osmCatSrf = osmFtur_row
			srf = osmCatSrf.split('_')[1]
			
			distance = np.linalg.norm(terrainPt - np.array(osmFturPts[index]))
			
			if distance <= 0.003:
				distanceFt = (distance * 100000) / 3.28  # Convert to feet
				# Map surface types to distance_by_srf keys
				if srf in distance_by_srf:
					distance_by_srf[srf]["dist"].append(distanceFt)
				elif osmCategory == "building":
					distance_by_srf["building"]["dist"].append(distanceFt)
				elif osmCategory == "construction":
					distance_by_srf["construction"]["dist"].append(distanceFt)
			elif distance > 0.003:
				if srf in distance_by_srf:
					distance_by_srf[srf]["dist"].append(MAX_DIST_FT)
				elif osmCategory == "building":
					distance_by_srf["building"]["dist"].append(MAX_DIST_FT)
				elif osmCategory == "construction":
					distance_by_srf["construction"]["dist"].append(MAX_DIST_FT)
	# Calculate mean distances for this terrain point
	for srfKey, obj in distance_by_srf.items():
		if obj["dist"]:
			avg_distance = sum(obj["dist"]) / len(obj["dist"])
			distance_by_srf[srfKey]["mean_dist"] = avg_distance
		else:
			# Set to None or a default value when no nearby features found
			distance_by_srf[srfKey]["mean_dist"] = None

	distToRoad_asphalt = fill_nulls(distance_by_srf["asphalt"]["mean_dist"], MAX_DIST_FT)
	distToRoad_concrete = fill_nulls(distance_by_srf["concrete"]["mean_dist"], MAX_DIST_FT)
	distToRoad_gravel = fill_nulls(distance_by_srf["gravel"]["mean_dist"], MAX_DIST_FT)
	distToRoad_unpaved = fill_nulls(distance_by_srf["unpaved"]["mean_dist"], MAX_DIST_FT)
	distToStructure_construction = fill_nulls(distance_by_srf["construction"]["mean_dist"], MAX_DIST_FT)
	distToStructure_building = fill_nulls(distance_by_srf["building"]["mean_dist"], MAX_DIST_FT)
	##############################################################################
	query_pt = Point(dfRow["lon"], dfRow["lat"])
	# Set land cover classification to None as default
	lsLandCover = None
	# If hydro features are present, check if point is within any hydro feature polygons
	if hasHydroFeatures:
		pt_inHydroFeature = pt_in_geom(query_pt, hydroFeatureGeom)
	# If point was not in hydro feature, classify based on nearest features
	if lsLandCover is None:
		distDict = {
			"RA":distance_by_srf["asphalt"]["mean_dist"],
			"RC":distance_by_srf["concrete"]["mean_dist"],
			"BG": distance_by_srf["building"]["mean_dist"]
		}
		lsLandCover = nearby_feature(distDict, MIN_DIST_FT)
	# If point still has no land cover, classify based on spectral indices
	if lsLandCover is None:
		lsLandCover = classify_land_cover(dfRow["ndvi"], dfRow["ndmi"])
	# If land cover is still None, set to 'UN' for unknown
	if lsLandCover is None:
		lsLandCover = "UN"
	##############################################################################
	lstf_temporal_str = f"[{','.join([f'{x:.1f}' for x in dfRow['lstf_temporal']])}]"
	ndvi_temporal_str = f"[{','.join([f'{x:.1f}' for x in dfRow['ndvi_temporal']])}]"
	ndmi_temporal_str = f"[{','.join([f'{x:.1f}' for x in dfRow['ndmi_temporal']])}]"
	output.append({
		"geoid" : guid, 
		"lat": dfRow["lat"],
		"lon": dfRow["lon"],
		"lstf": dfRow["lstf"],
		"lstf_serc": dfRow["lstf_serc"], 
		"lstf_arc": dfRow["lstf_arc"],
		"lstf_flag": dfRow["lstf_flag"],
		"ndvi": dfRow["ndvi"],
		"ndvi_serc": dfRow["ndvi_serc"],
		"ndvi_arc": dfRow["ndvi_arc"], 
		"ndvi_flag": dfRow["ndvi_flag"],
		"ndmi": dfRow["ndmi"],
		"ndmi_serc": dfRow["ndmi_serc"],
		"ndmi_arc": dfRow["ndmi_arc"],
		"ndmi_flag": dfRow["ndmi_flag"],
		"elv_rel": dfRow["elv_rel"],
		"elv": dfRow["elv"],
		"idx_row": dfRow["idx_row"],
		"idx_col": dfRow["idx_col"],
		"mdrdasp": distToRoad_asphalt, 
		"mdrdconc": distToRoad_concrete,
		"mdrdgrv": distToRoad_gravel, 
		"mdrdunp": distToRoad_unpaved, 
		"mdconst": distToStructure_construction, 
		"mdbldg": distToStructure_building,
		"lstf_ndvi_corr": dfRow["lstf_ndvi_corr"], 
		"lstf_ndmi_corr": dfRow["lstf_ndmi_corr"], 
		"ndvi_ndmi_corr": dfRow["ndvi_ndmi_corr"], 
		"lstf_ndvi_pval": dfRow["lstf_ndvi_pval"], 
		"lstf_ndmi_pval": dfRow["lstf_ndmi_pval"], 
		"ndvi_ndmi_pval": dfRow["ndvi_ndmi_pval"],
		"lstf_temporal": lstf_temporal_str, 
		"ndvi_temporal": ndvi_temporal_str, 
		"ndmi_temporal": ndmi_temporal_str, 
		"ls_land_cover": lsLandCover, 
		"cl_land_cover": None
	})
	guid+=1

terrain_composite_df = pd.DataFrame(output)
conn.register("terrain_composite_temp", terrain_composite_df)

conn.execute("""
    INSERT INTO terrain_composite (
        geoid, lat, lon, lstf, lstf_serc, 
        lstf_arc, lstf_flag, ndvi, ndvi_serc, ndvi_arc, 
        ndvi_flag, ndmi, ndmi_serc, ndmi_arc, ndmi_flag,
        elv_rel, elv, idx_row, idx_col, mdrdasp, 
        mdrdconc, mdrdgrv, mdrdunp, mdconst, mdbldg,
        lstf_ndvi_corr, lstf_ndmi_corr, ndvi_ndmi_corr, 
        lstf_ndvi_pval, lstf_ndmi_pval, ndvi_ndmi_pval,
        lstf_temporal, ndvi_temporal, ndmi_temporal, ls_land_cover, cl_land_cover
    )
    SELECT
        geoid, lat, lon, lstf, lstf_serc, 
        lstf_arc, lstf_flag, ndvi, ndvi_serc, ndvi_arc, 
        ndvi_flag, ndmi, ndmi_serc, ndmi_arc, ndmi_flag,
        elv_rel, elv, idx_row, idx_col, mdrdasp, 
        mdrdconc, mdrdgrv, mdrdunp, mdconst, mdbldg,
        lstf_ndvi_corr, lstf_ndmi_corr, ndvi_ndmi_corr, 
        lstf_ndvi_pval, lstf_ndmi_pval, ndvi_ndmi_pval,
        lstf_temporal, ndvi_temporal, ndmi_temporal, ls_land_cover, cl_land_cover
    FROM terrain_composite_temp
""")

conn.close()
print("Composite data saved to database.")