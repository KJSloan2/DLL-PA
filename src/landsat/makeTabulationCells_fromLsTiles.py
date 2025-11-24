import json
import sqlite3
import os
import math
from shapely.geometry import Polygon
from geopy.distance import distance
from geopy.point import Point
import numpy as np
######################################################################################
logJson = json.load(open(os.path.join(r"resources/A0_log.json")))
locationId = logJson["location_key"]
######################################################################################
def update_point(pt, movement):
	"""Move the given point by specified distances east and south."""
	latitude, longitude = pt
	dist_east, dist_south = movement

	start_point = Point(latitude, longitude)
	east_point = distance(meters=dist_east).destination(point=start_point, bearing=90)
	final_point = distance(meters=dist_south).destination(point=east_point, bearing=180)

	return final_point.latitude, final_point.longitude
######################################################################################
def haversine_meters(pt1, pt2):
	"""Calculate the distance in meters between two points."""
	R = 6371000  # Radius of Earth in meters
	phi1, phi2 = math.radians(pt1[0]), math.radians(pt2[0])
	delta_phi = math.radians(pt2[0] - pt1[0])
	delta_lambda = math.radians(pt2[1] - pt1[1])

	a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

	return R * c  # Distance in meters
######################################################################################
output = {"type": "FeatureCollection", "features": []}
######################################################################################
FROM_TILES = False

if FROM_TILES == True:
	tiles = logJson['tiles'][str(logJson['year_start'])]
	tileSize = 500
	cellCount = 3
else:
	tiles = [{
		'geometry':{
			'pt_nw':[-103.2102579789362, 36.3429816620186],
			'pt_se':[-94.19048261442563, 29.689458780031472]
		}
	}]

for tile_id, tile in tiles.items():
	pt_nw = tile['geometry']['pt_nw']
	pt_se = tile['geometry']['pt_se']
	pt_ne = [pt_nw[0], pt_se[1]]

	bb_width = (pt_ne[1] - pt_nw[1])
	bb_height = (pt_ne[0] - pt_se[0])

	metersPerPixelWidth = bb_width / tileSize
	metersPerPixelHeight = bb_height / tileSize

	steps = tileSize/cellCount
	stepsWidth = bb_width/cellCount
	stepsHeight = bb_height/cellCount

	#0.13500618375057627 0.135225261249861 0.027001236750115253 0.027045052249972203
	print(bb_width, bb_height, stepsWidth, stepsHeight)

	lat_start = pt_nw[0]
	lon_start = pt_nw[1]

	print(bb_width/cellCount, bb_height/cellCount)

	for i in range(cellCount):
		for j in range(cellCount):
			bb_pt_nw = [lat_start, lon_start]
			bb_pt_se = [float(bb_pt_nw[0])+stepsHeight, float(bb_pt_nw[1])+stepsWidth]
			bb_pt_ne = [bb_pt_nw[0], bb_pt_se[1]]
			bb_pt_sw = [bb_pt_se[0], bb_pt_nw[1]]

			mean_lat = np.mean([bb_pt_nw[0], bb_pt_ne[0], bb_pt_se[0], bb_pt_sw[0]])
			mean_lon = np.mean([bb_pt_nw[1], bb_pt_ne[1], bb_pt_se[1], bb_pt_sw[1]])

			cell_diam = haversine_meters([mean_lat, mean_lon], [bb_pt_nw[0], bb_pt_nw[1]])

			#cellId = f"{tileId}_{i}-{j}"
			cell_id = str(i)+"-"+str(j)
			feature = {
				"type": "Feature",
				"geometry": {
					"type": "Polygon",
					"coordinates": [[
						[bb_pt_nw[1],bb_pt_nw[0]],
						[bb_pt_ne[1],bb_pt_ne[0]],
						[bb_pt_se[1],bb_pt_se[0]],
						[bb_pt_sw[1],bb_pt_sw[0]]
						]]
				},
				"properties": {
					"cell_id": cell_id,
					"tile_id": tile_id,
					"cell_centroid":[mean_lat, mean_lon],
					"cell_diam":cell_diam,
					"mean_lstf": 0,
					"mean_ndvi":0,
					"roc_lstf": 0,
					"roc_ndvi": 0,
					"proximity":{
						"airport":None,
						"hospital":None
					}
				}
			}
			output["features"].append(feature)

			lon_start = bb_pt_se[1]  # Move east

		lon_start = pt_nw[1]  # Reset longitude
		lat_start+=stepsHeight  # Move south
######################################################################################
with open(os.path.join(r"resources", "A0_tileReferencePolygons.geojson"), "w", encoding='utf-8') as output_json:
	json.dump(output, output_json, indent=1, ensure_ascii=False)
######################################################################################
print("DONE")