import numpy as np
from shapely.geometry import shape, Point, Polygon, MultiPolygon
from shapely.strtree import STRtree
import fiona
import os
import json
######################################################################################
import sys
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize, make_new_geojson_feature
######################################################################################    
from spatial_utils import haversine, mpolygon_yeild_pts
######################################################################################
distanceThreshold = 50  # miles
# Prepare paths
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]

acs_dir = os.path.join(root_dir, "frontend", "public", "acs")

lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))

public_dir = os.path.join(root_dir, "frontend", "public", "acs")

siteCoordinates = siteFileMap[locationKey]["coordinates"]
#siteCoordinates = [39.0441971020514, -95.69657621458288]
######################################################################################
query_point = Point(float(siteCoordinates[1]), float(siteCoordinates[0]))

insideStateName = None
#-94.980417, 32.553782
#-77.781381, 39.228009
with fiona.open(os.path.join(parent_dir, "resources", "reference", "tl_2024_us_state.geojson"), mode="r") as fc:
    for i, feature in enumerate(fc):
        state_geoid = feature["properties"]["GEOID"]
        state_name = feature["properties"]["NAME"]
        state_geom = shape(feature["geometry"])
        
        inside_state = False
        if isinstance(state_geom, Polygon):
            if state_geom.covers(query_point):
                print(f"Query point is inside Polygon: GEOID={state_geoid}, NAME={state_name}")
                inside_state = True
        elif isinstance(state_geom, MultiPolygon):
            for poly in state_geom.geoms:
                if poly.covers(query_point):
                    print(f"Query point is inside MultiPolygon: GEOID={state_geoid}, NAME={state_name}")
                    inside_state = True
                    break

        if inside_state:
            siteFileMap[locationKey]["state"] = {
                "geoid": state_geoid,
                "name": state_name
            }
            insideStateName = state_name
            break
######################################################################################
######################################################################################
fileDict = {
    "place_acs_s0101": f"s0101_{insideStateName}_uscbPlace.json",
    "place_acs_dp03": f"dp03_{insideStateName}_uscbPlace.json",
    "place_acs_s2801": f"s2801_{insideStateName}_uscbPlace.json",
    }

for tabulationAreaKey, fileName in fileDict.items():
    data_path = os.path.join("backend", "data", "acs", insideStateName, fileName)
    acsSurvey = str(fileName.split("_")[0])

    publicAcsGeoJson = json.load(open(os.path.join(acs_dir, f"{acsSurvey}_uscbPlace.json")))

    # Tabulation areas nearby
    taNearby = []
    taNearbyDist = []
    inside_place_found = False
    fcRef = []

    with open(data_path, 'r', encoding='utf-8') as f:
        place_data = json.load(f)
        for i, feature in enumerate(place_data['features']):
            fcRef.append(feature)
            geoid = feature["properties"]["GEOID"]
            name = feature["properties"]["NAME"]
            place_geom = shape(feature["geometry"])
            
            # Check for point inside feature
            inside_this_place = False
            if isinstance(place_geom, Polygon):
                if place_geom.covers(query_point):
                    print(f"Query point is inside Polygon: GEOID={geoid}, NAME={name}")
                    inside_this_place = True
                    print(name, 0)
            elif isinstance(place_geom, MultiPolygon):
                for poly in place_geom.geoms:
                    if poly.covers(query_point):
                        print(f"Query point is inside MultiPolygon: GEOID={geoid}, NAME={name}")
                        inside_this_place = True
                        print(name, 0)
                        break
            
            # Handle inside place
            if inside_this_place:
                inside_place_found = True
                siteFileMap[locationKey][tabulationAreaKey] = {"fc_idx": i, "geoid": geoid, "name": name}
                siteFileMap[locationKey]["closest_tabulation_area"] = {
                    "fc_idx": i, "geoid": geoid, "name": name, "distance_miles": 0.0
                }
                publicAcsGeoJson["features"].append(make_new_geojson_feature(feature))
                print(name, 0)

            else:
                for coord in mpolygon_yeild_pts(feature["geometry"]):
                    dist = haversine(coord, (query_point.x, query_point.y))
                    dist_miles = dist["ml"]
                    if dist_miles <= distanceThreshold:
                        taNearby.append({"fc_idx": i, "geoid": geoid, "name": name, "dist_miles": dist_miles})
                        taNearbyDist.append(dist_miles)
                        print(name, dist_miles)
                        publicAcsGeoJson["features"].append(make_new_geojson_feature(feature))
                        break

    # Only process nearby places if we didn't find any inside places
    if not inside_place_found and len(taNearbyDist) > 0:
        featureNearbyDist_argSort = np.argsort(taNearbyDist)
        featureClosestIdx = featureNearbyDist_argSort[0]
        featureClosest = taNearby[featureClosestIdx]
        
        siteFileMap[locationKey]["closest_tabulation_area"] = {
            "fc_idx": featureClosest["fc_idx"],
            "geoid": featureClosest["geoid"],
            "name": featureClosest["name"],
            "distance_miles": featureClosest["dist_miles"]
        }

        publicAcsGeoJson["features"].append(make_new_geojson_feature(fcRef[featureClosest["fc_idx"]]))
        
        # Don't modify taNearby before storing it

        #nearby_tabulation_areas = taNearby.copy()

        # Optionally remove the closest one from the nearby list
        # nearby_tabulation_areas.pop(taClosestIdx)
        #siteFileMap[locationKey]["nearby_tabulation_areas"] = nearby_tabulation_areas

        siteFileMap[locationKey]["acs_"+acsSurvey] =  str("%s%s" % ("./acs/", acsSurvey+"_uscbPlace.json"))

    with open(os.path.join(acs_dir, f"{acsSurvey}_uscbPlace.json"), "w", encoding='utf-8') as json_acsData:
        json_acsData.write(json.dumps(publicAcsGeoJson, indent=1, default=json_serialize, ensure_ascii=False))

with open(os.path.join(lib_dir, "siteFileMap.json"), "w", encoding='utf-8') as json_siteFileMap:
	json_siteFileMap.write(json.dumps(siteFileMap, indent=1, default=json_serialize,  ensure_ascii=False))

print("getAcsData.py: DONE")
######################################################################################