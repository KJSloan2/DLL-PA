from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
import geopandas as gpd
import json
import fiona
import os
from collections import Counter
import numpy as np
import subprocess

######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
siteCoordinates = logJson["site_centroid"]

weatherFilterRegionsJson = os.path.join(parent_dir, "resources", "grid.geojson")
######################################################################################
site_point = Point(siteCoordinates[1], siteCoordinates[0])  # Note: Point expects (x, y) -> (lon, lat)
#    35.75559833804512, -81.90614181780768
with open(weatherFilterRegionsJson, "r") as file:
    weather_filter_regions = json.load(file)

featureId_inside = None
for feature in weather_filter_regions["features"]:
    feature_shape = shape(feature["geometry"])
    if feature_shape.contains(site_point):
        featureId_inside = feature["properties"]["id"]
        break

if featureId_inside:
    print(featureId_inside)
    #print(json.dumps(featureId_inside, indent=4))
else:
    print("No feature contains the site point.")

logJson["weather_filter_region"] = featureId_inside
with open(os.path.join(parent_dir, 'resources', "log.json"), "w", encoding='utf-8') as json_log:
	json_log.write(json.dumps(logJson, indent=2, ensure_ascii=False))
     
try:
    print("Starting Extreme Weather Site Analysis...")
    script_path = os.path.join(parent_dir, "extreme_weather", "extremeWeatherSiteAnalysis.py")
    subprocess.run(["python", script_path], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running extremeWeatherSiteAnalysis.py: {e}")
except Exception as e:
    print(f"Unexpected error running extremeWeatherSiteAnalysis.py: {e}")
print("DONE")

