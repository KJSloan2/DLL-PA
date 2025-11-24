import os
import time
import json
from collections import Counter
import numpy as np
import time

import sqlite3
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
######################################################################################
start_time = time.time()
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
gridRefGeoJson = json.load(open(os.path.join(parent_dir, 'resources', "healthcareFacilities_gridRef.geojson")))
######################################################################################

db_file = os.path.join(parent_dir, "data",  "healthcare_sites.db")
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

headers = [
    "object_id", "id", "fid", "name", "address", "city", "state", "zip", "type", "status", "county", 
    "countyfips", "naics_code", "source_dat", "val_method", "val_date", "owner", 
    "ttl_staff", "beds", "trauma", "date_creat", "helipad", "lat", "lon"
]

hospital_types = {
    "hosp_genacute": "General Acute Care Hospital", 
    "hosp_crit": "Critical Access", 
    "hosp_mil": "Military Hospital", 
    "hosp_rehab": "Rehabilitation Hospital", 
    "hosp_lt": "Long Term Care Hospital", 
    "hosp_psyc": "Psychiatric Hospital", 
    "hosp_child": "Children", 
    "hosp_spec": "Special Hospital", 
    "hosp_women": "Women", 
    "hosp_chronic": "Chronic Disease Hospital"
}

fc = {
    "type": "FeatureCollection",
    "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4269" } },
    "features": []
}

hospitalTypeRef = []
hospitalTypeKeyRef = []
for key, val in hospital_types.items():
    hospitalTypeRef.append(val)
    hospitalTypeKeyRef.append(key)

for feature in gridRefGeoJson["features"]:
    feature_id = feature["properties"]["id"]
    newFeature = {
        "type": "Feature",
        "geometry": feature["geometry"],
        "properties": {
            "id": feature_id,
            "helipads": 0,
            "trauma": 0,
            "hospital_objectids": []
        }
    }

    for hospTypeKey in hospitalTypeKeyRef:
        newFeature["properties"][hospTypeKey] = 0

    if "hospitals" in feature["properties"]:
        hospitals_objectIds = feature["properties"]["hospitals"]
        for objectId in hospitals_objectIds:
            cursor.execute("SELECT * FROM hospitals WHERE object_id = ?", (objectId,))
            row = cursor.fetchone()
            if row:
                hospType = row[headers.index("type")]
                if hospType in hospital_types.values():
                    newFeature["properties"][hospitalTypeKeyRef[hospitalTypeRef.index(hospType)]] += 1
                else:
                     print(f"Unknown hospital type: {hospType} for object_id: {objectId}")
                helipad = row[headers.index("helipad")]
                if helipad == "Y":
                    newFeature["properties"]["helipads"] += 1

                #trauma = row[headers.index("trauma")]
                #newFeature["properties"]["trauma"] += 1

    fc["features"].append(newFeature)

conn.close()

with open(os.path.join(parent_dir, "data", "healthcare", "healthcareFacilities_gridStats.geojson"), "w", encoding='utf-8') as geojson_gridRef:
	geojson_gridRef.write(json.dumps(fc, indent=1, default=json_serialize))