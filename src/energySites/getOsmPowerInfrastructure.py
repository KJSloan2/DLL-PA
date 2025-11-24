import requests
import json
import os
import subprocess

######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]

dataDirectory = os.path.join(parent_dir, 'data', 'landsat', locationKey)
outputDirectory = os.path.join(parent_dir, 'output', 'osm')
######################################################################################

coordinates = logJson["site_centroid"]
radius = 10000  # meters

# Query for power infrastructure
query = f"""
[out:json];
(
  way["power"="station"](around:{radius}, {coordinates[0]}, {coordinates[1]});
  way["power"="substation"](around:{radius}, {coordinates[0]}, {coordinates[1]});
  way["power"="line"](around:{radius}, {coordinates[0]}, {coordinates[1]});
  way["power"="generator"](around:{radius}, {coordinates[0]}, {coordinates[1]});
  way["power"="plant"](around:{radius}, {coordinates[0]}, {coordinates[1]});
  way["power"="transformer"](around:{radius}, {coordinates[0]}, {coordinates[1]});
);
out body;
>;
out skel qt;
"""

response = requests.post(
    "https://overpass-api.de/api/interpreter",
    data={"data": query}
)
osmData = response.json()

def overpass_to_geojson(overpass_data):
    elements = overpass_data['elements']
    nodes = {}
    features = []

    for el in elements:
        if el['type'] == 'node':
            nodes[el['id']] = [el['lon'], el['lat']]

    for el in elements:
        if el['type'] == 'node':
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [el["lon"], el["lat"]],
                },
                "properties": el.get("tags", {}),
            })

        elif el['type'] == 'way':
            try:
                coords = [nodes[node_id] for node_id in el['nodes']]
                geom_type = "LineString"

                if coords[0] == coords[-1] and len(coords) >= 4:
                    geom_type = "Polygon"
                    coords = [coords]

                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": geom_type,
                        "coordinates": coords,
                    },
                    "properties": el.get("tags", {}),
                })
            except KeyError:
                print(f"Missing node for way {el['id']}, skipping")

    return {
        "type": "FeatureCollection",
        "features": features,
    }

osmData_geojson = overpass_to_geojson(osmData)

osmData_geojson["features"] = [
    feature for feature in osmData_geojson["features"]
    if not (feature["geometry"]["type"] == "Point" and feature["properties"] == {})
]
outputFileName = locationKey+"_OSM_power.geojson"
outputPath = os.path.join(root_dir, "frontend", "public", "osm", outputFileName)

siteFileMap[locationKey]["power"] = "./osm/"+str(outputFileName)
with open(os.path.join(lib_dir, "siteFileMap.json"), "w", encoding='utf-8') as json_siteFileMap:
	json_siteFileMap.write(json.dumps(siteFileMap, indent=2, ensure_ascii=False))

print("Power infrastructure export complete.")