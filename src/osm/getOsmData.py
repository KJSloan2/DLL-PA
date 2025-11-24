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
#publicTerrain_dir = os.path.join(root_dir, "frontend", "public", "landsat")

'''building	house, school, commercial, yes, etc.
amenity	school, hospital, restaurant, toilets
highway	residential, primary, footway, bus_stop
landuse	residential, commercial, industrial
leisure	park, swimming_pool, stadium
shop	supermarket, bakery, clothes
natural	tree, water, peak, beach
tourism	hotel, museum, camp_site
railway	station, tram_stop, rail
man_made	tower, bridge, storage_tank
waterway	river, stream, canal
place	city, village, hamlet, suburb
boundary	administrative, national_park
power	substation, generator, line'''

#coordinates = logJson["site_centroid"]
coordinates = logJson["ls8_bounds"]["2014"]["centroid"]
print(coordinates)

apis = ["highway", "building"]
api = apis[1]
radius = 10000

query = f"""
[out:json];
(
  way["{api}"](around:{radius}, {coordinates[0]}, {coordinates[1]});
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

'''outputFileName = 'SGF_OSM_Highway-2500.json'
outputPath = os.path.join(parent_dir, 'output', 'osm', outputFileName)
with open(outputPath, "w", encoding='utf-8') as output_json:
	output_json.write(json.dumps(data, ensure_ascii=False))'''
	
def overpass_to_geojson(overpass_data):
    elements = overpass_data['elements']
    nodes = {}
    features = []

    # Step 1: Build a node lookup table
    for el in elements:
        if el['type'] == 'node':
            nodes[el['id']] = [el['lon'], el['lat']]

    # Step 2: Convert each element to GeoJSON feature
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

                # Close the polygon if first = last
                if coords[0] == coords[-1] and len(coords) >= 4:
                    geom_type = "Polygon"
                    coords = [coords]  # Wrap in extra array for Polygon format

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

outputFileName = locationKey+'_OSM_'+api+'.geojson'
outputPath = os.path.join(parent_dir, 'data', 'osm', outputFileName)
with open(outputPath, "w", encoding='utf-8') as output_json:
    output_json.write(json.dumps(osmData_geojson, ensure_ascii=False))

try:
    print("Starting cleanOsmBuildings...")
    script_path = os.path.join(script_dir, "cleanOsmBuildings.py")
    subprocess.run(["python", script_path], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running cleanOsmBuildings.py: {e}")
except Exception as e:
    print(f"Unexpected error running cleanOsmBuildings.py: {e}")
print("DONE")