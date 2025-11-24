import os
import json
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
idx_src = split_dir.index("src")
parent_dir = split_dir[idx_src-1]
######################################################################################
filePath = os.path.join(parent_dir, "output", "osm", "SGF_OSM_Highway-2500.json")
'''with open(filePath, 'r') as jsonFile:
    data = json.load(jsonFile)
    #version, generator, osm3s, elements
    for element in data["elements"]:
        print(element)'''

#32.77996937630524, -96.8087951892066
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


with open(filePath, 'r') as jsonFile:
    osmData = json.load(jsonFile)
    print(osmData)
    '''osmData_geojson = overpass_to_geojson(osmData)

    outputFileName = 'SGF_OSM_Highway-2500.geojson'
    outputPath = os.path.join(parent_dir, 'output', 'osm', outputFileName)
    with open(outputPath, "w", encoding='utf-8') as output_json:
        output_json.write(json.dumps(osmData_geojson, ensure_ascii=False))'''

print("DONE")