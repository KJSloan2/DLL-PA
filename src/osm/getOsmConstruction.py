'''
    Script to retrieve OSM construction and planned features using Overpy.
    This is a separate script due to the complexity of construction queries.
    Queries are divided into multiple parts to capture various tags related to construction
    without overwhelming the Overpass API.
'''
import json
import os
import time
from typing import Dict, List, Tuple
import overpy
######################################################################################
osmApi = overpy.Overpass()
Coord = Tuple[float, float]  # ( lon, lat)
######################################################################################
# Make this process into a reusable function to use across the codebase
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
ls8Bounds = logJson["ls8_bounds"]

ls8Bounds_key0 = next(iter(ls8Bounds))
ls8Bounds_item0 = ls8Bounds[ls8Bounds_key0]
ls8Bounds_item0_centroid = ls8Bounds_item0["centroid"]

searchRadius = 5

queryGeneralParams = {
    "construction": {
        "queries": [None],
        "geometry": ["Polygon", "Multipolygon"]
    },
}

radius = int(searchRadius * 1609.34)
osmGeneralQueryCategories = ["construction"]
queryKey = None

lat, lon = ls8Bounds_item0_centroid[1], ls8Bounds_item0_centroid[0]

osmGeneralQueryCategory = "construction"
filterGeometry = queryGeneralParams[osmGeneralQueryCategory]["geometry"]

# Build Overpass queries
query_a = f"""
    [out:json][timeout:180];
    (
    /* UNDER CONSTRUCTION */
    nwr["landuse"="construction"](around:{radius},{lat},{lon});
    nwr["construction"](around:{radius},{lat},{lon});
    nwr["building"="construction"](around:{radius},{lat},{lon});
    nwr["highway"="construction"](around:{radius},{lat},{lon});
    nwr["site"="construction"](around:{radius},{lat},{lon});
    );
    out body;
    >;
    out skel qt;
"""

query_b = f"""
    [out:json][timeout:180];
    (
    /* PLANNED / PROPOSED */
    nwr["proposed"](around:{radius},{lat},{lon});
    nwr["highway"="proposed"](around:{radius},{lat},{lon});
    nwr["building"="proposed"](around:{radius},{lat},{lon});
    nwr["proposed:highway"](around:{radius},{lat},{lon});
    nwr["proposed:building"](around:{radius},{lat},{lon});
    nwr["amenity"="proposed"](around:{radius},{lat},{lon});
    nwr["proposed:amenity"](around:{radius},{lat},{lon});
    );
    out body;
    >;
    out skel qt;
"""

query_c = f"""
    [out:json][timeout:180];
    (
    /* Optional early-stage sites */
    nwr["landuse"="brownfield"](around:{radius},{lat},{lon});
    nwr["landuse"="greenfield"](around:{radius},{lat},{lon});
    );
    out body;
    >;
    out skel qt;
"""
######################################################################################
queries = [query_a, query_b, query_c]
osmData_filtered = {
    "type": "FeatureCollection",
    "features": []
}
for query in queries:
    response = osmApi.query(query)
    def overpass_to_geojson(overpy_result):
        """
        Convert overpy Result object to GeoJSON format.
        Args:
            overpy_result (overpy.Result): nodes, ways, relations
        Returns:
            dict: GeoJSON FeatureCollection
        """
        features = []

        # Nodes -> Point features
        for node in overpy_result.nodes:
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(node.lon), float(node.lat)]
                },
                "properties": dict(node.tags) if node.tags else {}
            })

        # Ways -> LineString or Polygon
        for way in overpy_result.ways:
            try:
                coords = [
                    [float(node.lon), float(node.lat)]
                    for node in way.nodes
                ]

                if len(coords) < 2:
                    print(f"Way {way.id} has insufficient nodes, skipping")
                    continue

                geom_type = "LineString"
                coordinates = coords

                # Determine if Polygon
                if len(coords) >= 4 and coords[0] == coords[-1]:
                    geom_type = "Polygon"
                    coordinates = [coords]

                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": geom_type,
                        "coordinates": coordinates
                    },
                    "properties": dict(way.tags) if way.tags else {}
                })

            except Exception as e:
                print(f"Error processing way {way.id}: {e}")
                continue

        return {
            "type": "FeatureCollection",
            "features": features
        }
    ######################################################################################
    osmData_geojson = overpass_to_geojson(response)

    for feature in osmData_geojson["features"]:
        geometry_type = feature["geometry"]["type"]
        properties = feature.get("properties", {})

        # Keep matching geometry OR tags containing terminal/concourse
        '''if (
            geometry_type in filterGeometry or
            any(k in properties for k in ["terminal", "concourse"])
        ):'''

        if geometry_type in filterGeometry:
            osmData_filtered["features"].append(feature)

    if queryKey is not None:
        searchKey = f"{osmGeneralQueryCategory}-{queryKey}"
    else:
        searchKey = osmGeneralQueryCategory

outputFileName = (
    f"{locationKey}_{searchKey}_{str(searchRadius)}.geojson"
)
outputPath = os.path.join("data", outputFileName)

with open(outputPath, "w", encoding="utf-8") as output_json:
    output_json.write(
        json.dumps(osmData_filtered, ensure_ascii=False)
    )
######################################################################################
print("DONE")