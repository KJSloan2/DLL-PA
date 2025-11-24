import json
from shapely.geometry import Polygon, shape, mapping
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

stateReferenceGeoJson = json.load(open(os.path.join(
    parent_dir, "backend", "resources", 
    "reference", "tl_2024_us_state.geojson")))

# Define input bounding box
pt1 = (49.152976603661806, -125.90663272457583)  # NW corner (lat, lon)
pt2 = (23.512322094438833, -67.15115495493373)   # SE corner (lat, lon)

# Convert 50 miles to degrees (approximate)
cell_size_miles = 25
miles_per_degree = 69.0
cell_size_deg = cell_size_miles / miles_per_degree  # ≈ 0.7246 degrees

# Calculate bounds
north = pt1[0]
south = pt2[0]
west = pt1[1]
east = pt2[1]

# Generate grid
features = []
row = 0
lat = north

# Iterate through stateReferenceGeoJson features and convert them to Shapely geometries
state_shapes = [shape(feature["geometry"]) for feature in stateReferenceGeoJson["features"]]

while lat - cell_size_deg > south:
    col = 0
    lon = west
    while lon + cell_size_deg < east:
        square = Polygon([
            (lon, lat),
            (lon + cell_size_deg, lat),
            (lon + cell_size_deg, lat - cell_size_deg),
            (lon, lat - cell_size_deg),
            (lon, lat)
        ])

        # Check if the square overlaps with any state geometry
        if any(square.intersects(state_shape) for state_shape in state_shapes):
            features.append({
                "type": "Feature",
                "properties": {
                    "id": f"{row}-{col}"
                },
                "geometry": mapping(square)
            })

        lon += cell_size_deg
        col += 1
    lat -= cell_size_deg
    row += 1

# Build FeatureCollection
geojson_output = {
    "type": "FeatureCollection",
    "features": features
}

# Write to file
output_path = os.path.join(parent_dir, "backend", "resources", "grid.geojson")
with open(output_path, "w") as f:
    json.dump(geojson_output, f, indent=1)

print(f"Created {len(features)} valid square features in 'grid.geojson'")
