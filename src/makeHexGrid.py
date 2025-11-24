import json
import math
from shapely.geometry import Polygon, shape, mapping
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

MILES_PER_DEGREE = 69.0
CELL_SIZE_MILES = 50
SIDE_DEG = CELL_SIZE_MILES / MILES_PER_DEGREE  # ≈ 0.7246 degrees

# Derived hexagon dimensions for pointy-topped layout
HEX_WIDTH = math.sqrt(3) * SIDE_DEG
HEX_HEIGHT = 2 * SIDE_DEG
HORIZONTAL_SPACING = HEX_WIDTH
VERTICAL_SPACING = 1.5 * SIDE_DEG

# Bounding box over the CONUS
pt1 = (49.152976603661806, -125.90663272457583)  # NW corner (lat, lon)
pt2 = (23.512322094438833, -67.15115495493373)   # SE corner (lat, lon)
north, south = pt1[0], pt2[0]
west, east = pt1[1], pt2[1]

# === PATH SETUP ===
# Set this to your actual project root

reference_path = os.path.join(parent_dir, "backend", "resources", "reference", "tl_2024_us_state.geojson")
output_path = os.path.join(parent_dir, "backend", "resources", "grid.geojson")

# === LOAD REFERENCE STATE SHAPES ===
with open(reference_path) as f:
    state_geojson = json.load(f)
state_shapes = [shape(feature["geometry"]) for feature in state_geojson["features"]]

# === HEXAGON GENERATION FUNCTION ===
def create_pointy_hexagon(cx, cy, size):
    return Polygon([
        (
            cx + size * math.cos(math.radians(angle)),
            cy + size * math.sin(math.radians(angle))
        )
        for angle in [30, 90, 150, 210, 270, 330, 30]  # closed ring
    ])

# === GRID GENERATION ===
features = []
row = 0
cy = north
while cy - SIDE_DEG > south:
    offset = HORIZONTAL_SPACING / 2 if row % 2 == 1 else 0
    cx = west + offset
    col = 0
    while cx + HEX_WIDTH / 2 < east:
        hexagon = create_pointy_hexagon(cx, cy, SIDE_DEG)
        if any(hexagon.intersects(state) for state in state_shapes):
            features.append({
                "type": "Feature",
                "properties": {
                    "id": f"{row}-{col}"
                },
                "geometry": mapping(hexagon)
            })
        cx += HORIZONTAL_SPACING
        col += 1
    cy -= VERTICAL_SPACING
    row += 1

# === EXPORT TO GEOJSON ===
geojson_output = {
    "type": "FeatureCollection",
    "features": features
}

with open(output_path, "w") as f:
    json.dump(geojson_output, f, indent=1)

print(f"✅ Created {len(features)} hexagon features in '{output_path}'")