import os
import sys
import csv
import json
import duckdb
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from global_functions.utils import hex_to_rgb
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
RESET_TABLE = True
DIR_PATH = r"C:\Users\Kjslo\Documents\data\USDA\2024_30m_cdls"
RASTER_FNAME = "USDA_NASS_CDL-landCoverClassifications.csv"
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
csv_path = os.path.join(DIR_PATH, RASTER_FNAME)
csv_file = open(csv_path, mode='r', newline='', encoding='utf-8-sig')
csv_reader = csv.DictReader(csv_file)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
DB_NAME = 'usda_nass_cdl.duckdb'
TABLE_NAME = 'land_cover_classification_ref'
conn = duckdb.connect(DB_NAME)
cursor = conn.cursor()
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# If RESET_TABLE is true, delete all existing records in the table
if RESET_TABLE:
    cursor.execute(f"DELETE FROM {TABLE_NAME}")
    conn.commit()
    cursor = conn.cursor()
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
jsonLegend = []
for row in csv_reader:
    lc_int = int(row["VAL"])
    lc_hex = row["HEX"]
    lc = row["LAND_COVER"]
    rgb = hex_to_rgb(row["HEX"],scale255=False)
    r = int(rgb[0])
    g = int(rgb[1])
    b = int(rgb[2])
    print(lc_int, r, g, b, lc_hex, lc)
    cursor.execute(
        f'''INSERT INTO {TABLE_NAME} (VAL, R, G, B, HEX_COLOR, LAND_COVER) VALUES (?, ?, ?, ?, ?, ?)''',
        (lc_int, r, g, b, lc_hex, lc)
    )
    jsonLegend.append({
        "lc_val": lc_int,
        "lc_label": lc,
        "hex_color": lc_hex,
        "r": r,
        "g": g,
        "b": b
    })
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
conn.commit()
conn.close()
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
json_path = os.path.join("data", "cdl", "cropland_legend.json")
with open(json_path, mode='w', encoding='utf-8') as json_file:
    json.dump(jsonLegend, json_file, indent=2)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
print(f"{DB_NAME} - {TABLE_NAME} populated")
