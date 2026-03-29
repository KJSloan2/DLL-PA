import os
import sys
import csv
import json
import duckdb
import sqlite3
##################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from global_functions.utils import hex_to_rgb, get_workspace_paths
##################################################################################
workspace_paths = get_workspace_paths()
workspace_path = workspace_paths["parent"]
print(workspace_paths)
conn_runtime = sqlite3.connect(os.path.join(workspace_path, "runtime.db"))
cursor_runtime = conn_runtime.cursor()
query = "SELECT * FROM dir_lib WHERE DIR_NAME = ?"
cursor_runtime.execute(query, ("USDA_NASS_CDL_2024_30m",))

target_row = cursor_runtime.fetchone()
if target_row:
    headers = [description[0] for description in cursor_runtime.description]
    target_data = dict(zip(headers, target_row))
    target_path = target_data["DIR_PATH"]
else:
    target_path = None

##################################################################################
RESET_TABLE = False
CLASSIFICATIONS_CSV_FNAME = "USDA_NASS_CDL-landCoverClassifications.csv"
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
if target_path:
    csv_path = os.path.join(target_path, CLASSIFICATIONS_CSV_FNAME)
    csv_file = open(csv_path, mode='r', newline='', encoding='utf-8-sig')
    csv_reader = csv.DictReader(csv_file)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    DB_NAME = 'usda_nass_cdl.duckdb'
    TABLE_NAME = 'classification_ref'
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
else:
    print("Target path for USDA CDL not found in runtime dir_lib.")
