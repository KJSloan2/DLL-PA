import os
import sys
import numpy as np
import json
from shapely.geometry import MultiLineString, Polygon, Point, mapping
import sqlite3

from shapely.geometry import shape, Point, Polygon, MultiPolygon
from shapely.strtree import STRtree
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.sqlite_utils import get_table_info
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
######################################################################################
######################################################################################
DB_NAME = "runtime.db"
DB_TABLE = "site_info"

siteName, aoi_bb_pt_sw, aoi_bb_pt_ne, aoi_centroid = None, None, None, None

siteInfo = get_table_info(DB_NAME, DB_TABLE, ["NAME", "AOI_BB_PT_SW", "AOI_BB_PT_NE", "AOI_CENTROID"])
siteName = siteInfo['NAME']
aoi_bb_pt_sw = siteInfo["AOI_BB_PT_SW"]
aoi_bb_pt_ne = siteInfo["AOI_BB_PT_NE"]
aoi_centroid = siteInfo["AOI_CENTROID"]

print(f"Site Name: {siteName}"
      f"\nAOI BB PT SW: {aoi_bb_pt_sw}"
      f"\nAOI BB PT NE: {aoi_bb_pt_ne}"
      f"\nAOI CENTROID: {aoi_centroid}")

query_pt = None
if aoi_centroid:
    coord_str = aoi_centroid.strip("() ")
    coords = tuple(float(x.strip()) for x in coord_str.split(","))
    lon, lat = coords
    query_pt = Point(lon, lat)
    print(f"Query Point: {query_pt}")

if query_pt:
    conn = sqlite3.connect('runtime.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dir_lib WHERE DIR_NAME = ?", ("USCB_STATE_TIGER",))
    row = cursor.fetchone()
    column_names = [description[0] for description in cursor.description]
    if row:
        row_dict = dict(zip(column_names, row))
        tiger_dirPath = row_dict["DIR_PATH"]
        print(tiger_dirPath)

    if tiger_dirPath:
        tiger_fName = "tl_2024_us_state.geojson"
        tiger_geoJson = json.load(open(os.path.join(tiger_dirPath, tiger_fName)))
        fc = tiger_geoJson["features"]
        for i, feature in enumerate(fc):
            state_geoid = feature["properties"]["GEOID"]
            state_name = feature["properties"]["NAME"]
            state_fips = feature["properties"]["STATEFP"]
            state_geom = shape(feature["geometry"])
            
            inside_state = False
            if isinstance(state_geom, Polygon):
                if state_geom.covers(query_pt):
                    print(f"Query point is inside Polygon: GEOID={state_geoid}, NAME={state_name}")
                    inside_state = True
            elif isinstance(state_geom, MultiPolygon):
                for poly in state_geom.geoms:
                    if poly.covers(query_pt):
                        print(f"Query point is inside MultiPolygon: GEOID={state_geoid}, NAME={state_name}")
                        inside_state = True
                        break

            if inside_state:
                break
    if state_name and state_fips:
        print(state_name, state_fips)

        cursor.execute(
            """UPDATE site_info
            SET STATE_NAME = ?, STATE_FIPS = ?
            WHERE NAME = ?""",
            (state_name, state_fips, siteName)
        )
    conn.commit()
    conn.close()