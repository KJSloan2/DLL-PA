import os
import sys
import numpy as np
import json
from shapely.geometry import shape, Polygon, MultiPolygon, Point
import sqlite3
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
def inside_tiger_feature(geojson_path, query_point):
    """
    Find the feature in a GeoJSON file that contains the given point.
    
    Args:
        geojson_path: Full path to the GeoJSON file
        query_point: Shapely Point object to query
        
    Returns:
        dict: Feature properties if found, None otherwise
    """
    with open(geojson_path, 'r') as f:
        geojson_data = json.load(f)
    
    features = geojson_data.get("features", [])
    
    for feature in features:
        geom = shape(feature["geometry"])
        
        is_inside = False
        if isinstance(geom, Polygon):
            if geom.covers(query_point):
                is_inside = True
        elif isinstance(geom, MultiPolygon):
            for poly in geom.geoms:
                if poly.covers(query_point):
                    is_inside = True
                    break
        
        if is_inside:
            return feature["properties"]
    
    return None
######################################################################################
DB_NAME = "runtime.db"
DB_TABLE = "site_info"
######################################################################################
siteName, aoi_bb_pt_sw, aoi_bb_pt_ne, aoi_centroid = None, None, None, None

siteInfo = get_table_info(DB_NAME, DB_TABLE, ["NAME", "AOI_BB_PT_SW", "AOI_BB_PT_NE", "AOI_CENTROID"])
siteName = siteInfo['NAME']
aoi_bb_pt_sw = siteInfo["AOI_BB_PT_SW"]
aoi_bb_pt_ne = siteInfo["AOI_BB_PT_NE"]
aoi_centroid = siteInfo["AOI_CENTROID"]
######################################################################################
'''print(f"Site Name: {siteName}"
      f"\nAOI BB PT SW: {aoi_bb_pt_sw}"
      f"\nAOI BB PT NE: {aoi_bb_pt_ne}"
      f"\nAOI CENTROID: {aoi_centroid}")'''
######################################################################################
query_pt = None
if aoi_centroid:
    coord_str = aoi_centroid.strip("() ")
    coords = tuple(float(x.strip()) for x in coord_str.split(","))
    lon, lat = coords
    query_pt = Point(lon, lat)
    #print(f"Query Point: {query_pt}")

if query_pt:
    conn = sqlite3.connect('runtime.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dir_lib WHERE DIR_NAME = ?", ("USCB_STATE_TIGER",))
    row = cursor.fetchone()
    column_names = [description[0] for description in cursor.description]
    if row:
        row_dict = dict(zip(column_names, row))
        tiger_dirPath = row_dict["DIR_PATH"]

    state_geoid, state_name, state_fips =None, None, None

    if tiger_dirPath:
        tiger_fName = "tl_2024_us_state.geojson"

        state_props = inside_tiger_feature(
            os.path.join(tiger_dirPath, "tl_2024_us_state.geojson"),
            query_pt)

        state_geoid = state_props["GEOID"]
        state_name = state_props["NAME"]
        state_fips = state_props["STATEFP"]

    if state_name and state_fips:

        # Locate county in state
        
        cursor.execute("SELECT * FROM dir_lib WHERE DIR_NAME = ?", ("USCB_COUNTY_TIGER",))
        row = cursor.fetchone()
        column_names = [description[0] for description in cursor.description]
        if row:
            row_dict = dict(zip(column_names, row))
            tiger_dirPath = row_dict["DIR_PATH"]

            tiger_fName = "tl_2025_us_county.geojson"
           
            countyFips, countyName, countyGeoid = None, None, None

            county_props = inside_tiger_feature(
                os.path.join(tiger_dirPath, tiger_fName),
                query_pt)

            countyGeoid = county_props["GEOID"]
            countyName = county_props["NAME"]
            countyFips = county_props["COUNTYFP"]

        cursor.execute(
            """UPDATE site_info
            SET STATE_NAME = ?, STATE_FIPS = ?, COUNTY_NAME = ?, COUNTY_FIPS = ?
            WHERE NAME = ?""",
            (state_name, state_fips, countyName, countyFips, siteName)
        )
        print(state_name, state_fips, countyName, countyFips, siteName)
    conn.commit()
    conn.close()