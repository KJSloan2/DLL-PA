'''
    Interpolate points along osm features. The points are used to measure distance
    from OSM features to 3Dep+Ls8 points. Points are written to tempOsm.db
'''
import os
import sys
import json
import math
from typing import List, Tuple
import sqlite3
import duckdb
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#from utils import  json_serialize, make_new_geojson_feature    
from spatial_utils import haversine, mpolygon_yeild_pts
from global_functions.utils import get_files
from global_functions.sqlite_utils import get_table_info
######################################################################################
Point = Tuple[float, float]
######################################################################################
# Make this process into a reusable function to use across the codebase
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
######################################################################################
DB_NAME = "runtime.db"
DB_TABLE = "site_info"

def str_to_coords(coord_str):
    coord_str = coord_str.strip("() ")
    coords = tuple(float(x.strip()) for x in coord_str.split(","))
    return coords

siteInfo = get_table_info(DB_NAME, DB_TABLE, ["NAME"])
siteName = siteInfo['NAME']
######################################################################################
def angle_between_points(p1: Point, p2: Point, degrees: bool = True) -> float:
    """
    Compute the angle of the vector from p1 to p2.

    Returns:
        Angle measured from the positive x-axis to the segment p1 -> p2.
        By default in degrees, range (-180, 180].
    """
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]

    angle_rad = math.atan2(dy, dx)  # handles all quadrants
    if degrees:
        return math.degrees(angle_rad)
    return angle_rad
######################################################################################
def interpolate_points(p1: Point, p2: Point, n_points: int) -> List[Point]:
    """
    Linearly interpolate n_points new points between p1 and p2.

    Args:
        p1: Start point (x1, y1)
        p2: End point (x2, y2)
        n_points: Number of *new* points to insert between p1 and p2

    Returns:
        List of n_points points, not including p1 or p2.
    """
    if n_points <= 0:
        return []

    x1, y1 = p1
    x2, y2 = p2

    # Step in x and y for each new point
    step_x = (x2 - x1) / (n_points + 1)
    step_y = (y2 - y1) / (n_points + 1)

    return [
        (x1 + step_x * i, y1 + step_y * i)
        for i in range(1, n_points + 1)
    ]
######################################################################################
conn = duckdb.connect('tempGeo.duckdb')
cursor = conn.cursor()
######################################################################################
dist_thresh = 10
output_pts = {"lon": [], "lat": [], "srf": []}

dir_path = os.path.join(parent_dir, "data", "osm")
files = get_files(dir_path)
######################################################################################
validOsmCategories = list(["highway", "building", "construction"])
for osmCategory in validOsmCategories:
    cursor.execute(f"DELETE FROM {osmCategory}_srf")
    conn.commit()
######################################################################################
for fName in files:
    if fName.endswith(".geojson"):
        parse_fName = fName.split("_")
        if parse_fName[0] == siteName:
            fPath = os.path.join(parent_dir, dir_path, fName)
            with open(fPath, 'r', encoding='utf-8') as f:
                geoJson = json.load(f)
            if "features" in geoJson:
                for feature in geoJson["features"]:
                    props = feature["properties"]
                    osmCategory = props.get("osm_category", "unknown")

                    if osmCategory not in validOsmCategories:
                        continue
                    
                    geom = feature["geometry"]
                    geom_type = geom["type"]

                    surface_value = f"{osmCategory}_unknown"
                    if "surface" in props:
                        surface_value = f"{osmCategory}_{props["surface"]}"

                    coordinates = []
                    if geom_type == "LineString":
                        coordinates = geom["coordinates"]
                    elif geom_type == "MultiLineString":
                        for line in geom["coordinates"]:
                            coordinates.extend(line)
                    elif geom_type == "Polygon":
                        for ring in geom["coordinates"]:
                            coordinates.extend(ring)
                    elif geom_type == "MultiPolygon":
                        for polygon in geom["coordinates"]:
                            for ring in polygon:
                                coordinates.extend(ring)
                    if len(coordinates) > 2:

                        for i in range(len(coordinates) - 1):
                            pt1 = coordinates[i]
                            pt2 = coordinates[i + 1]
                            lon1, lat1 = pt1[0], pt1[1]
                            lon2, lat2 = pt2[0], pt2[1]

                            dist_mtr = haversine([lon1, lat1], [lon2, lat2])["m"]
                            new_pts = []
                            if dist_mtr > dist_thresh:
                                n_points = int(dist_mtr / dist_thresh)
                                new_pts.append(interpolate_points(pt1, pt2, n_points))

                            for ptLst in new_pts:
                                for pt1 in ptLst:
                                    cursor.execute(
                                        f'''INSERT INTO {osmCategory+"_srf"} (lon, lat, srf) VALUES (?, ?, ?)''',
                                        (pt1[0], pt1[1], surface_value)
                                        )
                                    print(pt1[0], pt1[1], surface_value)
                    else:
                        print(len(coordinates), "coordinates, skipping feature")
conn.commit()
conn.close()

print("featurePtInterpolation.py complete")
######################################################################################