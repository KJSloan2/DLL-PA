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
######################################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#from utils import  json_serialize, make_new_geojson_feature    
from spatial_utils import haversine, mpolygon_yeild_pts
######################################################################################
Point = Tuple[float, float]
######################################################################################
# Make this process into a reusable function to use across the codebase
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
ls8Bounds = logJson["ls8_bounds"]
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
conn = sqlite3.connect('tempGeo.db')
cursor = conn.cursor()
######################################################################################
dist_thresh = 10
output_pts = {"lon": [], "lat": [], "srf": []}

for osmCategory in ["highway", "building", "construction"]:

    data_fName = f"{locationKey}_{osmCategory}.geojson"
    data_path = os.path.join(parent_dir, "data", "osm", osmCategory, data_fName)

    cursor.execute(f"DELETE FROM {osmCategory}_srf")
    conn.commit()

    with open(data_path, "r", encoding="utf-8") as f:
        data_json = json.load(f)

    for feature in data_json["features"]:
        geometry = feature["geometry"]
        geometry_type = geometry["type"]

        properties = feature["properties"]
        if "surface" in properties:
            surface_value = properties["surface"]
        else:
            surface_value = "unknown"

        if geometry_type == "LineString":
            coordinates = geometry["coordinates"]

            for i in range(len(coordinates) - 1):
                pt1 = coordinates[i]
                pt2 = coordinates[i + 1]
                lon1, lat1 = pt1[0], pt1[1]
                lon2, lat2 = pt2[0], pt2[1]

                dist_mtr = haversine([lon1, lat1], [lon2, lat2])["m"]

                if dist_mtr > dist_thresh:
                    n_points = int(dist_mtr / dist_thresh)
                    new_pts = interpolate_points(pt1, pt2, n_points)

                    for pt in new_pts:
                        cursor.execute(
                            f'''INSERT INTO {osmCategory+"_srf"} (lon, lat, srf) VALUES (?, ?, ?)''',
                            (pt[0], pt[1], surface_value)
                        )
                cursor.execute(
                    f'''INSERT INTO {osmCategory+"_srf"} (lon, lat, srf) VALUES (?, ?, ?)''',
                    (pt1[0], pt1[1], surface_value)
                    )
                
conn.commit()
conn.close()
######################################################################################