import os
import sys
import math
import csv
import sqlite3
import matplotlib.pyplot as plt
import numpy as np
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
VIZUALIZE = False
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
#query = f"SELECT * FROM land_cover_classification_ref"
#cursor.execute(query)
#rows = cursor.fetchall()
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from raster_utils import (
    read_tiff,
    get_tiff_dimensions, 
    get_tiff_bounds,
    make_tiff_bb
)
from spatial_utils import (transform_coords)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
conn_runtime = sqlite3.connect('runtime.db')
cursor_runtime = conn_runtime.cursor() 
query_runtime = "SELECT * FROM site_info"
cursor_runtime.execute(query_runtime)
site_info = cursor_runtime.fetchone()
siteInfo_headers = [description[0] for description in cursor_runtime.description]

siteInfoDict = dict(zip(siteInfo_headers, site_info))
locationId = siteInfoDict['NAME']

aoi_bb_pt_sw = eval(siteInfoDict['AOI_BB_PT_SW'])
aoi_bb_pt_ne = eval(siteInfoDict['AOI_BB_PT_NE'])
aoi_centroid = eval(siteInfoDict['AOI_CENTROID'])
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

conn_runtime = sqlite3.connect('runtime.db')
cursor_runtime = conn_runtime.cursor() 
query_runtime = "SELECT * FROM site_info"
cursor_runtime.execute(query_runtime)
site_info = cursor_runtime.fetchone()
print(site_info)

conn = sqlite3.connect('usda_nass_cdl.db')
cursor = conn.cursor()

data_fName = "2024_30m_cdls.tif"
data_path = os.path.join(r"C:\Users\Kjslo\Documents\data\USDA\2024_30m_cdls", data_fName)

src = read_tiff(data_path)
tiffDimensions = get_tiff_dimensions(src)
tiffBounds = get_tiff_bounds(src)
print("TIFF Dimensions:", tiffDimensions)
print("TIFF Bounds:", tiffBounds)

#TIFF Bounds: left=-2356095.0, bottom=276915.0, right=2258235.0, top=3172605.0
rasterBBObj = make_tiff_bb(tiffBounds)

raster_bb_pts = rasterBBObj["bb"]
raster_bb_width = rasterBBObj["w"]
raster_bb_height = rasterBBObj["h"]

coordsTransformed = transform_coords('epsg:5070', 'epsg:4326', raster_bb_pts)
raster_bb_pt_sw = coordsTransformed[0]
raster_bb_pt_ne = coordsTransformed[1]

def compute_pixel_offset(target_bb_pt, raster_bb_pt):
    dlon = target_bb_pt[0] - raster_bb_pt[0]
    dlat = target_bb_pt[1] - raster_bb_pt[1]
    lat_ref = math.radians((target_bb_pt[1] + raster_bb_pt[1]) / 2.0)
    m_per_deg_lat = 111320.0
    m_per_deg_lon = 111320.0 * math.cos(lat_ref)

    dx_m = dlon * m_per_deg_lon
    dy_m = dlat * m_per_deg_lat

    dx_pixels = int(dx_m / 30.0)
    dy_pixels = int(dy_m / 30.0)

    return (dx_pixels, dy_pixels)

targetIdxs_start = compute_pixel_offset(aoi_bb_pt_sw, raster_bb_pt_sw)
targetIdxs_end = compute_pixel_offset(aoi_bb_pt_ne, raster_bb_pt_sw)

print(targetIdxs_start, targetIdxs_end)
band = src.read(1)
band_window = band[
    targetIdxs_start[1]:targetIdxs_end[1],
    targetIdxs_start[0]:targetIdxs_end[0]
]
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
def degrees_per_meter(lat_ref):
    """
    Calculate degrees per meter at a given latitude.
    
    Args:
        lat_ref: Reference latitude in degrees
        
    Returns:
        tuple: (deg_per_m_lon, deg_per_m_lat)
    """
    lat_rad = math.radians(lat_ref)
    m_per_deg_lat = 111320.0
    m_per_deg_lon = 111320.0 * math.cos(lat_rad)
    
    deg_per_m_lat = 1.0 / m_per_deg_lat
    deg_per_m_lon = 1.0 / m_per_deg_lon
    
    return (deg_per_m_lon, deg_per_m_lat)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
def calc_pixel_size_deg(pixel_size_m, bb_pts, ):
    lat_center = (bb_pts[0][1] + bb_pts[1][1]) / 2.0
    deg_per_m_lon, deg_per_m_lat = degrees_per_meter(lat_center)

    pxl_size_deg_lon = pixel_size_m * deg_per_m_lon
    pxl_size_deg_lat = pixel_size_m * deg_per_m_lat

    return pxl_size_deg_lon, pxl_size_deg_lat
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
def get_land_cover_info(cursor, val):
    """
    Get land cover classification and hex color for a given CDL value.
    
    Args:
        cursor: SQLite cursor object
        val: The CDL value to lookup
        
    Returns:
        tuple: (hex_color, land_cover) or (None, None) if not found
    """
    query = "SELECT R, G, B, LAND_COVER FROM land_cover_classification_ref WHERE VAL = ?"
    cursor.execute(query, (int(val),))
    result = cursor.fetchone()
    
    if result:
        #print(result[0], result[1], result[2], result[3])
        return result[0], result[1], result[2], result[3]   # (RGB, land_cover)
    else:
        return None, None, None, None
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
cursor.execute("DELETE FROM cdl_data")
conn.commit()



dataToViz = {"VAL": [], "RGB": [], "LAND_COVER": []}
rgb_array = np.zeros((band_window.shape[0], band_window.shape[1], 3))

psd_lon, psd_lat = calc_pixel_size_deg(30.0, [aoi_bb_pt_sw, aoi_bb_pt_ne])

rasterCount = 0
catalog = {}
catalogRef = []
dataOut = {"lat": [], "lon": [], "val": [], "lc":[]}
cy = aoi_bb_pt_sw[1]
for i, row in enumerate(band_window):
    cx = aoi_bb_pt_sw[0]
    for j, val in enumerate(row):
        if not np.isnan(val):
            r, g, b, land_cover = get_land_cover_info(cursor, val)
            rgb_array[i, j] = (r, g, b)
            if val not in catalogRef:
                catalogRef.append(val)
                catalog[val] = {"lc": land_cover, "count": 1}
            else:
                catalog[val]["count"] += 1

            pixel_lon = cx + (psd_lon / 2.0)
            pixel_lat = cy + (psd_lat / 2.0)

            dataOut["lat"].append(pixel_lat)
            dataOut["lon"].append(pixel_lon)
            dataOut["val"].append(val)
            dataOut["lc"].append(land_cover) 
            rasterId = f"{i}-{j}"
 
            cursor.execute(
                "INSERT INTO cdl_data (RASTER_ID, LAT, LON, LC_VAL, LC_LABEL) VALUES (?, ?, ?, ?, ?)",
                (rasterId, pixel_lat, pixel_lon, int(val), land_cover)
            )

            rasterCount+=1
        cx += psd_lon
    cy += psd_lat
conn.commit()

'''for key, obj in catalog.items():
    lc = obj["lc"]
    count = obj["count"]
    prctTotal = (count / rasterCount) * 100
    print(key, lc, prctTotal)

if VIZUALIZE:
    # Visualize
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Original values
    ax1.imshow(band_window, cmap='tab20')
    ax1.set_title("CDL Values (Raw)")
    ax1.axis('off')

    # Colored by land cover classification
    ax2.imshow(rgb_array)
    ax2.set_title("Land Cover Classification (CDL Colors)")
    ax2.axis('off')

    plt.tight_layout()
    plt.show()

    # Optional: Show legend of unique land cover types
    unique_vals = np.unique(band_window[~np.isnan(band_window)])
    print("\nLand Cover Types in Window:")
    for val in unique_vals:
        r, g, b, land_cover = get_land_cover_info(cursor, int(val))
        print(f"  {int(val)}: {land_cover} (R:{r}, G:{g}, B:{b})")'''