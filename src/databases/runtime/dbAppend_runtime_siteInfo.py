import os
import sys
import json
import sqlite3

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationId = logJson["location_key"]

conn = sqlite3.connect('runtime.db')
cursor = conn.cursor()

TABLE_NAME = "site_info"
RESET_TABLE = True

if RESET_TABLE:
    cursor.execute(f"DELETE FROM {TABLE_NAME}")
    conn.commit()
    cursor = conn.cursor()

years = []
for year in logJson["ls8_bounds"]:
    years.append(int(year))
mostRecentYear = max(years) 
mostRecentYear = str(mostRecentYear)
bb = logJson["ls8_bounds"][mostRecentYear]["bb"]
bb_sw = bb[0]
bb_ne = bb[1]
bb_nw = [bb_sw[0], bb_ne[1]]
bb_se = [bb_ne[0], bb_sw[1]]

bb_sw = ",".join(list(map(str, bb_sw)))
bb_ne = ",".join(list(map(str, bb_ne)))
bb_nw = ",".join(list(map(str, bb_nw)))
bb_se = ",".join(list(map(str, bb_se)))

centroid = ",".join(list(map(str, logJson["ls8_bounds"][mostRecentYear]["centroid"])))
print(centroid)

dataToAdd = [
    {
        "NAME":locationId, 
        "STATE_NAME":"",
        "STATE_FIPS":"",
        "COUNTY_NAME":"",    
        "COUNTY_FIPS":"",
        "PLACE_NAME":"",
        "PLACE_GEOID":"",
        "HAS_HYDRO_FEATURES": "",
        "AOI_BB_PT_SW": str(bb_sw) , 
        "AOI_BB_PT_SE": str(bb_se),
        "AOI_BB_PT_NW": str(bb_nw),
        "AOI_BB_PT_NE": str(bb_ne),
        "AOI_CENTROID": str(centroid)
        },
]

for data in dataToAdd:
    print(data)
    
    # Add the INSERT statement
    cursor.execute(
        '''INSERT INTO site_info (
        NAME, STATE_NAME, STATE_FIPS, 
        COUNTY_NAME, COUNTY_FIPS, PLACE_NAME, 
        PLACE_GEOID, HAS_HYDRO_FEATURES,
        AOI_BB_PT_SW, AOI_BB_PT_SE, AOI_BB_PT_NW, AOI_BB_PT_NE, AOI_CENTROID) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (
            data["NAME"], data["STATE_NAME"], data["STATE_FIPS"], data["COUNTY_NAME"],
            data["COUNTY_FIPS"], data["PLACE_NAME"], data["PLACE_GEOID"], 
            data["HAS_HYDRO_FEATURES"],
            data["AOI_BB_PT_SW"], data["AOI_BB_PT_SE"], data["AOI_BB_PT_NW"], data["AOI_BB_PT_NE"], data["AOI_CENTROID"]
            )
    )

conn.commit()
conn.close()