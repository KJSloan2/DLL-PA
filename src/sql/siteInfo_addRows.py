import os
import sys
import sqlite3

conn = sqlite3.connect('ref.db')
cursor = conn.cursor()

dataToAdd = [
    {
        "ROW_ID":"1", 
        "NAME":"MildredLakeWI", 
        "COORDS":"(-89.53337756915289, 45.67704149350317)",
        "STATE_NAME":"",
        "STATE_FIPS":"",
        "COUNTY_NAME":"",    
        "COUNTY_FIPS":"",
        "PLACE_NAME":"",
        "PLACE_GEOID":"",
        "HAS_HYDRO_FEATURES": ""
        },
]

for data in dataToAdd:
    print(data)
    
    # Add the INSERT statement
    cursor.execute(
        '''INSERT INTO site_info (
        ROW_ID, NAME, COORDS, STATE_NAME, STATE_FIPS, 
        COUNTY_NAME, COUNTY_FIPS, PLACE_NAME, 
        PLACE_GEOID, HAS_HYDRO_FEATURES) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (
            data["ROW_ID"], data["NAME"], data["COORDS"],
            data["STATE_NAME"], data["STATE_FIPS"], data["COUNTY_NAME"],
            data["COUNTY_FIPS"], data["PLACE_NAME"], data["PLACE_GEOID"], 
            data["HAS_HYDRO_FEATURES"]
            )
    )

conn.commit()
conn.close()