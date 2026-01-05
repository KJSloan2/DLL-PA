import os
import sys
import sqlite3

conn = sqlite3.connect('runtime.db')
cursor = conn.cursor()

TABLE_NAME = 'dir_lib'
CLEAR_TABLE = True
if CLEAR_TABLE:
    cursor.execute(f"DELETE FROM {TABLE_NAME}")

dataToAdd = [
    {
        "DIR_NAME":"USGS_WSS", 
        "DIR_PATH":"C:\\Users\\Kjslo\\Documents\\data\\USGS\\Web_Soil_Survey", 
        "USEAGE":"Soil data reference"
        },
	{
        "DIR_NAME":"USCB_HYDROGRAPHY_TIGER", 
        "DIR_PATH":"C:\\Users\\Kjslo\\Documents\\data\\USCB\\Tiger\\Hydrography", 
        "USEAGE":"Body of water spatial reference"
    },
    {
        "DIR_NAME":"USCB_TIGER_TABULATION_US", 
        "DIR_PATH":"C:\\Users\\Kjslo\\Documents\\data\\USCB\\Tiger\\Tabulation_US", 
        "USEAGE":"Tabulation areas for entire US"
    },
    {
        "DIR_NAME":"USDA_NASS_CDL_2024_30m", 
        "DIR_PATH":"C:\\Users\\Kjslo\\Documents\\data\\USDA\\2024_30m_cdls", 
        "USEAGE":"USDA Cropland Data Layer 2024 30m resolution"
    }
]

for data in dataToAdd:
    print(data)
    
    # Add the INSERT statement
    cursor.execute(
        f'''INSERT INTO {TABLE_NAME} (DIR_NAME, DIR_PATH, USEAGE) VALUES (?, ?, ?)''',
        (data["DIR_NAME"], data["DIR_PATH"], data["USEAGE"])
    )

# Don't forget to commit the changes
conn.commit()
conn.close()