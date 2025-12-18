import os
import sys
import sqlite3

conn = sqlite3.connect('ref.db')
cursor = conn.cursor()

dataToAdd = [
    {"DIR_NAME":"USGS_WSS", "DIR_PATH":"C:\\Users\\Kjslo\\Documents\\data\\USGS\\Web_Soil_Survey", "USEAGE":"Soil data reference"},
	{"DIR_NAME":"USCB_HYDROGRAPHY_TIGER", "DIR_PATH":"C:\\Users\\Kjslo\\Documents\\data\\USCB\\Tiger\\Hydrography", "USEAGE":"Body of water spatial reference"},
    {"DIR_NAME":"USCB_TIGER_TABULATION_US", "DIR_PATH":"C:\\Users\\Kjslo\\Documents\\data\\USCB\\Tiger\\Tabulation_US", "USEAGE":"Tabulation areas for entire US"}
]
for data in dataToAdd:
    print(data)
    
    # Add the INSERT statement
    cursor.execute(
        '''INSERT INTO dir_ref (DIR_NAME, DIR_PATH, USEAGE) VALUES (?, ?, ?)''',
        (data["DIR_NAME"], data["DIR_PATH"], data["USEAGE"])
    )

# Don't forget to commit the changes
conn.commit()
conn.close()