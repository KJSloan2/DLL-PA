import sqlite3
import json
import os

######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
######################################################################################
geojson_file = os.path.join(parent_dir,  "data", "healthcare", "us-hospitals.geojson")
db_file = os.path.join(parent_dir, "data",  "healthcare_sites.db")
######################################################################################

conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Create the hospitals table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS hospitals (
        object_id INTEGER PRIMARY KEY,   
        id TEXT,
        fid INTEGER,
        name TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        type TEXT,
        status TEXT,
        county TEXT,
        countyfips TEXT,
        naics_code TEXT,
        source_dat TEXT,
        val_method TEXT,
        val_date TEXT,
        owner TEXT,
        ttl_staff TEXT,
        beds TEXT,
        trauma TEXT,
        date_creat TEXT,
        helipad TEXT,
        lat FLOAT,
        lon FLOAT
    )
""")

def safe_get_geo_point(props, field):
    geo_point = props.get("geo_point")
    if geo_point and isinstance(geo_point, dict):
        return geo_point.get(field)
    return None

with open(geojson_file, 'r', encoding='utf-8') as file:
    data = json.load(file)

objectid = 0
for feature in data['features']:
    print(feature["properties"]["name"])
    properties = feature['properties']
    cursor.execute("""
    INSERT INTO hospitals (
        object_id, id, fid, name, address, city, state, zip, type, status, county, 
                   countyfips, naics_code, source_dat, val_method, val_date, owner, 
                   ttl_staff, beds, trauma, date_creat, helipad, lat, lon
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        objectid,
        properties.get("id"),
        properties.get("fid"),
        properties.get("name"),
        properties.get("address"),
        properties.get("city"),
        properties.get("state"),
        properties.get("zip"),
        properties.get("type"),
        properties.get("status"),
        properties.get("county"),
        properties.get("countyfips"),
        properties.get("naics_code"),
        properties.get("source_dat"),
        properties.get("val_method"),
        properties.get("val_date"),
        properties.get("owner"),
        properties.get("ttl_staff"),
        properties.get("beds"),
        properties.get("trauma"),
        properties.get("date_creat"),
        properties.get("helipad"),
        safe_get_geo_point(properties, "lat"),
        safe_get_geo_point(properties, "lon"),
    ))

    objectid+=1

'''properties.get("geo_point", {}).get("lat"),
properties.get("geo_point", {}).get("lon"),'''
# Commit changes and close the connection
conn.commit()
conn.close()

print("Data successfully inserted into hospitals table.")