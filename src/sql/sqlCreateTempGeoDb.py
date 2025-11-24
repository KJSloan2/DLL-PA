import os
import sqlite3

conn = sqlite3.connect('tempGeo.db')

def create_table(table_name, columns):
    cursor = conn.cursor()
    # Create a dynamic SQL query to create a table
    column_defs = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})"
    # Execute the query
    cursor.execute(query)
    # Commit and close
    conn.commit()
    print(table_name)

#"railway", "waterway", "aeroway", "powerline", "pipeline"
'''osmFeatureTags = ["highway_srf", "building_srf", "construction_srf"]
for tag in osmFeatureTags:
    create_table(tag, {
        'geoid': 'INTEGER PRIMARY KEY',
        'lat': 'FLOAT',
        'lon': 'FLOAT', 
        'surface': 'FLOAT', 
        })'''

# Create table for 3Dep + LS8 composite data
create_table('terrainComposite', {
    'geoid': 'INTEGER PRIMARY KEY',
    'lat': 'FLOAT',
    'lon': 'FLOAT', 
    'lstf': 'FLOAT',
    'lstf_serc': 'FLOAT', 
    'lstf_arc': 'FLOAT',
    'lstf_flag': 'TEXT', 
    'ndvi': 'FLOAT', 
    'ndvi_serc': 'FLOAT', 
    'ndvi_arc': 'FLOAT', 
    'ndvi_flag': 'TEXT', 
    'ndmi': 'FLOAT',
    'ndmi_serc': 'FLOAT',
    'ndmi_arc': 'FLOAT',
    'ndmi_flag': 'TEXT',
    'elv_rel': 'FLOAT',
    'elv': 'FLOAT',
    'idx_row': 'INTEGER',
    'idx_col': 'INTEGER',
    "mdrdasp": 'FLOAT',
    "mdrdconc": 'FLOAT',
    "mdrdgrv": 'FLOAT',
    "mdrdunp": 'FLOAT',
    "mdconst": 'FLOAT',
    "mdbldg": 'FLOAT',
    })

conn.close()