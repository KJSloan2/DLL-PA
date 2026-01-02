import os
import sqlite3

args = {
    "make_osm_srf_table": True,
    "make_composite_table": True,
    "make_spectral_table": True,
    "make_threeDep_table": True
}

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

if args["make_osm_srf_table"]:
    #"railway", "waterway", "aeroway", "powerline", "pipeline"
    osmFeatureTags = ["highway_srf", "building_srf", "construction_srf"]
    for tag in osmFeatureTags:
        create_table(tag, {
            'geoid': 'INTEGER PRIMARY KEY',
            'lat': 'FLOAT',
            'lon': 'FLOAT', 
            'srf': 'TEXT', 
            })

if args["make_composite_table"]:
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
        'lstf_ndvi_corr': 'FLOAT',
        'lstf_ndmi_corr': 'FLOAT', 
        'ndvi_ndmi_corr': 'FLOAT',
        'lstf_ndvi_pval': 'FLOAT', 
        'lstf_ndmi_pval': 'FLOAT', 
        'ndvi_ndmi_pval': 'FLOAT'
        })

if args["make_spectral_table"]:
    # Create table for 3Dep + LS8 composite data
    create_table('spectral_temporal', {
        'geoid': 'INTEGER PRIMARY KEY',
        'lat': 'FLOAT',
        'lon': 'FLOAT', 
        'lstf': 'FLOAT',
        'lstf_serc': 'FLOAT', 
        'lstf_arc': 'FLOAT',
        'ndvi': 'FLOAT', 
        'ndvi_serc': 'FLOAT', 
        'ndvi_arc': 'FLOAT', 
        'ndmi': 'FLOAT',
        'ndmi_serc': 'FLOAT',
        'ndmi_arc': 'FLOAT',
        'idx_row': 'INTEGER',
        'idx_col': 'INTEGER',
        'lstf_ndvi_corr': 'FLOAT',
        'lstf_ndmi_corr': 'FLOAT', 
        'ndvi_ndmi_corr': 'FLOAT',
        'lstf_ndvi_pval': 'FLOAT', 
        'lstf_ndmi_pval': 'FLOAT', 
        'ndvi_ndmi_pval': 'FLOAT'
        })

if args["make_threeDep_table"]:
    # Create table for 3Dep + LS8 composite data
    create_table('three_dep', {
        'rowId': 'INTEGER PRIMARY KEY',
        'idx_row': 'INTEGER',
        'idx_col': 'INTEGER',
        'lat': 'FLOAT',
        'lon': 'FLOAT',
        'elv_rel': 'FLOAT',
        'elv': 'FLOAT',
        'slope': 'FLOAT',
        })

    
conn.close()