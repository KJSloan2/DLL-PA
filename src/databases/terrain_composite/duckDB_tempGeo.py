import os
import sys
import sqlite3

from duckdb import query
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#from sqlite_utils import create_duckDb_table
######################################################################################
def create_duckDb_table(conn, table_name, columns, drop=False):
    """
    Works with both SQLite and DuckDB connections
    """
    # Check if DuckDB connection
    is_duckdb = hasattr(conn, 'execute') and 'duckdb' in str(type(conn))
    
    # Handle DROP
    if drop:
        if is_duckdb:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        else:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
    
    # Build column definitions with type conversion
    col_defs = []
    for col_name, col_type in columns.items():
        # Convert SQLite types to DuckDB types if needed
        if is_duckdb:
            if col_type == 'FLOAT':
                col_type = 'DOUBLE'
            elif col_type == 'TEXT':
                col_type = 'VARCHAR'
        col_defs.append(f"{col_name} {col_type}")
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
    
    # Execute CREATE TABLE
    if is_duckdb:
        conn.execute(create_sql)
        conn.commit()
    else:
        cursor = conn.cursor()
        cursor.execute(create_sql)
        conn.commit()
######################################################################################
DROP = True
args = {
    "make_osm_srf_table": False,
    "make_composite_table": True,
    "make_spectral_table": False,
    "make_threeDep_table": False
}

conn = sqlite3.connect('tempGeo.db')

if args["make_osm_srf_table"] == True:
    #"railway", "waterway", "aeroway", "powerline", "pipeline"
    osmFeatureTags = ["highway_srf", "building_srf", "construction_srf"]
    for tag in osmFeatureTags:
        create_duckDb_table(conn, tag, {
            'geoid': 'INTEGER PRIMARY KEY',
            'lat': 'DOUBLE',
            'lon': 'DOUBLE', 
            'srf': 'VARCHAR', 
            }, DROP)

if args["make_composite_table"] == True:
    # Create table for 3Dep + LS8 composite data
    create_duckDb_table(conn, 'terrain_composite', {
        'geoid': 'INTEGER PRIMARY KEY',
        'lat': 'DOUBLE',
        'lon': 'DOUBLE', 
        'lstf': 'DOUBLE',
        'lstf_serc': 'DOUBLE', 
        'lstf_arc': 'DOUBLE',
        'lstf_flag': 'VARCHAR', 
        'ndvi': 'DOUBLE', 
        'ndvi_serc': 'DOUBLE', 
        'ndvi_arc': 'DOUBLE', 
        'ndvi_flag': 'VARCHAR', 
        'ndmi': 'DOUBLE',
        'ndmi_serc': 'DOUBLE',
        'ndmi_arc': 'DOUBLE',
        'ndmi_flag': 'VARCHAR',
        'elv_rel': 'DOUBLE',
        'elv': 'DOUBLE',
        'idx_row': 'INTEGER',
        'idx_col': 'INTEGER',
        "mdrdasp": 'DOUBLE',
        "mdrdconc": 'DOUBLE',
        "mdrdgrv": 'DOUBLE',
        "mdrdunp": 'DOUBLE',
        "mdconst": 'DOUBLE',
        "mdbldg": 'DOUBLE',
        'lstf_ndvi_corr': 'DOUBLE',
        'lstf_ndmi_corr': 'DOUBLE', 
        'ndvi_ndmi_corr': 'DOUBLE',
        'lstf_ndvi_pval': 'DOUBLE', 
        'lstf_ndmi_pval': 'DOUBLE', 
        'ndvi_ndmi_pval': 'DOUBLE',
        'lstf_temporal': 'VARCHAR',
        'ndvi_temporal': 'VARCHAR',
        'ndmi_temporal': 'VARCHAR',
        'ls_land_cover': 'VARCHAR',
        'cl_land_cover': 'INTEGER',
        # Terrain attributes
        'terrain_classification': 'VARCHAR',
        'slope': 'DOUBLE',
        'aspect': 'DOUBLE',
        'tri': 'DOUBLE',
        'tpi': 'DOUBLE',
        'anisotropy': 'DOUBLE',
        'cgap': 'DOUBLE',
        'downhill_fraction': 'DOUBLE',
        'cgap_uphill': 'DOUBLE',
        'uphill_fraction': 'DOUBLE',
        'dom_angle_deg': 'DOUBLE',
        'dom_dir': 'DOUBLE',
        'dom_ptb_lat': 'DOUBLE',
        'dom_ptb_lon': 'DOUBLE',
        'dom_dir_elv': 'DOUBLE',
        'dd_geoid': 'INTEGER',
        }, DROP)

if args["make_spectral_table"] == True:
    # Create table for 3Dep + LS8 composite data
    create_duckDb_table(conn, 'spectral_temporal', {
        'geoid': 'INTEGER PRIMARY KEY',
        'lat': 'DOUBLE',
        'lon': 'DOUBLE', 
        'lstf': 'DOUBLE',
        'lstf_serc': 'DOUBLE', 
        'lstf_arc': 'DOUBLE',
        'ndvi': 'DOUBLE', 
        'ndvi_serc': 'DOUBLE', 
        'ndvi_arc': 'DOUBLE', 
        'ndmi': 'DOUBLE',
        'ndmi_serc': 'DOUBLE',
        'ndmi_arc': 'DOUBLE',
        'idx_row': 'INTEGER',
        'idx_col': 'INTEGER',
        'lstf_ndvi_corr': 'DOUBLE',
        'lstf_ndmi_corr': 'DOUBLE', 
        'ndvi_ndmi_corr': 'DOUBLE',
        'lstf_ndvi_pval': 'DOUBLE', 
        'lstf_ndmi_pval': 'DOUBLE', 
        'ndvi_ndmi_pval': 'DOUBLE',
        'lstf_temporal': 'VARCHAR',
        'ndvi_temporal': 'VARCHAR',
        'ndmi_temporal': 'VARCHAR',
        }, DROP)

if args["make_threeDep_table"] == True:
    # Create table for 3Dep + LS8 composite data
    create_duckDb_table(conn, 'three_dep', {
        'rowId': 'INTEGER PRIMARY KEY',
        'idx_row': 'INTEGER',
        'idx_col': 'INTEGER',
        'lat': 'DOUBLE',
        'lon': 'DOUBLE',
        'elv_rel': 'DOUBLE',
        'elv': 'DOUBLE',
        'slope': 'DOUBLE',
        }, DROP)
    
conn.commit()
conn.close()

print("DuckDB tempGeo tables created")