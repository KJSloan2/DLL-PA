import os
import sqlite3

args = {
    "make_dir_ref_table": True,
    "make_site_info_table": True
}

conn = sqlite3.connect('ref.db')

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
        
if args["make_dir_ref_table"]:
    create_table('dir_ref', {
        'ROW_ID': 'TEXT PRIMARY KEY',
        'DIR_NAME': 'TEXT',
        'DIR_PATH': 'TEXT',
        'USEAGE': 'TEXT',
        })
    
if args["make_site_info_table"]:
    create_table('site_info', {
        'ROW_ID': 'TEXT PRIMARY KEY',
        'NAME': 'TEXT',
        'COORDS': 'TEXT',
        'STATE_NAME': 'TEXT',
        'STATE_FIPS': 'TEXT',
        'COUNTY_NAME': 'TEXT',
        'COUNTY_FIPS': 'TEXT',
        'PLACE_NAME': 'TEXT',
        'PLACE_GEOID': 'TEXT',
        'HAS_HYDRO_FEATURES': 'TEXT'
        })
conn.close()