import os
import sqlite3

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

conn = sqlite3.connect('tabulation_ref.db')

#Initialize a data table to store temporal representations of the raster data for the tile
create_table('msa_ref', {
    'geoid': 'TEXT PRIMARY KEY',
     "csafp": 'TEXT', "cbsafp": 'TEXT', "geopidfq": 'TEXT',
     "name": 'TEXT', "namelsad": 'TEXT', "lsad": 'TEXT', "memi": 'TEXT',
     "mtfcc": 'TEXT', "aland": 'INTEGER', "awater": 'INTEGER'
     })

conn.close()