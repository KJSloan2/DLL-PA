import os
import sqlite3
import pandas as pd
import sqlite3

conn = sqlite3.connect(r"backend\data\healthcare_sites.db")

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


#Initialize a data table to store temporal representations of the raster data for the tile
create_table('hospitals', {
    "object_id": "INTEGER PRIMARY KEY",
    "id": "TEXT",
    "fid": "INTEGER",
    "name": "TEXT",
    "address": "TEXT",
    "city": "TEXT",
    "state": "TEXT",
    "zip": "TEXT",
    "type": "TEXT",
    "status": "TEXT",
    "county": "TEXT",
    "countyfips": "TEXT",
    "naics_code": "TEXT",
    "source_dat": "TEXT",
    "val_method":"TEXT",
    "val_date": "TEXT",
    "owner": "TEXT",
    "ttl_staff": "TEXT",
    "beds": "TEXT",
    "trauma": "TEXT",
    "date_creat": "TEXT",
    "helipad": "TEXT",
    "lat": "FLOAT",
    "lon": "FLOAT",
})

conn.close()