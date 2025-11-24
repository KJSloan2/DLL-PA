import sqlite3
import os
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))


db_file = os.path.join(parent_dir, "data",  "healthcare_sites.db")
######################################################################################

conn = sqlite3.connect(db_file)
cur = conn.cursor()

# Drop the table if it exists
cur.execute("DROP TABLE IF EXISTS hospitals;")

conn.commit()
cur.close()
conn.close()