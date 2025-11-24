import sqlite3


conn = sqlite3.connect(r"backend\data\healthcare_sites.db")
cursor = conn.cursor()

# Query to select all rows from the agricultural_digesters table
#query = "SELECT * FROM agricultural_digesters"

# Execute the query
#cursor.execute(query)

#query = "SELECT name FROM hospitals"
#cursor.execute(query)

for row in cursor.fetchall():
    print(row)
    '''latitude, longitude = row
    print(f"Latitude: {latitude}, Longitude: {longitude}")'''

# Close the connection
conn.close()

print("DONE")