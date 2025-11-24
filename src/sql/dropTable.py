import sqlite3

# Step 1: Connect to the SQLite3 database
conn = sqlite3.connect('tabulation_ref.db')
cursor = conn.cursor()

# Step 2: Drop the table (if it exists)
cursor.execute('DROP TABLE IF EXISTS gaz_place')
print('TABLE DROPPED')
# Step 3: Commit the changes
conn.commit()

# Step 4: Close the connection
conn.close()