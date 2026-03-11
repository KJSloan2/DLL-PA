import sqlite3

def create_table(db_conn, table_name, columns, drop_table):
    cursor = db_conn.cursor()
    if drop_table: 
        cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
    # Step 3: Commit the changes
    db_conn.commit()
    # Create a dynamic SQL query to create a table
    column_defs = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})"
    # Execute the query
    cursor.execute(query)
    # Commit and close
    db_conn.commit()
    print(table_name)
######################################################################################
######################################################################################
def get_table_info(dbName, dbTable, infoToGet):
    conn = sqlite3.connect(dbName)
    cursor = conn.cursor()
    table = cursor.execute(f"SELECT * FROM {dbTable}").fetchone()
    table_headers = [description[0] for description in cursor.description]
    headers_dict = dict(zip(table_headers, table))
    infoDict = {}
    for key in infoToGet:
        if key in headers_dict:
            try:
                infoDict[key] = eval(headers_dict[key])
            except Exception as e:
                print(f"Error evaluating {key}: {e}")
            try:
                infoDict[key] = headers_dict[key]
            except Exception as e:
                print(f"Error evaluating {key}: {e}")
    return infoDict