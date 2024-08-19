import pyodbc
from datetime import datetime
from typing import Union, List, Tuple

def get_origin_connection() -> pyodbc.Connection:
    connection_string = "DSN= database_origin;"
    return pyodbc.connect(connection_string)

def get_destination_connection() -> pyodbc.Connection:
    connection_string = "DSN= database_destination;"
    return pyodbc.connect(connection_string)

def execute_query(conn: pyodbc.Connection, query: str, params: Union[tuple, list] = None) -> Union[None, list]:
    rows = None
    try:
        with conn.cursor() as curs:
            if params:
                if query.strip().upper().startswith('INSERT'):
                    curs.executemany(query, params)
                else:
                    curs.execute(query, params)
            else:
                curs.execute(query)
            if query.strip().upper().startswith('SELECT'):
                rows = curs.fetchall()
    except Exception as error:
        raise error
    return rows

# Function to get the most recent date from the destination table
def get_most_recent_date(conn: pyodbc.Connection) -> datetime:
    query = "SELECT MAX(date_column) FROM table_name"
    result = execute_query(conn, query)
    if result and result[0][0]:
        return result[0][0]
    return None

# Step 1: Fetch data from the origin database
query1 = '''
SELECT
    your_conditions
'''

# Connect to the destination database and get the most recent date
dest_conn = get_destination_connection()
most_recent_date = get_most_recent_date(dest_conn)
dest_conn.close()

# Execute query1 to get data from the source
source_conn = get_origin_connection()
data = execute_query(source_conn, query1)
source_conn.close()

# Filter out rows based on the most recent date from the destination
def filter_data_by_date(rows, recent_date):
    if not recent_date:
        return rows  # If there's no date in the destination table, include all rows
    filtered_rows = [row for row in rows if row[1] > recent_date]
    return filtered_rows

filtered_data = filter_data_by_date(data, most_recent_date)

# If there's no new data to insert, exit
if not filtered_data:
    print("No new data to insert.")
    exit()

# Step 2: Create the destination table if it does not exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS table_name (
    column1 VARCHAR(255),
    column2 VARCHAR(255),
    date_colum DATE
);
'''

# Connect to the destination database and create the table
dest_conn = get_destination_connection()
execute_query(dest_conn, create_table_query)
dest_conn.close()

# Step 3: Prepare the INSERT statement
insert_query = '''
INSERT INTO table_name (
    column1,
    column2,
    date_column
) VALUES (?, ?, ?)
'''

# Step 4: Transform data to fit the destination table schema
def transform_data(rows):
    transformed_rows = []
    for row in rows:
        column1 = row[0]
        column2 = row[1]
        column3 = row[2]
        
        transformed_rows.append((
            column1,
            column2,
            column3
        ))
    return transformed_rows

transformed_data = transform_data(filtered_data)

# Step 5: Insert transformed data into the destination table
def insert_data(data):
    conn = get_destination_connection()
    try:
        with conn.cursor() as curs:
            curs.executemany(insert_query, data)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        conn.close()

insert_data(transformed_data)
