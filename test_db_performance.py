import os
import time
import psycopg2
import datetime

# Get the database password from the environment variables
db_password = os.getenv('DBPW')

# Establish a connection to the database
conn = psycopg2.connect(host="frescodb", dbname="anvil", user="fresco", password=db_password)

# Create a cursor object
cur = conn.cursor()

# Define the start and end of the date range
start_date = datetime.datetime(2022, 7, 1, 0, 0, 0)
end_date = datetime.datetime(2022, 7, 31, 23, 59, 59)

# Define the SQL query
sql_query = """
    SELECT *
    FROM public.host_data
    WHERE time >= %s AND time <= %s
"""

print("Querying the DB.")

# Record the start time
start_time = time.time()

# Execute the SQL query
cur.execute(sql_query, (start_date, end_date))

# Record the end time
end_time = time.time()

# Calculate the elapsed time, in seconds
elapsed_time = end_time - start_time

print(f"The query took {elapsed_time} seconds to execute.")


# Close the cursor and connection
cur.close()
conn.close()
