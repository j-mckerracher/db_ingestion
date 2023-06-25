import os
import psycopg2
import pytz
from datetime import datetime
import csv

# # Establish a connection to the "anvil" database
# connection = psycopg2.connect("host=172.21.161.27 dbname=anvil user=postgres password=yCZXNhGfYZbRdwH0")
#
# # Create a cursor object to interact with the database
# cursor = connection.cursor()
#
# # Folder path containing the CSV files
# folder_path = "time_series"
#
# timezone = pytz.timezone('MST')
#
# # SQL Query
# query = "INSERT INTO host_data (jid, host, event, value, unit, time) VALUES (%s, %s, %s, %s, %s, %s)"
#
# # Iterate over each file in the folder
# for filename in os.listdir(folder_path):
#     if filename.endswith(".csv"):
#         file_path = os.path.join(folder_path, filename)
#
#         current_time = datetime.now(timezone)
#         formatted_time = current_time.strftime("%H:%M:%S")
#         print(f"Started working on {filename} at {formatted_time}")
#
#         with open(file_path, 'r') as f:
#             reader = csv.reader(f)
#
#             next(reader)  # Skip the header row
#
#             rows = list(reader)
#
#             # Insert the rows in bulk
#             cursor.executemany(query, rows)
#
#         # Commit the transaction after each file has been processed
#         connection.commit()
#
# # Close the cursor and the database connection
# cursor.close()
# connection.close()

# Establish a connection to the "anvil" database
connection = psycopg2.connect("host=172.21.161.27 dbname=anvil user=postgres password=yCZXNhGfYZbRdwH0")

# Create a cursor object to interact with the database
cursor = connection.cursor()

# Folder path containing the CSV files
folder_path = "time_series"

timezone = pytz.timezone('MST')

# Iterate over each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)

        current_time = datetime.now(timezone)
        formatted_time = current_time.strftime("%H:%M:%S")
        print(f"Started working on {filename} at {formatted_time}")

        # Open and read the CSV file
        with open(file_path, 'r') as file:
            # Skip the header row
            next(file)

            # Iterate over each line (row) in the CSV file
            for line in file:
                # Split the line into individual values
                values = line.strip().split(',')

                # Insert the values into the "host_data" table
                query = f"INSERT INTO host_data (jid, host, event, value, unit, time) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(query, (values[0], values[1], values[2], values[3], values[4], values[5]))
                connection.commit()


# Close the cursor and the database connection
cursor.close()
connection.close()

