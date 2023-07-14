import os
import time
import psycopg2

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(host="frescodb", dbname="anvil", user="admin", password=f"{os.getenv('DBPW')}")

# Create a cursor object
cur = conn.cursor()
print(conn.get_dsn_parameters(), "\n")

# Directory path containing the CSV files
directory_path = "/mnt/data/JobResourceUsage"

try:
    # Iterate over each file in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path, filename)

            # Open the CSV file
            with open(file_path, 'r') as f:
                # Skip the header row
                next(f)

                print(f"Started working on {filename}")

                # Record the start time
                start_time = time.time()

                # Use copy_from to run the COPY command
                cur.copy_from(f, 'host_data', columns=(
                    'jid',
                    'host',
                    'event',
                    'value',
                    'unit',
                    'time'), sep=',', null="")

                time_taken = time.time() - start_time

                print(f"Finished working on {filename} in {time_taken} seconds.")
    conn.commit()
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if conn:
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")