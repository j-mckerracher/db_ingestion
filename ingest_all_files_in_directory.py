import os
from datetime import datetime
import psycopg2
import pytz
import pandas as pd

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(host="frescodb", dbname="anvil", user="admin", password=f"{os.getenv('DBPW')}")

# Create a cursor object
cur = conn.cursor()
print(conn.get_dsn_parameters(), "\n")

# Directory path containing the job_data CSV files
job_accounting_directory_path = "/mnt/data/JobAccounting"

# Directory path containing the job_data CSV files
job_resource_usage_directory_path = "/mnt/data/JobResourceUsage"

try:
    # Iterate over each file in the directory
    for filename in os.listdir(job_accounting_directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(job_accounting_directory_path, filename)

            # use pandas to drop columns that we don't need
            df = pd.read_csv(file_path)
            df = df.drop(columns=[
                'Shared',
                'Cpu Time',
                'Node Time',
                'Requested Nodes',
                'Wait Time',
                'Wall Time',
                'Eligible Time'
            ])
            # Transform Hosts column to PostgreSQL array literal format
            df['Hosts'] = df['Hosts'].apply(lambda x: '{' + x.replace(',', ' ') + '}')

            df.to_csv(file_path, index=False)

            # Open the CSV file
            with open(file_path, 'r') as f:
                # Skip the header row
                next(f)

                # time stuff
                timezone = pytz.timezone('MST')
                current_time = datetime.now(timezone)
                formatted_time = current_time.strftime("%H:%M:%S")

                print(f"Started working on {filename} at {formatted_time}.")

                # Add into the job_data table
                cur.copy_from(f, 'job_data', columns=(
                    'account',   # Account
                    'jid',   # Job ID
                    'ncores',   # Cores
                    'ngpus',  # Gpus
                    'nhosts',  # Nodes
                    'timelimit',  # Requested Wall Time
                    'queue',  # Queue
                    'end_time',  # End Time
                    'start_time',  # Start Time
                    'submit_time',  # Submit Time
                    'username',  # user
                    'exitcode',   # Exit Status
                    'host_list',  # hosts
                    'jobname'  # job name
                ), sep=',', null="")

                # Add into the host_data table
                # cur.copy_from(f, 'host_data', columns=(
                #     'jid',
                #     'host',
                #     'event',
                #     'value',
                #     'unit',
                #     'time'), sep=',', null="")

                current_time = datetime.now(timezone)
                formatted_time = current_time.strftime("%H:%M:%S")
                print(f"Finished working on {filename} at {formatted_time}.")
    conn.commit()
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if conn:
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")
