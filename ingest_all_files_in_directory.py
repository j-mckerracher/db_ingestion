import os
from datetime import datetime
import psycopg2
import pytz
import pandas as pd
import tempfile

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

            # define which cols need to be removed
            drop_columns = [
                'Shared',
                'Cpu Time',
                'Node Time',
                'Requested Nodes',
                'Wait Time',
                'Wall Time',
                'Eligible Time'
            ]

            # Only drop columns that exist in the DataFrame
            df = df.drop(columns=[col for col in drop_columns if col in df.columns])

            # Transform Hosts column to PostgreSQL array literal format
            df['Hosts'] = df['Hosts'].apply(lambda x: '{' + x.replace(',', ' ') + '}')

            # Ensure the CSV column order matches the table column order
            df = df.reindex(columns=[
                'Account',  # Account
                'Job Id',  # Job ID
                'Cores',  # Cores
                'Gpus',  # Gpus
                'Nodes',  # Nodes
                'Requested Wall Time',  # Requested Wall Time
                'Queue',  # Queue
                'End Time',  # End Time
                'Start Time',  # Start Time
                'Submit Time',  # Submit Time
                'User',  # user
                'Exit Status',  # Exit Status
                'Hosts',  # hosts
                'Job Name'  # job name
            ])

            # Create a temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=True)

            df.to_csv(temp_file.name, index=False)

            # Open the temporary file
            with open(temp_file.name, 'r') as f:
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

            # Close the temporary file (it will be deleted at this point)
            temp_file.close()
    conn.commit()
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if conn:
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")
