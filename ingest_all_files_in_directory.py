import os
import psycopg2
import pandas as pd
import tempfile
import re

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(host="frescodb", dbname="anvil", user="admin", password=f"{os.getenv('DBPW')}")

# Create a cursor object
cur = conn.cursor()
print(conn.get_dsn_parameters(), "\n")

# Directory path containing the job_data CSV files
job_accounting_directory_path = "/mnt/data/JobAccounting"

try:
    # Iterate over each file in the directory
    for filename in os.listdir(job_accounting_directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(job_accounting_directory_path, filename)

            # use pandas to drop columns that we don't need
            df = pd.read_csv(file_path)

            # Drop unnecessary columns
            df.drop(columns=['Shared', 'Cpu Time', 'Node Time', 'Requested Nodes', 'Wait Time', 'Wall Time',
                             'Eligible Time'], errors='ignore', inplace=True)

            # Transform Hosts column to a string enclosed in braces with elements separated by pipe characters
            df['Hosts'] = df['Hosts'].apply(lambda x: '{' + re.sub(r'\s+', '|', x) + '}')

            # Rename columns to match the database schema
            df.rename(columns={
                'Account': 'account',
                'Job Id': 'jid',
                'Cores': 'ncores',
                'Gpus': 'ngpus',
                'Nodes': 'nhosts',
                'Requested Wall Time': 'timelimit',
                'Queue': 'queue',
                'End Time': 'end_time',
                'Start Time': 'start_time',
                'Submit Time': 'submit_time',
                'User': 'username',
                'Exit Status': 'exitcode',
                'Hosts': 'host_list',
                'Job Name': 'jobname'
            }, inplace=True)

            # Remove any non-alphanumeric characters from 'jobname'
            df['jobname'] = df['jobname'].str.replace(r'\W+', '')

            # Define the order of the columns to match the database
            column_order = ['account', 'jid', 'ncores', 'ngpus', 'nhosts', 'timelimit', 'queue',
                            'end_time', 'start_time', 'submit_time', 'username', 'exitcode',
                            'host_list', 'jobname']

            # Reorder the columns in the DataFrame
            df = df[column_order]

            # Write the DataFrame to a new CSV file
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            df.to_csv(temp_file.name, index=False)

            # Debugging -> Re-open the temporary file in read mode to print
            with open(temp_file.name, 'r') as f:
                # Read the first few lines
                lines = [next(f) for _ in range(50)]
            print(''.join(lines))

            temp_file.close()

            # Open the temporary file
            with open(temp_file.name, 'r') as f:
                # Skip the header row
                next(f)

                print(f"Copying {filename} to the database")
                # insert the data into the database
                cur.copy_from(f, 'job_data', sep=',', null="None", columns=column_order)
                print("Done")

                # Remove the temporary file
                os.remove(temp_file.name)

                # Commit the changes
            conn.commit()

    # After all files have been processed, replace pipe characters back to commas and convert the host_list column to an array within the database
    cur.execute("""
        UPDATE job_data
        SET host_list = string_to_array(replace(host_list, '|', ','), ',')
    """)
    conn.commit()

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if conn:
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")
