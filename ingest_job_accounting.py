import os
import pandas as pd
import psycopg2
from psycopg2 import extras

# Connect to the PostgreSQL database
conn = psycopg2.connect(host="frescodb", dbname="anvil", user="admin", password=f"{os.getenv('DBPW')}")
cursor = conn.cursor()

print("Connected to the database")

job_accounting_directory_path = "/mnt/data/JobAccounting"

for filename in os.listdir(job_accounting_directory_path):
    if filename.endswith(".csv") and "job_accounting_oct2022_anon.csv" not in filename and "job_accounting_jan2023_anon.csv" not in filename:
        file_path = os.path.join(job_accounting_directory_path, filename)

        print(f'Starting on {filename}')

        # Load the CSV file
        df = pd.read_csv(file_path, delimiter=',')

        # Print out the column names
        print(df.columns)

        # List of unnecessary columns
        columns_to_drop = ['Shared', 'Cpu Time', 'Node Time', 'Requested Nodes', 'Wait Time', 'Wall Time',
                           'Eligible Time']

        # Check if the columns exist in the DataFrame before dropping them
        columns_to_drop = [col for col in columns_to_drop if col in df.columns]

        # Drop the unnecessary columns
        df = df.drop(columns=columns_to_drop)

        # Drop rows with missing 'submit_time' values
        df = df.dropna(subset=['submit_time'])

        # Rename the columns to match the database schema
        df = df.rename(columns={
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
        })

        print(df.head())

        # Convert 'Hosts' to a list
        df['host_list'] = df['host_list'].astype(str).str.split(',')

        # Convert timestamp columns to the appropriate format
        df['end_time'] = pd.to_datetime(df['end_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['start_time'] = pd.to_datetime(df['start_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['submit_time'] = pd.to_datetime(df['submit_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

        print(df.head())
        # Order the DataFrame columns to match the PostgreSQL table
        df = df[['jid', 'submit_time', 'start_time', 'end_time',
                 'timelimit', 'nhosts', 'ncores', 'ngpus',
                 'username', 'account', 'queue', 'jobname',
                 'exitcode', 'host_list']]

        # Prepare the INSERT statement
        insert_sql = """
            INSERT INTO job_data (
                jid, submit_time, start_time, end_time, 
                timelimit, nhosts, ncores, ngpus, 
                username, account, queue, jobname, 
                exitcode, host_list
            )
            VALUES %s
        """

        print("Preparing the INSERT statement")

        # Prepare the data to be inserted
        data_to_insert = list(df.itertuples(index=False, name=None))

        print(f"Inserting data for {filename}")

        # Use psycopg2.extras.execute_values() to insert the data
        extras.execute_values(cursor, insert_sql, data_to_insert)

        # Commit the transaction
        conn.commit()

        print(f"Data committed for {filename}")

# Close the cursor and connection
cursor.close()
conn.close()
