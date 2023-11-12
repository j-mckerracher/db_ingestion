import os
import argparse
import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Import CSV files into the restructured_sparse_data database.')
parser.add_argument('--file_dir', help='Directory containing CSV files', required=True)
args = parser.parse_args()

# DB conn params
db_username = 'admin'
db_password = f"{os.getenv('DBPW')}"
db_host = 'frescodb'
db_port = '5432'
db_name = 'anvil'
db_table_name = 'restructured_sparse_data'

# Directory containing CSV files
file_dir = args.file_dir

# Create a database engine
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Iterate over files in file_dir
for file in os.listdir(file_dir):
    if file.endswith(".csv"):
        csv_file_path = os.path.join(file_dir, file)

        # Read CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Drop the 'Unnamed: 0' index col if it exists
        if 'Unnamed: 0' in df.columns:
            df = df.drop('Unnamed: 0', axis=1)

        # Infer schema and write DataFrame to SQL
        # Use 'replace' for the first file and 'append' for subsequent files
        if_exists_param = 'replace' if file == os.listdir(file_dir)[0] else 'append'
        df.to_sql(db_table_name, engine, if_exists=if_exists_param, index=False)

        print(f"Data from {file} imported successfully into {db_table_name}")

print("All files imported successfully.")
