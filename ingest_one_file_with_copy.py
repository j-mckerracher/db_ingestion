import os
from datetime import datetime
import psycopg2
import pytz

connection = psycopg2.connect(f"host=frescodb, user=admin, password={os.getenv('DBPW')}")

cursor = connection.cursor()

# Folder path containing the CSV files
folder_path = "time_series"

timezone = pytz.timezone('MST')

filename = r"job_ts_metrics_july2022_anon.csv"

try:
    print(connection.get_dsn_parameters(), "\n")
    file_path = os.path.join(folder_path, filename)
    current_time = datetime.now(timezone)
    formatted_time = current_time.strftime("%H:%M:%S")
    print(f"Started working on {filename} at {formatted_time}.")

    # Execute COPY command
    copy_sql = """
        COPY host_data(jid, host, event, value, unit, time) 
        FROM STDIN WITH CSV HEADER DELIMITER as ','
    """
    file = open(file_path, 'r')
    cursor.copy_expert(sql=copy_sql, file=file)
    connection.commit()
    file.close()

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
