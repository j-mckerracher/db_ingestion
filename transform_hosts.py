import csv
import os

input_folder = r""

output_folder = r""

# Iterate over each file in the directory
for filename in os.listdir(input_folder):
    if filename.endswith(".csv"):
        input_file_path = os.path.join(input_folder, filename)
        output_file_path = os.path.join(output_folder, filename)

        # Open the file in read mode and open a new temporary file in write mode
        with open(input_file_path, 'r') as csvfile, open(output_file_path, 'w', newline='') as outfile:
            # Create a CSV reader and writer
            reader = csv.DictReader(csvfile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)

            # Write the header to the new file
            writer.writeheader()

            # Iterate over each row in the CSV file
            for row in reader:
                # Replace commas with spaces in the 'Hosts' column
                if 'Hosts' in row and row['Hosts'] is not None:
                    row['Hosts'] = '{' + row['Hosts'].replace(',', ' ') + '}'

                # Write the row to the new file
                writer.writerow(row)
