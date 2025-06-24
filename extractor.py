import os
import csv
from pathlib import Path

# Folder paths
output_folder = "outputs"
csv_filename = "convergence_data.csv"
missing_info_filename = "files_missing_info.txt"

# Collect data for CSV and missing info
csv_data = []
files_missing_info = []

# Iterate through files in the outputs folder
for file_name in os.listdir(output_folder):
    file_path = os.path.join(output_folder, file_name)
    
    if os.path.isfile(file_path):
        with open(file_path, "r") as file:
            lines = file.readlines()
            if lines:
                last_line = lines[-1].strip()
                if "has converged in" in last_line:
                    parts = last_line.split(" ")
                    num_nodes, topology, protocol = Path(file_name).stem.split("-")[1:]
                    convergence_time = int(parts[-2])
                    csv_data.append([num_nodes, topology, protocol, convergence_time])
                else:
                    files_missing_info.append(file_name)

# Write CSV data to file
with open(csv_filename, "w", newline="") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["num_nodes", "topology", "protocol", "convergencetime"])
    csv_writer.writerows(csv_data)

# Write filenames missing info to file
with open(missing_info_filename, "w") as missing_info_file:
    missing_info_file.write("\n".join(files_missing_info))
