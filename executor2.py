import subprocess
import os

# Build the project
build_command = "dotnet build"
subprocess.run(build_command, shell=True, check=True)

# Define the parameters
num_nodes_list = [25, 50, 100, 500, 1000, 5000, 8000, 10000, 15000]
topologies = ["full", "2d", "line", "imperfect3d"]
protocols = ["gossip", "pushsum"]

# Create the outputs folder if it doesn't exist
os.makedirs("outputs", exist_ok=True)

# Iterate through all combinations of parameters
for num_nodes in num_nodes_list:
    for topology in topologies:
        for protocol in protocols:
            # Skip if already present
            filename = f"outputs/filename-{num_nodes}-{topology}-{protocol}.txt"
            if os.path.exists(filename):
                print(filename)
                continue

            # Skip specific combinations
            if (num_nodes >= 8000 and protocol == "pushsum"):
                continue

            # Create the command
            command = f"dotnet bin/Debug/net7.0/gossip.dll {num_nodes} {topology} {protocol}"

            # Execute the command and log output to file
            with open(filename, "w") as file:
                try:
                    subprocess.run(command, shell=True, stdout=file, stderr=subprocess.STDOUT, check=True)
                except subprocess.CalledProcessError as e:
                    error_msg = f"Command failed: {command}\nError: {e}\n"
                    file.write(error_msg)
