# individual_lambdas/lambda_2_data_processing.py

# Load data from the source/transformed bucket ('transformed' folder in this offline implementation)

import os
import json
from matplotlib import pyplot as plt

# Get all files in the transformed folder
transformed_files = os.listdir("transformed")
# Load all json files in the transformed folder
for transformed_file in transformed_files:
    if transformed_file.endswith(".json"):
        with open(f"transformed/{transformed_file}", "r") as f:
            date = transformed_file.split("-", 1)[1]
            transformed_data = json.load(f)
            # Create graph for each timeseries
            # Set the title of this timeseries
            for timeseries in transformed_data["data"]:
                x = []
                y = []
                for data in timeseries["data"]:
                    x.append(data["timestamp"])
                    y.append(data["value"])
                plt.title(f"{transformed_data['name']} - {timeseries['label']} ({timeseries['uom'].replace(".", "/")})")
                plt.plot(x, y, label=timeseries["label"])
                # Rotate the x axis labels so they don't overlap
                plt.xticks(rotation=45, ha='right')
                # Save the plot png to the 'out' folder (represents the public S3 bucket)
                plt.savefig(f"out/{transformed_data['name'].replace(', ', '_')}-{timeseries['label'].replace(' ', '_')}-{date}.png", bbox_inches="tight", dpi=80)
                # Show the plot in the notebook
                plt.show()
                # Clear the plot so we can make a new one in the next iteration
                plt.clf()