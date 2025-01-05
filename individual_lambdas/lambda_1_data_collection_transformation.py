# individual_lambdas/lambda_1_data_collection_transformation.py

# Import Libraries

import requests
import json
from datetime import datetime
from bs4 import BeautifulSoup

# API URLS
api_hostname = "https://uk-air.defra.gov.uk/sos-ukair"

# The following section is being used to get the list of timeseries from UK-AIR, and finding all the timeseries available at the Sibton site in Suffolk
# This is the data collection stage

# Get timeseries list
timeseries_url = api_hostname + "/api/v1/timeseries.json"
timeseries_response = requests.get(timeseries_url).json()

# Find all timeseries at a given point
# For testing purposes we are using the "Sibton" station

search_station_lat = 52.2944
search_station_lon = 1.4634969999517549

timeseries_ids = {}

for timeseries in timeseries_response:
    try:
        if timeseries["station"]["geometry"]["coordinates"][0] == search_station_lat and timeseries["station"]["geometry"]["coordinates"][1] == search_station_lon and timeseries["station"]["geometry"]["type"] == "Point":
            # print(timeseries)
            # Lookup the label on eionet
            # This is not 100% needed, so if it fails just use the existing label
            try:
                eionet_url = timeseries["label"].split(" ")[0]
                eionet_response = requests.get(eionet_url)
                eionet_soup = BeautifulSoup(eionet_response.text, 'html.parser')
                # Get the table with class datatable to find the notation text
                eionet_table = eionet_soup.find_all("table", class_="datatable")
                table_rows = eionet_table[0].find_all("tr")
                for row in table_rows:
                    row_name = row.find_all("th")
                    if row_name[0].text.strip() == "Notation":
                        # This is the notation field, get the value which is in the td element
                        row_value = row.find_all("td")[0].text.strip()
                        timeseries_ids[f"{row_value} - {timeseries["uom"]}"] = timeseries["id"]
            except:
                print("Failed to lookup Eionet URL for " + timeseries["label"])
                timeseries_ids[f"{timeseries["label"]} - {timeseries["uom"]}"] = timeseries["id"]
    except KeyError:
        # Skip if the station does not have a geometry, since we're only searching for one with known geometry
        pass

# The following block is transforming the data so that future blocks can more easily use it to create graphs. It then saves this data into the transformed folder (representing the AWS bucket) and would also save to the AWS RDS instance
# Get data from the timeseries
# So we can save the data to the 'transformed' bucket (folder) and to the theoretical AWS RDS instance
this_station_data = {
    "name": "Sibton, Suffolk",
    "lat": search_station_lat,
    "lon": search_station_lon,
    "data": []
}
for label, timeseries_id in timeseries_ids.items():

    station_data_url = api_hostname + "/api/v1/timeseries/" + timeseries_id + "/getData"

    # Get todays date as YYYY-MM-DD
    today = datetime.today().strftime('%Y-%m-%d')

    # This timespan means it will get the 24 hours of data up to the curernt date (all of yesterdays data)
    params = {
        "timespan": f"PT24H/{today}",
    }

    station_data_response = requests.get(station_data_url, params=params).json()

    # Build data dictionary for this timeseries
    this_timeseries_data = {
        "label": label.split(" - ")[0],
        "uom": label.split(" - ")[1],
        "data": []
    }

    # Add the data to the station data
    for data in station_data_response["values"]:
        transformed_timestamp = datetime.fromtimestamp(data["timestamp"] / 1000).strftime('%Y-%m-%d_%H:%M')
        this_timeseries_data["data"].append({
            "timestamp": transformed_timestamp,
            "value": data["value"]
        })
    
    # Add the data dictioanry to the overall station
    this_station_data["data"].append(this_timeseries_data)


# Save the data to the 'transformed' bucket (folder)
# Get todays date at YYYY-MM-DD
today = datetime.today().strftime('%Y-%m-%d')
# This file writing section would be replaced by code to insert the data into the AWS S3 bucket
with open(f"transformed/{this_station_data['name'].replace(', ', '_')}-{today}.json", "w") as f:
    json.dump(this_station_data, f, indent=4)
# Code would go here to insert data to the AWS RDS instance