# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8173e9e9-6f92-44c5-abf7-3582368e1294",
# META       "default_lakehouse_name": "FHIR_Lakehouse",
# META       "default_lakehouse_workspace_id": "8b818f37-4938-41af-a721-f37ba1a845c7",
# META       "known_lakehouses": [
# META         {
# META           "id": "8173e9e9-6f92-44c5-abf7-3582368e1294"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

print("FHIR Data Engineering Project started!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

url = "https://hapi.fhir.org/baseR4/Patient?_count=2&_format=json"

response = requests.get(url)

print(response.status_code)
print(response.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = response.json()

print("Resource type:", data["resourceType"])
print("Records returned:", len(data.get("entry", [])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

patients = [entry["resource"] for entry in data["entry"]]

print("Extracted Patients:", len(patients))
print(patients[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

df = pd.json_normalize(patients)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resource_type = "Patient"

print("Processing resource:", resource_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_url = "https://hapi.fhir.org/baseR4"

api_url = f"{base_url}/{resource_type}"

print(api_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

params = {
    "_count": 100,
    "_format": "json"
}

print(params)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response = requests.get(api_url, params=params)

print("Status code:", response.status_code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bundle = response.json()

print("Bundle type:", bundle["resourceType"])
print("Records in page:", len(bundle.get("entry", [])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resources = [entry["resource"] for entry in bundle.get("entry", [])]

print("Resources extracted:", len(resources))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

next_link = next(
    (link["url"] for link in bundle.get("link", [])
     if link["relation"] == "next"),
    None
)

print(next_link)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

next_response = requests.get(next_link)
next_bundle = next_response.json()

print("Status code:", next_response.status_code)
print("Records on next page:", len(next_bundle.get("entry", [])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_resources = resources + [
    entry["resource"]
    for entry in next_bundle.get("entry", [])
]

print("Total records collected:", len(all_resources))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_resources = []
current_url = api_url
current_params = params
max_pages = 5
page_count = 0

while current_url and page_count < max_pages:

    response = requests.get(current_url, params=current_params, timeout=30)
    response.raise_for_status()

    bundle = response.json()

    all_resources.extend(
        entry["resource"]
        for entry in bundle.get("entry", [])
    )

    page_count += 1

    current_url = next(
        (link["url"] for link in bundle.get("link", [])
         if link["relation"] == "next"),
        None
    )

    current_params = None

print("Pages fetched:", page_count)
print("Total records:", len(all_resources))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("First resource type:", all_resources[0]["resourceType"])
print("First resource ID:", all_resources[0]["id"])
print("Total resources:", len(all_resources))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

patient_ids = [r["id"] for r in all_resources]

print("Total records:", len(patient_ids))
print("Unique IDs:", len(set(patient_ids)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(all_resources[0].keys())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select the FHIR resource we want to extract
resource_type = "Patient"

# Build the API URL dynamically
base_url = "https://hapi.fhir.org/baseR4"
api_url = f"{base_url}/{resource_type}"

print("Resource:", resource_type)
print("API URL:", api_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch API pages, save original Bundles, and extract resources

def fetch_fhir_data(resource_type, max_pages=5):

    api_url = f"https://hapi.fhir.org/baseR4/{resource_type}"
    params = {"_count": 100, "_format": "json"}

    all_resources = []
    current_url = api_url
    page_count = 0

    # Create date-based Raw folder
    raw_folder = f"/lakehouse/default/Files/Raw/{resource_type}/{year}/{month}/{day}"
    os.makedirs(raw_folder, exist_ok=True)

    while current_url and page_count < max_pages:

        response = requests.get(current_url, params=params, timeout=30)
        response.raise_for_status()

        bundle = response.json()

        # Save original API Bundle without modifying it
        file_path = f"{raw_folder}/{resource_type}_{timestamp}_page{page_count+1}.json"

        with open(file_path, "w") as f:
            json.dump(bundle, f, indent=2)

        # Extract individual resources
        all_resources.extend(
            entry["resource"]
            for entry in bundle.get("entry", [])
        )

        page_count += 1

        # Follow API pagination link
        current_url = next(
            (link["url"] for link in bundle.get("link", [])
             if link["relation"] == "next"),
            None
        )

        params = None

    print(f"{resource_type}: {page_count} pages saved")

    return all_resources

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch 5 pages of Patient data using our function
patients = fetch_fhir_data("Patient", max_pages=5)

print("Patients extracted:", len(patients))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test the same function with Encounter
encounters = fetch_fhir_data("Encounter", max_pages=2)

print("Encounters extracted:", len(encounters))
print("Resource type:", encounters[0]["resourceType"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch 2 pages of Observation data
observations = fetch_fhir_data("Observation", max_pages=2)

print("Observations extracted:", len(observations))
print("Resource type:", observations[0]["resourceType"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch 2 pages of Condition data
conditions = fetch_fhir_data("Condition", max_pages=2)

print("Conditions extracted:", len(conditions))
print("Resource type:", conditions[0]["resourceType"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save original Patient data as JSON in Lakehouse Raw

import json
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

file_path = f"/lakehouse/default/Files/Raw/Patient/Patient_{timestamp}.json"

with open(file_path, "w") as f:
    json.dump(patients, f, indent=2)

print("Saved:", file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save original Encounter data to Raw

file_path = f"/lakehouse/default/Files/Raw/Encounter/Encounter_{timestamp}.json"

with open(file_path, "w") as f:
    json.dump(encounters, f, indent=2)

print("Saved:", file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save original Observation data to Raw

file_path = f"/lakehouse/default/Files/Raw/Observation/Observation_{timestamp}.json"

with open(file_path, "w") as f:
    json.dump(observations, f, indent=2)

print("Saved:", file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save original Condition data to Raw

file_path = f"/lakehouse/default/Files/Raw/Condition/Condition_{timestamp}.json"

with open(file_path, "w") as f:
    json.dump(conditions, f, indent=2)

print("Saved:", file_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify saved Raw files

import os

for resource in ["Patient", "Encounter", "Observation", "Condition"]:
    folder = f"/lakehouse/default/Files/Raw/{resource}"
    files = os.listdir(folder)

    print(resource, ":", files)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Organize Raw files by extraction date

import os
import shutil

date_folder = timestamp[:8]  # YYYYMMDD
year, month, day = date_folder[:4], date_folder[4:6], date_folder[6:8]

for resource in ["Patient", "Encounter", "Observation", "Condition"]:

    # Source file
    source = f"/lakehouse/default/Files/Raw/{resource}/{resource}_{timestamp}.json"

    # Create date-based destination folder
    destination_folder = f"/lakehouse/default/Files/Raw/{resource}/{year}/{month}/{day}"
    os.makedirs(destination_folder, exist_ok=True)

    # Move file
    destination = f"{destination_folder}/{resource}_{timestamp}.json"
    shutil.move(source, destination)

    print(f"{resource}: moved successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify date-based Raw folders

for resource in ["Patient", "Encounter", "Observation", "Condition"]:
    path = f"/lakehouse/default/Files/Raw/{resource}/{year}/{month}/{day}"
    print(resource, ":", os.listdir(path))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Re-fetch Patient data and save original API Bundles
patients = fetch_fhir_data("Patient", max_pages=5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Re-fetch Encounter data and save original API Bundles
encounters = fetch_fhir_data("Encounter", max_pages=5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Re-fetch Observation data and save original API Bundles
observations = fetch_fhir_data("Observation", max_pages=5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Re-fetch Condition data and save original API Bundles
conditions = fetch_fhir_data("Condition", max_pages=5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify original Bundle files in Raw

for resource in ["Patient", "Encounter", "Observation", "Condition"]:
    path = f"/lakehouse/default/Files/Raw/{resource}/{year}/{month}/{day}"
    files = os.listdir(path)
    print(resource, ":", files)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect one saved original Bundle

with open(file_path, "r") as f:
    saved_bundle = json.load(f)

print("Type:", saved_bundle["resourceType"])
print("Entries:", len(saved_bundle.get("entry", [])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the first saved Patient Bundle

path = f"/lakehouse/default/Files/Raw/Patient/{year}/{month}/{day}/Patient_{timestamp}_page1.json"

with open(path, "r") as f:
    saved_bundle = json.load(f)

print("Type:", saved_bundle["resourceType"])
print("Entries:", len(saved_bundle.get("entry", [])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
