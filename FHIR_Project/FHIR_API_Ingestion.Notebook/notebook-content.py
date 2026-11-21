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

# MARKDOWN ********************

# **RAW LAYER — API TO RAW JSON**

# CELL ********************

# Import required libraries
import requests
import json
import os
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# FHIR API configuration
base_url = "https://hapi.fhir.org/baseR4"

# Resources to ingest
resource_types = ["Patient", "Encounter", "Observation", "Condition"]

# Ingestion settings
page_size = 100
max_pages = 5

# Generate a unique batch timestamp for this execution
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Extract date folders from batch timestamp
year = timestamp[:4]
month = timestamp[4:6]
day = timestamp[6:8]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch FHIR data page-by-page and save original Bundle JSON files to Raw

def fetch_fhir_data(resource_type):

    # Construct resource API endpoint
    api_url = f"{base_url}/{resource_type}"

    # Initial API parameters
    params = {
        "_count": page_size,
        "_format": "json"
    }

    # Create date-based Raw folder
    raw_folder = f"Files/Raw/{resource_type}/{year}/{month}/{day}"
    os.makedirs(f"/lakehouse/default/{raw_folder}", exist_ok=True)

    current_url = api_url
    page_count = 0
    total_records = 0

    # Fetch pages until there are no more pages or max_pages is reached
    while current_url and page_count < max_pages:

        response = requests.get(current_url, params=params, timeout=30)
        response.raise_for_status()

        # Read original FHIR Bundle
        bundle = response.json()

        # Save original Bundle JSON without modifying its structure
        file_path = f"/lakehouse/default/{raw_folder}/{resource_type}_{timestamp}_page{page_count+1}.json"

        with open(file_path, "w") as file:
            json.dump(bundle, file, indent=2)

        # Count resources in this Bundle
        total_records += len(bundle.get("entry", []))

        # Find next page URL supplied by FHIR API
        current_url = next(
            (link["url"] for link in bundle.get("link", [])
             if link["relation"] == "next"),
            None
        )

        # Next page URL already contains its parameters
        params = None

        page_count += 1

    print(f"{resource_type}: {page_count} pages | {total_records} records")

    return {
        "resource": resource_type,
        "pages": page_count,
        "records": total_records,
        "batch_id": timestamp
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ingest all FHIR resources into the Raw layer

raw_results = []

for resource in resource_types:
    result = fetch_fhir_data(resource)
    raw_results.append(result)

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
