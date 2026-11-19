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
