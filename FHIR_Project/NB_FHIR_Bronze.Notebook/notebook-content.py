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

# # Bronze Layer – FHIR Data Engineering
# 
# Purpose:
# - Read original FHIR Bundle JSON files from Raw.
# - Extract individual FHIR resources.
# - Add ingestion metadata.
# - Store data as Delta tables.
# 
# Source: Raw layer in FHIR_Lakehouse
# Target: Bronze Delta tables
# Resources: Patient, Encounter, Observation, Condition

# CELL ********************

# Import Spark functions 
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create separate schemas for the Bronze 
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bronze layer configuration
from pyspark.sql.functions import explode, current_timestamp, lit
import re

# FHIR resources to process
resources = ["Patient", "Encounter", "Observation", "Condition"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Discover Raw JSON files for each FHIR resource

def get_raw_files(resource):

    # Root folder for this resource
    root_path = f"Files/Raw/{resource}"

    # Recursively collect all JSON files
    files = []

    def scan_folder(path):
        for item in notebookutils.fs.ls(path):
            if item.isDir:
                scan_folder(item.path)
            elif item.name.endswith(".json"):
                files.append(item.path)

    scan_folder(root_path)

    # Keep only files matching the resource name
    files = [f for f in files if f.split("/")[-1].startswith(resource + "_")]

    if not files:
        raise ValueError(f"No Raw JSON files found for {resource}")

    return files

# Example: discover Patient Raw files
patient_files = get_raw_files("Patient")

print("Patient Raw files found:", len(patient_files))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read all discovered Raw Patient Bundle files into Spark
patient_bundle_df = spark.read.option("multiline", "true").json(get_raw_files("Patient"))

# Display Bundle structure
patient_bundle_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flatten FHIR Bundles into individual Patient records
patient_bronze_df = patient_bundle_df.select(
    explode("entry").alias("entry")
).select("entry.resource.*")

display(patient_bronze_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add Bronze ingestion metadata
from datetime import datetime

patient_bronze_df = (
    patient_bronze_df
    .withColumn("extraction_timestamp", current_timestamp())
    .withColumn("source_api", lit("HAPI FHIR R4"))
    .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
)

display(patient_bronze_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save Patient records as a Bronze Delta table in the bronze schema
patient_bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze.Patient")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read all Raw Encounter Bundle files
encounter_bundle_df = spark.read.option("multiline", "true").json(
    get_raw_files("Encounter")
)

# Extract individual Encounter records
encounter_bronze_df = encounter_bundle_df.select(
    explode("entry").alias("entry")
).select("entry.resource.*")

display(encounter_bronze_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add Bronze ingestion metadata to Encounter records
encounter_bronze_df = (
    encounter_bronze_df
    .withColumn("extraction_timestamp", current_timestamp())
    .withColumn("source_api", lit("HAPI FHIR R4"))
    .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
)

display(encounter_bronze_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save Encounter records as a Bronze Delta table in the bronze schema
encounter_bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze.Encounter")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read all Raw Observation Bundle files
observation_bundle_df = spark.read.option("multiline", "true").json(
    get_raw_files("Observation")
)

# Extract individual Observation records
observation_bronze_df = observation_bundle_df.select(
    explode("entry").alias("entry")
).select("entry.resource.*")

display(observation_bronze_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add Bronze ingestion metadata to Observation records
observation_bronze_df = (
    observation_bronze_df
    .withColumn("extraction_timestamp", current_timestamp())
    .withColumn("source_api", lit("HAPI FHIR R4"))
    .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
)

display(observation_bronze_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save Observation records as a Bronze Delta table in the bronze schema
# overwriteSchema allows the Delta table schema to be updated if needed

observation_bronze_df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("bronze.Observation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read all Raw Condition Bundle files
condition_bundle_df = spark.read.option("multiline", "true").json(
    get_raw_files("Condition")
)

# Extract individual Condition records
condition_bronze_df = condition_bundle_df.select(
    explode("entry").alias("entry")
).select("entry.resource.*")

display(condition_bronze_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

condition_bronze_df = (
    condition_bronze_df
    .withColumn("extraction_timestamp", current_timestamp())
    .withColumn("source_api", lit("HAPI FHIR R4"))
    .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
)

display(condition_bronze_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save Condition records as a Bronze Delta table in the bronze schema
condition_bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze.Condition")

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
