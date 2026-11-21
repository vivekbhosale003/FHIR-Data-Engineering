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

# # Silver Layer – FHIR Data Engineering
# 
# **Purpose:**
# - Read data from Bronze Delta tables.
# - Clean and standardize FHIR resources.
# - Handle duplicate records and null values.
# - Apply data quality checks.
# - Transform data into structured Silver tables.
# 
# **Source:** Bronze Delta tables in `FHIR_Lakehouse`  
# **Target:** Silver Delta tables  
# **Resources:** Patient, Encounter, Observation, Condition

# CELL ********************

# Cell 1: Silver setup

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

resources = ["Patient", "Encounter", "Observation", "Condition"]

print("Silver setup completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Clean and flatten Patient data

df = spark.table("bronze.Patient")

patient_clean = df.select(
    col("id"),
    col("active"),
    lower(col("gender")).alias("gender"),
    to_date(col("birthDate")).alias("birthDate"),
    col("name")[0]["family"].alias("family_name"),
    col("name")[0]["given"][0].alias("given_name"),
    col("telecom")[0]["value"].alias("contact"),
    col("address")[0]["city"].alias("city"),
    col("address")[0]["state"].alias("state"),
    col("address")[0]["country"].alias("country"),
    col("meta.lastUpdated").alias("source_last_updated"),
    col("extraction_timestamp"),
    col("source_api"),
    col("batch_id")
).filter(col("id").isNotNull())

# Deduplicate using the latest source update
window_spec = Window.partitionBy("id").orderBy(
    col("source_last_updated").desc_nulls_last(),
    col("extraction_timestamp").desc_nulls_last()
)

patient_clean = (
    patient_clean
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print("Patient cleaning completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Clean and flatten Encounter data

df = spark.table("bronze.Encounter")

encounter_clean = df.select(
    col("id"),
    col("status"),
    col("class.code").alias("encounter_class"),
    col("subject.reference").alias("patient_reference"),
    col("period.start").alias("period_start"),
    col("period.end").alias("period_end"),
    col("type")[0]["coding"][0]["code"].alias("encounter_type_code"),
    col("type")[0]["coding"][0]["display"].alias("encounter_type"),
    col("serviceProvider.reference").alias("service_provider"),
    col("meta.lastUpdated").alias("source_last_updated"),
    col("extraction_timestamp"),
    col("source_api"),
    col("batch_id")
).filter(col("id").isNotNull())

# Deduplicate using latest source update
window_spec = Window.partitionBy("id").orderBy(
    col("source_last_updated").desc_nulls_last(),
    col("extraction_timestamp").desc_nulls_last()
)

encounter_clean = (
    encounter_clean
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print("Encounter cleaning completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Clean and flatten Observation data

df = spark.table("bronze.Observation")

observation_clean = df.select(
    col("id"),
    col("status"),
    col("subject.reference").alias("patient_reference"),
    col("category")[0]["coding"][0]["code"].alias("category_code"),
    col("category")[0]["coding"][0]["display"].alias("category"),
    col("code")["coding"][0]["code"].alias("observation_code"),
    col("code")["coding"][0]["display"].alias("observation_name"),
    col("valueQuantity")["value"].alias("value"),
    col("valueQuantity")["unit"].alias("unit"),
    col("valueQuantity")["code"].alias("unit_code"),
    col("effectiveDateTime").alias("effective_datetime"),
    col("meta.lastUpdated").alias("source_last_updated"),
    col("extraction_timestamp"),
    col("source_api"),
    col("batch_id")
).filter(col("id").isNotNull())

# Deduplicate using latest source update
window_spec = Window.partitionBy("id").orderBy(
    col("source_last_updated").desc_nulls_last(),
    col("extraction_timestamp").desc_nulls_last()
)

observation_clean = (
    observation_clean
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print("Observation cleaning completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Clean and flatten Condition data

df = spark.table("bronze.Condition")

condition_clean = df.select(
    col("id"),
    col("subject.reference").alias("patient_reference"),
    col("clinicalStatus.coding")[0]["code"].alias("clinical_status"),
    col("verificationStatus.coding")[0]["code"].alias("verification_status"),
    col("category")[0]["coding"][0]["code"].alias("condition_category"),
    col("code")["coding"][0]["code"].alias("condition_code"),
    col("code")["coding"][0]["display"].alias("condition_name"),
    col("onsetDateTime").alias("onset_datetime"),
    col("recordedDate").alias("recorded_date"),
    col("meta.lastUpdated").alias("source_last_updated"),
    col("extraction_timestamp"),
    col("source_api"),
    col("batch_id")
).filter(col("id").isNotNull())

# Deduplicate using latest source update
window_spec = Window.partitionBy("id").orderBy(
    col("source_last_updated").desc_nulls_last(),
    col("extraction_timestamp").desc_nulls_last()
)

condition_clean = (
    condition_clean
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print("Condition cleaning completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Prepare cleaned resources

silver_input = {
    "Patient": patient_clean,
    "Encounter": encounter_clean,
    "Observation": observation_clean,
    "Condition": condition_clean
}

print("All cleaned resources prepared.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7: Reusable SCD Type 2 function

def apply_scd2(df, resource):

    target_table = f"silver.{resource}"

    # Exclude ingestion metadata from change detection
    exclude_cols = ["extraction_timestamp", "source_api", "batch_id"]

    business_cols = [c for c in df.columns if c not in exclude_cols]

    # Generate hash to identify data changes
    df = df.withColumn(
        "record_hash",
        sha2(to_json(struct(*business_cols)), 256)
    )

    # Initial load
    if not spark.catalog.tableExists(target_table):

        initial_df = (
            df.withColumn("effective_start_date", current_timestamp())
              .withColumn("effective_end_date", lit(None).cast("timestamp"))
              .withColumn("is_current", lit(True))
              .withColumn("scd_version", lit(1))
        )

        initial_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

    else:

        target = DeltaTable.forName(spark, target_table)

        # Expire current records if their data has changed
        target.alias("t").merge(
            df.select("id", "record_hash").alias("s"),
            "t.id = s.id AND t.is_current = true"
        ).whenMatchedUpdate(
            condition="t.record_hash <> s.record_hash",
            set={
                "is_current": "false",
                "effective_end_date": "current_timestamp()"
            }
        ).execute()

        # Identify new records and changed records
        current_ids = (
            spark.table(target_table)
            .filter(col("is_current") == True)
            .select("id")
        )

        new_records = df.join(current_ids, "id", "left_anti")

        # Determine next SCD version
        versions = (
            spark.table(target_table)
            .groupBy("id")
            .agg(max("scd_version").alias("max_version"))
        )

        new_records = (
            new_records.join(versions, "id", "left")
            .withColumn(
                "scd_version",
                coalesce(col("max_version"), lit(0)) + 1
            )
            .drop("max_version")
            .withColumn("effective_start_date", current_timestamp())
            .withColumn("effective_end_date", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
        )

        # Insert new versions
        if new_records.limit(1).count() > 0:
            new_records.write.format("delta").mode("append").saveAsTable(target_table)

    print(f"SCD Type 2 completed: {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8: Apply SCD Type 2 to all resources

for resource in resources:
    apply_scd2(silver_input[resource], resource)

print("All Silver SCD Type 2 tables created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Silver Patient table schema

spark.table("silver.Patient").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
