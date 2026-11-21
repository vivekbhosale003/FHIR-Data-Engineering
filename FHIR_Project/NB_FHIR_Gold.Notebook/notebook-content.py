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

# Cell 1: Gold setup

from pyspark.sql.functions import *

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

resources = ["Patient", "Encounter", "Observation", "Condition"]

print("Gold setup completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Create Gold tables from current Silver records

for resource in resources:

    df = spark.table(f"silver.{resource}").filter(col("is_current") == True)

    df.write.format("delta").mode("overwrite").saveAsTable(f"gold.{resource}")

    print(f"Gold table created: gold.{resource}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Patient Gender Summary View

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_Patient_Gender_Summary AS

SELECT
    COALESCE(gender, 'Unknown') AS gender,
    COUNT(*) AS patient_count

FROM gold.Patient

GROUP BY COALESCE(gender, 'Unknown')
""")

print("Patient Gender Summary view created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Encounter Status Summary View

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_Encounter_Status_Summary AS

SELECT
    COALESCE(status, 'Unknown') AS encounter_status,
    COUNT(*) AS encounter_count

FROM gold.Encounter

GROUP BY COALESCE(status, 'Unknown')
""")

print("Encounter Status Summary view created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Monthly Encounter Summary View

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_Encounter_Monthly AS

SELECT
    YEAR(period_start) AS encounter_year,
    MONTH(period_start) AS encounter_month,
    COUNT(*) AS encounter_count

FROM gold.Encounter

WHERE period_start IS NOT NULL

GROUP BY
    YEAR(period_start),
    MONTH(period_start)

ORDER BY
    encounter_year,
    encounter_month
""")

print("Monthly Encounter Summary view created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Observation Category Summary View

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_Observation_Category_Summary AS

SELECT
    COALESCE(category, 'Unknown') AS observation_category,
    COUNT(*) AS observation_count

FROM gold.Observation

GROUP BY COALESCE(category, 'Unknown')
""")

print("Observation Category Summary view created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7: Condition Status Summary View

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_Condition_Status_Summary AS

SELECT
    COALESCE(clinical_status, 'Unknown') AS clinical_status,
    COUNT(*) AS condition_count

FROM gold.Condition

GROUP BY COALESCE(clinical_status, 'Unknown')
""")

print("Condition Status Summary view created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8: Patient Clinical Overview View

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_Patient_Clinical_Overview AS

SELECT
    p.id AS patient_id,
    p.given_name,
    p.family_name,
    p.gender,
    p.birthDate,
    p.city,
    p.state,
    p.country,

    e.id AS encounter_id,
    e.status AS encounter_status,
    e.encounter_class,
    e.period_start,
    e.period_end,

    c.id AS condition_id,
    c.condition_name,
    c.condition_code,
    c.clinical_status

FROM gold.Patient p

LEFT JOIN gold.Encounter e
    ON p.id = REPLACE(e.patient_reference, 'Patient/', '')

LEFT JOIN gold.Condition c
    ON p.id = REPLACE(c.patient_reference, 'Patient/', '')
""")

print("Patient Clinical Overview view created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 9: Patient Observation Analysis View

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_Patient_Observation_Analysis AS

SELECT
    p.id AS patient_id,
    p.given_name,
    p.family_name,
    p.gender,

    o.id AS observation_id,
    o.observation_name,
    o.observation_code,
    o.category,
    o.value,
    o.unit,
    o.effective_datetime,
    o.status AS observation_status

FROM gold.Patient p

LEFT JOIN gold.Observation o
    ON p.id = REPLACE(o.patient_reference, 'Patient/', '')
""")

print("Patient Observation Analysis view created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 10: Verify Gold views

views = [
    "vw_Patient_Gender_Summary",
    "vw_Encounter_Status_Summary",
    "vw_Encounter_Monthly",
    "vw_Observation_Category_Summary",
    "vw_Condition_Status_Summary",
    "vw_Patient_Clinical_Overview",
    "vw_Patient_Observation_Analysis"
]

for view in views:
    print(f"\n--- {view} ---")
    spark.sql(f"SELECT * FROM gold.{view}").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 11: Gold relationship and row count check

for resource in resources:
    print(f"{resource}: {spark.table(f'gold.{resource}').count()} rows")

print("\nPatient-Encounter matches:")
spark.sql("""
SELECT COUNT(*) AS matched_records
FROM gold.Patient p
INNER JOIN gold.Encounter e
    ON p.id = REPLACE(e.patient_reference, 'Patient/', '')
""").show()

print("\nPatient-Observation matches:")
spark.sql("""
SELECT COUNT(*) AS matched_records
FROM gold.Patient p
INNER JOIN gold.Observation o
    ON p.id = REPLACE(o.patient_reference, 'Patient/', '')
""").show()

print("\nPatient-Condition matches:")
spark.sql("""
SELECT COUNT(*) AS matched_records
FROM gold.Patient p
INNER JOIN gold.Condition c
    ON p.id = REPLACE(c.patient_reference, 'Patient/', '')
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 12: Inspect actual Patient references

print("Patient IDs:")
spark.sql("""
SELECT id
FROM gold.Patient
LIMIT 10
""").show(truncate=False)

print("Encounter patient references:")
spark.sql("""
SELECT DISTINCT patient_reference
FROM gold.Encounter
WHERE patient_reference IS NOT NULL
LIMIT 10
""").show(truncate=False)

print("Observation patient references:")
spark.sql("""
SELECT DISTINCT patient_reference
FROM gold.Observation
WHERE patient_reference IS NOT NULL
LIMIT 10
""").show(truncate=False)

print("Condition patient references:")
spark.sql("""
SELECT DISTINCT patient_reference
FROM gold.Condition
WHERE patient_reference IS NOT NULL
LIMIT 10
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 13: Identify unmatched patient references

for resource in ["Encounter", "Observation", "Condition"]:

    print(f"\nUnmatched Patient references in {resource}:")

    spark.sql(f"""
    SELECT DISTINCT
        REPLACE(patient_reference, 'Patient/', '') AS patient_id
    FROM gold.{resource}
    WHERE patient_reference IS NOT NULL
      AND REPLACE(patient_reference, 'Patient/', '')
          NOT IN (SELECT id FROM gold.Patient)
    LIMIT 10
    """).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 14: Patient Reference Data Quality Summary

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_Patient_Reference_Quality AS

SELECT
    'Encounter' AS resource_name,
    COUNT(*) AS total_records,
    COUNT(p.id) AS matched_patient_records,
    COUNT(*) - COUNT(p.id) AS unmatched_patient_records

FROM gold.Encounter e
LEFT JOIN gold.Patient p
    ON p.id = REPLACE(e.patient_reference, 'Patient/', '')

UNION ALL

SELECT
    'Observation',
    COUNT(*),
    COUNT(p.id),
    COUNT(*) - COUNT(p.id)

FROM gold.Observation o
LEFT JOIN gold.Patient p
    ON p.id = REPLACE(o.patient_reference, 'Patient/', '')

UNION ALL

SELECT
    'Condition',
    COUNT(*),
    COUNT(p.id),
    COUNT(*) - COUNT(p.id)

FROM gold.Condition c
LEFT JOIN gold.Patient p
    ON p.id = REPLACE(c.patient_reference, 'Patient/', '')
""")

print("Patient Reference Quality View created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 15: Final Gold verification

gold_views = [
    "vw_Patient_Gender_Summary",
    "vw_Encounter_Status_Summary",
    "vw_Encounter_Monthly",
    "vw_Observation_Category_Summary",
    "vw_Condition_Status_Summary",
    "vw_Patient_Clinical_Overview",
    "vw_Patient_Observation_Analysis",
    "vw_Patient_Reference_Quality"
]

for view in gold_views:
    print(f"\n--- {view} ---")
    spark.sql(f"SELECT * FROM gold.{view}").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
