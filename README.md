FHIR Data Engineering Project – Microsoft Fabric

Project Overview

This project demonstrates an end-to-end healthcare data engineering solution using Microsoft Fabric. Healthcare data is ingested from a public FHIR R4 API, processed through a Medallion Architecture (Bronze, Silver, Gold), and made available for analytics through a Power BI Direct Lake report.

The project demonstrates API ingestion, data transformation, Delta Lake, SCD Type 2, pipeline orchestration, and reporting.

Architecture

Source API → Raw → Bronze → Silver → Gold → Power BI

Source: HAPI FHIR R4 Public API

Orchestration: Microsoft Fabric Data Pipeline

Storage: Fabric Lakehouse / Delta Tables

Analytics: Power BI Direct Lake

Data Source

Public FHIR R4 API:

https://hapi.fhir.org/baseR4

The project extracts the following FHIR resource types:

Patient

Encounter

Observation

Condition

Medallion Architecture

Raw Layer

Extracts data from the FHIR API.

Handles API pagination.

Stores raw JSON files in the Lakehouse.

Maintains raw data for downstream processing.

Bronze Layer

Loads raw JSON data into Delta tables.

Preserves source-level data structures.

Creates Bronze tables for each resource.

Silver Layer

Flattens nested FHIR structures.

Cleans and standardizes data.

Removes duplicate records.

Implements SCD Type 2 for historical tracking.

Maintains record hash and effective date metadata.

Gold Layer

Stores analytical-ready tables.

Uses current records from Silver.

Creates datasets for reporting and analytics.

Gold tables:

gold.Patient

gold.Encounter

gold.Observation

gold.Condition

Pipeline Orchestration

The end-to-end pipeline is implemented using Microsoft Fabric Data Pipelines.

Pipeline: PL_FHIR_EndToEnd

Pipeline execution flow:

Raw API Extraction

Bronze Processing

Silver Processing

Gold Processing

Semantic Model Refresh

Each processing stage is orchestrated sequentially, with downstream execution dependent on successful completion of the preceding stage.

Power BI Dashboard

The project includes a single-page healthcare analytics dashboard built using Power BI and a Direct Lake semantic model.

Dashboard metrics and visuals include:

Total Patients

Total Encounters

Total Observations

Total Conditions

Patients by Gender

Encounters by Status

Observations by Category

Conditions by Clinical Status

Patient Details

Data Quality Observations

The public FHIR API contains independently generated resource records, which can result in patient references that do not match records in the Patient dataset.

The project identifies and retains these data quality observations rather than artificially forcing reference matches.

This demonstrates real-world data quality assessment and handling of source-system inconsistencies.

Technology Stack

Microsoft Fabric

Fabric Data Factory / Data Pipelines

Fabric Notebooks

Lakehouse

Delta Lake

PySpark

Python

FHIR R4 API

Power BI

Direct Lake

SCD Type 2

Medallion Architecture

Project Objective

To build a practical end-to-end data engineering pipeline that demonstrates data ingestion, transformation, historical data tracking, orchestration, analytics-ready data preparation, and dashboard reporting using Microsoft Fabric.

Author: Vivek Bhosale
GitHub: https://github.com/vivekbhosale003/FHIR-Data-Engineering
