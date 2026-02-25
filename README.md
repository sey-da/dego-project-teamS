# dego-project-teamS
DEGO Course Project — TeamS -– Credit Application Governance Analysis
## Team Members
- Sengul Seyda Yilmaz
## Project Description
Credit scoring bias analysis for DEGO course .
## Structure
- `data/` – Dataset files
- `notebooks/` – Jupyter analysis notebooks
- `src/` – Python source code
- `reports/` – Final deliverables
------------------------------------------------------------------
------------------------------------------------------------------
- ## Data Engineering Pipeline

This project implements a layered data engineering pipeline using Databricks and PySpark.

### Architecture
Bronze → Silver → Gold

### Notebook Structure

```text
notebooks/
    bronze/
        01_data_ingestion.py
    silver/
        02_data_cleaning.py
    pipeline/
        00_pipeline_runner.py

### Pipeline Logic
- 01_data_ingestion.py loads raw JSON data into Bronze layer.
- 02_data_cleaning.py standardizes, validates and cleans data into Silver layer.
- 00_pipeline_runner.py orchestrates ingestion and cleaning sequentially.
