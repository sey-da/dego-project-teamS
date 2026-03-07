# dego-project-teamS
DEGO Course Project — TeamS -– Credit Application Governance Analysis
## Team Members
- Sengul Seyda Yilmaz
## Project Description
Data quality evaluation and bias analysis for a credit approval dataset, developed as part of the DEGO course.
## Structure
- `data/` – Dataset files
- `notebooks/` – Jupyter analysis notebooks
- `src/` – Python source code
- `reports/` – Final deliverables
------------------------------------------------------------------
------------------------------------------------------------------
```
project-teamS/
│
├── README.md
│
├── data/
│   └── novacred_applications.json   (optional)
│
├── notebooks/
│   ├── 01-data-quality.ipynb
│   └── 02-bias-analysis.ipynb
│
└── src/
    ├── 00_pipeline_runner.py
    ├── 01_data_ingestion.py
    └── 02_data_cleaning.py
```
------------------------------------------------------------------
------------------------------------------------------------------

# Credit Approval Data Quality and Bias Analysis

## Project Overview

This project evaluates data quality and potential bias in a credit approval dataset through a structured analytical workflow. The objective is to ensure that the dataset is reliable before assessing fairness in approval decisions.

The repository follows a clear separation between data validation and bias evaluation, reflecting a common practice in data engineering and data science workflows. By validating the dataset first, the analysis ensures that fairness metrics are applied to data that has been properly examined for reliability and structural integrity.

## Analytical Workflow

The project follows a structured pipeline that prepares the dataset before performing fairness evaluation.

Raw Data → Ingestion → Data Quality Assessment → Cleaning & Validation → Structured Dataset → Bias Analysis

- **Raw Data**  
  Credit application records provided in JSON format.

- **Ingestion**  
  The dataset is loaded and inspected to understand its schema and structure.

- **Data Quality Assessment**  
  The dataset is evaluated across multiple quality dimensions to detect potential issues.

- **Cleaning and Validation**  
  Missing values, duplicates, and structural inconsistencies are examined and addressed.

- **Structured Dataset**  
  A validated dataset is prepared for analytical processing.

- **Bias Analysis**  
  Fairness metrics are applied to detect potential disparities in approval outcomes.

## Notebooks

### 01-data-quality.ipynb

This notebook performs a comprehensive data quality evaluation using widely accepted quality dimensions:

- completeness  
- uniqueness  
- validity  
- consistency  
- accuracy  

The analysis identifies potential issues such as missing values, duplicate records, and structural inconsistencies, ensuring the dataset is suitable for downstream analysis.

### 02-bias-analysis.ipynb

This notebook evaluates potential bias in credit approval outcomes.

The analysis investigates several sources of bias commonly discussed in responsible data analysis, including:

- historical bias reflected in approval outcomes  
- selection bias related to dataset representation  
- disparate impact across gender groups using the Disparate Impact ratio  
- potential proxy variables such as income level and ZIP code that may indirectly encode demographic characteristics  

Group level fairness is primarily assessed by calculating the **Disparate Impact ratio for gender**, and the results are interpreted using the **80 percent rule**, a commonly used guideline for identifying potential adverse impact.
