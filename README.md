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

```
## Project Overview

This project follows a structured data engineering and fairness evaluation workflow.

The data engineering layer is implemented inside the `src/` directory and includes modular pipeline components for data ingestion, cleaning, and transformation. These scripts represent the reproducible backend processing logic that ensures data quality, structural consistency, and analytical reliability.

The analytical findings and methodological explanations are documented inside the `notebooks/` directory:

- `01-data-quality.ipynb` presents the comprehensive data quality assessment across completeness, uniqueness, validity, consistency, and accuracy dimensions, including remediation steps.
- `02-bias-analysis.ipynb` calculates and interprets the Disparate Impact ratio for gender, applying the 80 percent rule to assess potential group-level bias.

The separation between engineering logic and analytical documentation ensures clarity, reproducibility, and professional project organization.
```
