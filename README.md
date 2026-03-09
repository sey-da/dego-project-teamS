# dego-project-teamS
DEGO Course Project — TeamS -– Credit Application Governance Analysis
## Author
| Name | Role | Contributions |
|-----|-----|-----|
| Sengul Seyda Yilmaz | Data Engineer | Data quality assessment notebook, data validation, duplicate detection, data cleaning, and preparation of the dataset for downstream analysis |
| Sengul Seyda Yilmaz | Data Scientist | Bias analysis notebook, fairness evaluation using the Disparate Impact ratio and the 80 percent rule, interpretation of bias metrics and documentation of key findings |

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
│   ├── raw_credit_applications.json
│   └── credit_applications_clean_sample.csv   # optional
│
├── notebooks/
│   ├── 01-data-quality.ipynb
│   └── 02-bias-analysis.ipynb
│
├── reports/   # optional
│   └── Data_Quality_and_Bias_Analysis_Report.pdf
│
└── src/       # optional
```
------------------------------------------------------------------
------------------------------------------------------------------

# Credit Approval Data Quality and Bias Analysis

## Project Overview

This project evaluates data quality and potential bias in a credit approval dataset through a structured analytical workflow. The objective is to ensure that the dataset is reliable before assessing fairness in approval decisions.

The repository follows a clear separation between data validation and bias evaluation, reflecting a common practice in data engineering and data science workflows. By validating the dataset first, the analysis ensures that fairness metrics are applied to data that has been properly examined for reliability and structural integrity.
## Dataset

The dataset contains credit application records used to evaluate loan approval decisions. The data includes demographic attributes, financial indicators, and approval outcomes.

Key attributes used in the analysis include:

- gender
- annual_income
- date_of_birth
- zip_code
- email
- loan_approved

The dataset is provided in JSON format and contains nested structures that require preprocessing before analysis.

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

## How to Run the Analysis

1. Open the notebooks in a Python environment or Databricks workspace.
2. Run **01-data-quality.ipynb** to evaluate dataset quality and identify issues.
3. Run **02-bias-analysis.ipynb** to compute fairness metrics and evaluate potential bias in approval outcomes.

4. ## Key Outputs

The project produces the following outputs:

- Data quality assessment results
- Identified data quality issues and remediation strategies
- Approval rate comparison across gender groups
- Disparate Impact calculation
- Discussion of potential bias and governance considerations
