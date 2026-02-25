# Databricks notebook source
# 02 - Data Cleaning & Standardization

# ============================================================
# 02 – DATA CLEANING & STANDARDIZATION (GOLD LAYER PREPARATION)
# ============================================================
# Purpose:
# - Load the curated Silver-layer dataset for further refinement.
# - Prepare the dataset for transformation, validation, and governance steps.
#
# Why this step is necessary:
# - The Silver layer contains structured and deduplicated data.
# - Data Cleaning ensures consistency, standardization, and privacy compliance.
# - This stage prepares the dataset for analytical consumption (Gold layer).
#
# Output:
# - apps_clean: working DataFrame used for cleaning, standardization,
#   and governance transformations before Gold storage.
# ============================================================

from pyspark.sql import functions as F

apps_clean = spark.table("applications_silver")

display(apps_clean)
apps_clean.printSchema()

# COMMAND ----------

# Quick sanity checks
print("rows:", apps_clean.count())
print("columns:", len(apps_clean.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC Quick Sanity Checks
# MAGIC
# MAGIC At the beginning of the data cleaning phase, basic integrity checks were performed to validate that the dataset was correctly loaded from the Silver layer and is structurally consistent.
# MAGIC
# MAGIC The following validations were conducted:
# MAGIC 	•	Total number of rows
# MAGIC 	•	Total number of columns
# MAGIC
# MAGIC Results:
# MAGIC 	•	Total records: 500
# MAGIC 	•	Total columns: 16
# MAGIC
# MAGIC These checks ensure that no unintended data loss occurred during the ingestion and transformation steps. They also provide a baseline reference to compare against later stages of the cleaning process, particularly after standardization, masking, or column removal operations.

# COMMAND ----------

# ============================================================
# 2.1 GENDER STANDARDIZATION
# ============================================================
# Purpose:
# - Normalize inconsistent gender values.
# - Ensure consistent categorical representation.
#
# Why this is necessary:
# - Raw data contains variations such as "M", "Male", "F", "Female".
# - Inconsistent categories reduce analytical accuracy.
# - Standardization improves data quality and aggregation reliability.
#
# Output:
# - gender_standardized column with values: Male, Female, Unknown.
# ============================================================
from pyspark.sql import functions as F

apps_clean = apps_clean.withColumn(
    "gender_standardized",
    F.when(F.lower(F.col("gender")).isin("m", "male"), "Male")
     .when(F.lower(F.col("gender")).isin("f", "female"), "Female")
     .otherwise("Unknown")
)

display(apps_clean.select("gender", "gender_standardized"))

# COMMAND ----------

# ============================================================
# 2.2 DATE OF BIRTH STANDARDIZATION
# ============================================================
# Purpose:
# - Convert inconsistent date_of_birth formats into a unified date type.
# - Create a structured and analytics-ready date column.
#
# Why this is necessary:
# - Raw data contains multiple date formats such as:
#   yyyy-MM-dd, dd/MM/yyyy, yyyy/MM/dd, MM/dd/yyyy.
# - Inconsistent date formats prevent proper filtering,
#   time-based analysis, and age calculations.
# - Converting to Spark DateType improves reliability and usability.
#
# Method:
# - Use try_to_date with multiple patterns.
# - Apply coalesce to select the first successfully parsed format.
#
# Output:
# - dob_parsed column of type Date.
# - Invalid or unparseable values remain null for further validation.
# ============================================================
from pyspark.sql import functions as F

apps_clean = apps_clean.withColumn(
    "dob_parsed",
    F.coalesce(
        F.expr("try_to_date(date_of_birth, 'yyyy-MM-dd')"),
        F.expr("try_to_date(date_of_birth, 'dd/MM/yyyy')"),
        F.expr("try_to_date(date_of_birth, 'yyyy/MM/dd')"),
        F.expr("try_to_date(date_of_birth, 'MM/dd/yyyy')")
    )
)

display(apps_clean.select("date_of_birth", "dob_parsed").limit(20))

# COMMAND ----------

# ============================================================
# Schema Verification
# ============================================================
# Purpose:
# - Verify column data types after cleaning transformations.
# - Ensure date_of_birth is converted to DateType (dob_parsed).
# - Confirm numeric fields remain numeric.
# - Validate that no unintended schema changes occurred.
#
# Why this is important:
# - Prevents downstream failures.
# - Ensures compatibility with Gold layer storage.
# - Supports data consistency and governance validation.
# ============================================================

apps_clean.printSchema()

# COMMAND ----------

# ============================================================
# 2.3 – Email Format Validation
# ============================================================
# Purpose:
# - Validate the structure of email addresses using regex.
# - Flag missing or invalid emails.
#
# Logic:
# - Null or empty emails → marked as False.
# - Emails matching standard pattern → marked as True.
#
# Why this is important:
# - Ensures contact information quality.
# - Supports data completeness and validity checks.
# - Prevents downstream analytics errors.
# ============================================================
from pyspark.sql import functions as F

apps_clean = apps_clean.withColumn(
    "email_valid",
    F.when(F.col("email").isNull() | (F.trim(F.col("email")) == ""), F.lit(False))
     .otherwise(F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"))
)

display(apps_clean.select("email", "email_valid").limit(20))

# COMMAND ----------

# ============================================================
# 2.4 – Post-Validation Quality Check (Email)
# ============================================================
# Purpose:
# - Verify dataset size after cleaning.
# Count total rows and invalid emails after validation
#
# Why this is important:
# - Ensures no unintended row loss during transformations.
# - Measures data validity for contact information.
# - Supports data quality reporting.
# ============================================================


apps_clean.select(
    
    # 1. Count all rows in the dataset
    # This confirms we did not accidentally lose records during cleaning
    F.count("*").alias("total_rows"),
    
    # 2. Count rows where email_valid is False
    # This tells us how many emails are either invalid or missing
    F.count(
        F.when(
            F.col("email_valid") == False,  # condition: email failed validation
            True                            # count this row if condition is met
        )
    ).alias("invalid_or_missing_email")

).display()

# COMMAND ----------

# ============================================================
# Step 4 - Mask Sensitive Data (PII) and Minimize Exposure
# ============================================================
# Purpose:
# - Protect personally identifiable information (PII) by pseudonymizing
#   sensitive fields using SHA-256 hashing.
# - Create hashed surrogate columns for SSN, IP address, email, and full name.
#
# Why this step is necessary:
# - Reduces privacy risk and supports data governance requirements.
# - Prevents raw PII from being propagated to downstream layers (Gold, reporting).
# - Retains analytical usability through consistent hashed identifiers.
#
# Output:
# - New columns: ssn_hash, ip_hash, email_hash, name_hash
# - Original PII columns removed: ssn, ip_address, email, full_name
# ============================================================

apps_clean = (
    apps_clean
    .withColumn("ssn_hash", F.sha2(F.col("ssn"), 256))
    .withColumn("ip_hash", F.sha2(F.col("ip_address"), 256))
    .withColumn("email_hash", F.sha2(F.lower(F.col("email")), 256))
    .withColumn("name_hash", F.sha2(F.lower(F.col("full_name")), 256))
)

apps_clean = apps_clean.drop("ssn", "ip_address", "email", "full_name")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PII Handling and Data Governance Controls
# MAGIC
# MAGIC The dataset contains personally identifiable information (PII), including SSN, IP address, email, and full name. These attributes present privacy and regulatory risks if exposed in analytical layers.
# MAGIC
# MAGIC To comply with data governance and privacy best practices, sensitive fields are pseudonymized using SHA-256 hashing. This ensures that raw PII values are not propagated to downstream layers such as Gold tables, reporting datasets, or analytical environments.
# MAGIC
# MAGIC After generating hashed equivalents, original PII columns are removed to minimize exposure and enforce data minimization principles.
# MAGIC
# MAGIC This approach:
# MAGIC - Reduces re-identification risk  
# MAGIC - Supports privacy-by-design principles  
# MAGIC - Maintains analytical usability through consistent hashed identifiers  
# MAGIC - Aligns with governance and compliance standards  

# COMMAND ----------

# 2.6 – Post-Transformation Row Count Validation
# ------------------------------------------------
# Purpose:
# - Recalculate total row count after cleaning and PII masking.
# - Ensure that no unintended data loss occurred during transformations.
#
# Why this is important:
# - Hashing and column drops should not change the number of records.
# - Confirms structural integrity of the dataset.
# - Supports pipeline reliability and auditability.
#
# Expected outcome:
# - total_rows should remain equal to the Silver layer row count.
apps_clean.select(
    F.count("*").alias("total_rows")
).display()

# COMMAND ----------

# 2.7 – Critical Field Null Analysis
# -----------------------------------
# Purpose:
# - Assess completeness of key analytical fields after cleaning.
# - Quantify remaining null values in transformed and numeric columns.
#
# Fields evaluated:
# - dob_parsed        → validated date field
# - annual_income     → core financial metric
# - loan_approved     → target decision variable
#
# Why this is important:
# - Ensures cleaning did not introduce unexpected nulls.
# - Validates readiness for downstream analytics or modeling.
# - Supports data quality monitoring and governance controls.
#
# Method:
# - Count total rows.
# - Sum null occurrences per column using boolean-to-integer casting.

from pyspark.sql import functions as F

apps_clean.select(
    F.count("*").alias("total_rows"),

    F.sum(F.col("dob_parsed").isNull().cast("int")).alias("null_dob_parsed"),
    F.sum(F.col("annual_income").isNull().cast("int")).alias("null_annual_income"),
    F.sum(F.col("loan_approved").isNull().cast("int")).alias("null_loan_approved")
).display()

# COMMAND ----------

# 2.8 – Post-PII Removal Integrity Check
# ---------------------------------------
# Purpose:
# - Verify that hashed PII columns were successfully created.
# - Confirm original sensitive fields were removed.
# - Ensure no unintended structural changes occurred.
#
# Why this is important:
# - Validates proper pseudonymization of sensitive attributes.
# - Confirms governance compliance before publishing Gold layer.
# - Ensures analytical usability is preserved after masking.
#
# Method:
# - Display sample hashed columns.
# - Review final column list of the dataset.

apps_clean.select(
    "ssn_hash",
    "email_hash",
    "ip_hash",
    "name_hash"
).limit(5).display()

apps_clean.columns

# COMMAND ----------


# 2.9 – Final Schema Verification
# --------------------------------
# Purpose:
# - Confirm final structure and data types after all cleaning steps.
# - Ensure transformations did not unintentionally alter column types.
# - Validate readiness for Gold layer storage.
#
# Why this is important:
# - Guarantees schema consistency.
# - Prevents downstream merge or type conflicts.
# - Supports data governance and quality assurance.

apps_clean.printSchema()

# COMMAND ----------

# 2.10 – Detect Potential Type Issues (String vs Numeric)
# -------------------------------------------------------
# Purpose:
# - Identify columns currently stored as string data type.
# - Ensure no numeric fields were accidentally stored as string during transformations.
#
# Why this is important:
# - Prevents aggregation and calculation errors.
# - Ensures numeric columns remain suitable for analytics and modeling.
# - Supports schema integrity before writing to Gold layer.

string_columns = [c for c, t in apps_clean.dtypes if t == "string"]
string_columns

# COMMAND ----------

# 2.11 – Persist Cleaned Data to Gold Layer
# ------------------------------------------
# Purpose:
# - Store the fully cleaned and standardized dataset in the Gold layer.
# - Ensure the final analytical dataset is consistent and schema-aligned.
#
# Why this step is necessary:
# - Prevents schema merge conflicts by dropping the existing table.
# - Ensures the Gold table reflects the latest transformations.
# - Provides a curated dataset ready for analytics, reporting, or modeling.
#
# Output:
# - applications_gold: finalized Delta table containing cleaned and governed data.

spark.sql("DROP TABLE IF EXISTS applications_gold")

# Write fresh Gold table
apps_clean.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("applications_gold")