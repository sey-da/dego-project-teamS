# Databricks notebook source

# ============================================================
# 1. DATA INGESTION – SCHEMA ENFORCEMENT (BRONZE → STRUCTURED)
# ============================================================
# Purpose:
# - Load the raw nested JSON credit application dataset.
# - Enforce an explicit schema instead of relying on schema inference.
# - Ensure full alignment with the project documentation (data types and structure).
#
# Why schema enforcement?
# - Prevents incorrect automatic type detection.
# - Guarantees annual_income, debt_to_income, savings_balance are numeric.
# - Preserves nested structure (applicant_info, financials, decision).
# - Improves data consistency and reliability.
#
# Output:
# - df: structured DataFrame with nested JSON schema correctly applied.
# ============================================================

from pyspark.sql.types import *

schema = StructType([
    StructField("_id", StringType(), True),

    StructField("applicant_info", StructType([
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("ssn", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("zip_code", StringType(), True)
    ]), True),

    StructField("financials", StructType([
        StructField("annual_income", DoubleType(), True),          # Number
        StructField("credit_history_months", IntegerType(), True), # Integer
        StructField("debt_to_income", DoubleType(), True),         # Number
        StructField("savings_balance", DoubleType(), True)         # Number
    ]), True),

    StructField("spending_behavior", ArrayType(
        StructType([
            StructField("category", StringType(), True),           # String
            StructField("amount", DoubleType(), True)              # Number
        ])
    ), True),

    StructField("decision", StructType([
        StructField("loan_approved", BooleanType(), True),         # Boolean
        StructField("interest_rate", DoubleType(), True),          # Number
        StructField("approved_amount", DoubleType(), True),        # Number
        StructField("rejection_reason", StringType(), True)        # String
    ]), True)
])

df = spark.read \
    .option("multiline", "true") \
    .schema(schema) \
    .json("/Volumes/workspace/default/raw_data/raw_credit_applications.json")

display(df)
df.printSchema()

# COMMAND ----------

# ============================================================
# 2. APPLICATION-LEVEL FLATTENING
# ============================================================
# Purpose:
# - Convert the nested JSON structure into a flat, structured table.
# - Extract fields from applicant_info, financials, and decision structs.
# - Create one row per application for analytical processing.
#
# Why this step is necessary:
# - Nested structs are not optimal for relational analysis.
# - Flattening enables easier querying, validation, and data quality checks.
# - Prepares the dataset for Silver-layer storage.
#
# Output:
# - applications: flat DataFrame with structured application-level attributes.
# ============================================================
from pyspark.sql.functions import col

applications = df.select(
    col("_id"),
    col("applicant_info.*"),
    col("financials.*"),
    col("decision.*")
)

display(applications)
applications.printSchema()

# COMMAND ----------

# ============================================================
# 3. DATA QUALITY CHECK – COMPLETENESS (annual_income)
# ============================================================
# Purpose:
# - Assess missing values in key financial attribute (annual_income).
# - Quantify the number and percentage of null values.
#
# Why this is important:
# - annual_income is a critical numeric variable for credit evaluation.
# - Missing values affect analytical reliability and model performance.
# - Supports the Completeness data quality dimension.
#
# Output:
# - Total number of records
# - Count of null annual_income values
# - Percentage of missing annual_income
# ============================================================
print(applications.columns)
from pyspark.sql import functions as F

applications.select(
    F.count("*").alias("total"),
    F.sum(F.col("annual_income").isNull().cast("int")).alias("annual_income_nulls"),
    (F.sum(F.col("annual_income").isNull().cast("int")) / F.count("*") * 100).alias("annual_income_null_pct")
).show()

# COMMAND ----------

# ============================================================
# 4. DATA QUALITY CHECK – UNIQUENESS (Duplicate _id Detection)
# ============================================================
# Purpose:
# - Verify that the primary key (_id) is unique for each application.
# - Identify duplicate application records.
#
# Why this is important:
# - _id represents the unique identifier of each loan application.
# - Duplicate IDs violate the Uniqueness and Consistency dimensions
#   of data quality.
# - Duplicates can lead to incorrect aggregations and biased analysis.
#
# Actions Taken:
# - Count total records.
# - Identify duplicate _id values.
# - Calculate duplicate percentage.
# - Remove duplicates by keeping one record per _id.
#
# Output:
# - applications_dedup: deduplicated application-level dataset.
# ============================================================

from pyspark.sql import functions as F

# Count total records
total_records = applications.count()

# Detect duplicate IDs
duplicate_ids = (
    applications
    .groupBy("_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_ids.count()

print("Total records:", total_records)
print("Duplicate _id count:", duplicate_count)
print("Duplicate percentage:", (duplicate_count / total_records) * 100)

duplicate_ids.show()

# Remove duplicates
applications_dedup = applications.dropDuplicates(["_id"])

# COMMAND ----------

# ============================================================
# 5. NORMALIZE SPENDING BEHAVIOR (ARRAY EXPANSION)
# ============================================================
# Purpose:
# - Transform the nested spending_behavior array into a relational table.
# - Convert one-to-many relationship (1 application → multiple spendings)
#   into a structured tabular format.
#
# Why this step is necessary:
# - Arrays are not suitable for relational analytics.
# - explode() creates one row per spending record.
# - Ensures proper normalization between applications and spending data.
#
# Output:
# - spending: table containing _id, spend_category, and spend_amount.
# - One application may produce multiple spending rows.
# ============================================================
from pyspark.sql.functions import explode

spending = df.select(
    col("_id"),
    explode("spending_behavior").alias("spend")
).select(
    col("_id"),
    col("spend.category").alias("spend_category"),
    col("spend.amount").alias("spend_amount")
)

display(spending)

# COMMAND ----------

# ============================================================
# 6. DATA QUALITY ASSESSMENT – COMPLETENESS CHECK
# ============================================================
# Purpose:
# - Evaluate completeness of critical applicant-level attributes.
# - Identify and quantify missing values in key fields.
#
# Fields Assessed:
# - email
# - date_of_birth
# - gender
#
# Data Quality Dimension:
# - Completeness
#
# Output:
# - Summary table containing total records and missing value counts.
# ============================================================
from pyspark.sql.functions import count, when, trim, col

dq = applications.select(
    count("*").alias("total_rows"),
    count(when(col("email").isNull() | (trim(col("email"))==""), True)).alias("missing_email"),
    count(when(col("date_of_birth").isNull() | (trim(col("date_of_birth"))==""), True)).alias("missing_dob"),
    count(when(col("gender").isNull() | (trim(col("gender"))==""), True)).alias("missing_gender")
)

display(dq)

# COMMAND ----------

# ============================================================
# 7. SAVE SILVER TABLES (CURATED LAYER)
# ============================================================
# Purpose:
# - Persist the structured and validated datasets into the Silver layer.
# - Store curated application-level and spending-level tables in Delta format.
#
# Why this step is necessary:
# - Enables reliable downstream processing and analytics.
# - Ensures structured, deduplicated, and quality-assessed data is stored.
# - Prevents schema conflicts by dropping existing tables before overwrite.
#
# Tables Created:
# - applications_silver (deduplicated application-level data)
# - spending_silver (normalized spending data)
#
# Storage Format:
# - Delta Lake (supports ACID transactions and schema enforcement)
# ============================================================

# Drop tables if they already exist (to avoid schema conflict)
spark.sql("DROP TABLE IF EXISTS applications_silver")
spark.sql("DROP TABLE IF EXISTS spending_silver")

# Write applications silver table
applications_dedup.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("applications_silver")

# Write spending silver table
spending.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("spending_silver")
