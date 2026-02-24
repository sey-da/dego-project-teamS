# Databricks notebook source
df = spark.read.option("multiline","true") \
    .json("/Volumes/workspace/default/raw_data/raw_credit_applications.json")

display(df)
df.printSchema()

# COMMAND ----------

#1. Load Raw JSON Data
from pyspark.sql.functions import col

applications = df.select(
    col("_id"),
    col("loan_purpose"),
    col("processing_timestamp"),
    col("applicant_info.*"),
    col("financials.*"),
    col("decision.*")
)

display(applications)
applications.printSchema()

# COMMAND ----------

#2. Flatten Application-Level Data

from pyspark.sql.functions import col

applications = df.select(
    col("_id"),
    col("loan_purpose"),
    col("processing_timestamp"),
    col("applicant_info.*"),
    col("financials.*"),
    col("decision.*")
)

display(applications)
applications.printSchema()

# COMMAND ----------

# 3. Extract Spending Behavior (Explode Array)
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

# 4. Initial Data Quality Assessment
from pyspark.sql.functions import count, when, trim

dq = applications.select(
    count("*").alias("total_rows"),
    count(when(col("email").isNull() | (trim(col("email"))==""), True)).alias("missing_email"),
    count(when(col("date_of_birth").isNull() | (trim(col("date_of_birth"))==""), True)).alias("missing_dob"),
    count(when(col("gender").isNull() | (trim(col("gender"))==""), True)).alias("missing_gender"),
    count(when(col("loan_purpose").isNull() | (trim(col("loan_purpose"))==""), True)).alias("missing_loan_purpose")
)

display(dq)

# COMMAND ----------

# 5. Save Silver Tables (Delta Format)
applications.write.mode("overwrite").format("delta").saveAsTable("applications_silver")
spending.write.mode("overwrite").format("delta").saveAsTable("spending_silver")