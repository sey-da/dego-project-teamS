# Databricks notebook source
# 02 - Data Cleaning & Standardization

from pyspark.sql import functions as F

apps_clean = spark.table("applications_silver")

display(apps_clean)
apps_clean.printSchema()

# COMMAND ----------

# Quick sanity checks
print("rows:", apps.count())
print("columns:", len(apps.columns))

# COMMAND ----------

#Step 1 — Standardize Gender
from pyspark.sql import functions as F

apps_clean = apps.withColumn(
    "gender_standardized",
    F.when(F.lower(F.col("gender")).isin("m", "male"), "Male")
     .when(F.lower(F.col("gender")).isin("f", "female"), "Female")
     .otherwise("Unknown")
)

display(apps_clean.select("gender", "gender_standardized"))

# COMMAND ----------

#Step 2 — Standardize Date of Birth
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

apps_clean.printSchema()

# COMMAND ----------

#Step 3 — Basic Email Validation
from pyspark.sql import functions as F

apps_clean = apps_clean.withColumn(
    "email_valid",
    F.when(F.col("email").isNull() | (F.trim(F.col("email")) == ""), F.lit(False))
     .otherwise(F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"))
)

display(apps_clean.select("email", "email_valid").limit(20))

# COMMAND ----------

# Count total rows and invalid emails after validation

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

#Step 4 — Mask Sensitive Data (PII)

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
# MAGIC **PII HANDLING**
# MAGIC
# MAGIC The dataset contains personally identifiable information, PII, including SSN, IP address, email, full name, and date of birth.
# MAGIC
# MAGIC To meet data governance and privacy requirements, selected PII fields are pseudonymized using hashing techniques. Raw sensitive values are not propagated to downstream datasets.
# MAGIC
# MAGIC After creating hashed equivalents, original PII columns are removed where not required for further processing.
# MAGIC
# MAGIC This approach reduces exposure risk while preserving analytical usability.
# MAGIC

# COMMAND ----------

# Re-run Validation Checks
apps_clean.select(
    F.count("*").alias("total_rows")
).display()

# COMMAND ----------

# Check null counts for critical columns

apps_clean.select(
    F.count(F.when(F.col("dob_parsed").isNull(), True)).alias("null_dob"),
    F.count(F.when(F.col("loan_purpose").isNull(), True)).alias("null_loan_purpose"),
    F.count(F.when(F.col("annual_salary").isNull(), True)).alias("null_annual_salary"),
    F.count(F.when(F.col("loan_approved").isNull(), True)).alias("null_loan_approved")
).display()

# COMMAND ----------

# Ensure no unintended data loss after dropping PII
apps_clean.select(
    "ssn_hash",
    "email_hash",
    "ip_hash",
    "name_hash"
).limit(5).display()

apps_clean.columns

# COMMAND ----------


#Verify Schema
apps_clean.printSchema()

# COMMAND ----------

#Detect numeric fields accidentally stored as string
string_columns = [c for c, t in apps_clean.dtypes if t == "string"]
string_columns

# COMMAND ----------

apps_clean.write.mode("overwrite").format("delta").saveAsTable("applications_gold")