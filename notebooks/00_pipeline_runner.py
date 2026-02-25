# Databricks notebook source
# 00 - Pipeline Runner
# Runs ingestion and cleaning sequentially

dbutils.notebook.run("/Workspace/Users/sengulseyday@gmail.com/01_data_ingestion", 0)
dbutils.notebook.run("/Workspace/Users/sengulseyday@gmail.com/02_data_cleaning", 0)

print("Pipeline completed successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- Verify Created Tables in Default Database
# MAGIC -- Purpose:
# MAGIC -- - Confirm that Bronze/Silver/Gold tables were successfully created
# MAGIC -- - Ensure tables are persistent (isTemporary = false)
# MAGIC -- - Validate final pipeline output structure
# MAGIC -- =====================================================
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# =====================================================
# Final Row Count Validation
# Purpose:
# - Compare Silver and Gold table row counts
# - Ensure no unintended data loss during cleaning
# - Confirm pipeline integrity from Silver to Gold layer
# =====================================================

print("silver:", spark.table("applications_silver").count())
print("gold:", spark.table("applications_gold").count())

# COMMAND ----------

# =====================================================
# PII Removal Verification (Gold Layer)
# Purpose:
# - Confirm that raw sensitive columns are removed
# - Ensure only hashed versions remain in Gold table
# - Validate compliance with data governance policy
# =====================================================

gold = spark.table("applications_gold")
gold.printSchema()
print([c for c in ["ssn","email","ip_address","full_name"] if c in gold.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC #Pipeline Execution Summary
# MAGIC
# MAGIC - Bronze to Silver ingestion completed successfully.
# MAGIC - Silver to Gold transformation completed.
# MAGIC - No row loss detected.
# MAGIC - PII successfully masked.
# MAGIC - Gold table ready for analytics.