# Databricks notebook source
# MAGIC %md
# MAGIC # Access Dataframes using SQL

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

results_df = spark.read.parquet(f"{presentation_folder_path}/race_results.parquet")

# COMMAND ----------

results_df.createOrReplaceGlobalTempView("gv_results_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_results_view;

# COMMAND ----------

