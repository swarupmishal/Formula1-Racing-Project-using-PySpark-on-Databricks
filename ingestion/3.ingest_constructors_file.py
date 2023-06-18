# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.dropdown("Environment", "Dev", ["Prod", "Dev", "Test"], "Environment")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# display(constructor_df)
constructor_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted columns

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("env", lit(env)).withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# display(constructor_final_df)
constructor_final_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write output to parquet file

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/processed/constructors", True)

# constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors.parquet")
# constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")