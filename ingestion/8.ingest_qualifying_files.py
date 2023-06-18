# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Qualifying folder

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.dropdown("Environment", "Dev", ["Prod", "Dev", "Test"], "Environment")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON files using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql import functions as f

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("constructorId", IntegerType(), False),
                                      StructField("number", IntegerType(), False),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

df = spark.read.schema(qualifying_schema).option("multiline", True).json(f"{raw_folder_path}/{v_file_date}/qualifying/")

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns

# COMMAND ----------

transformed_df = df.withColumnRenamed("qualifyId", "qualify_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumn("env", f.lit(env)).withColumn("ingestion_date", f.current_timestamp())

# COMMAND ----------

transformed_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write the output to processed container in parquet format

# COMMAND ----------

# dbutils.fs.rm("dbfs:/FileStore/tables/processed/qualifying", True)

# transformed_df \
# .write \
# .mode("overwrite") \
# .parquet(f"{processed_folder_path}/qualifying.parquet")
# transformed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(transformed_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 DESC