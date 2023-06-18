# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.dropdown("Environment", "Dev", ["Prod", "Dev", "Test"], "Environment")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", '2021-03-21')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql import functions as f

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), False),
                                      StructField("milliseconds", IntegerType(), False),
                                     ])

# COMMAND ----------

df = spark.read.schema(pit_stops_schema).option("multiline", True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns

# COMMAND ----------

transformed_df = df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("env", f.lit(env)).withColumn("ingestion_date", f.current_timestamp()).withColumn("file_date", f.lit(v_file_date))

# COMMAND ----------

transformed_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write the output to processed container in parquet format

# COMMAND ----------

# dbutils.fs.rm("dbfs:/FileStore/tables/processed/pit_stops", True)

# transformed_df \
# .write \
# .mode("overwrite") \
# .parquet(f"{processed_folder_path}/pit_stops.parquet")

# transformed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# overwrite_partition(transformed_df, db_name='f1_processed', table_name='pit_stops', partition_column='race_id')
merge_condition = "tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(transformed_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP table f1_processed.pit_stops;
# MAGIC SELECT race_id, count(1) FROM f1_processed.pit_stops
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 DESC;