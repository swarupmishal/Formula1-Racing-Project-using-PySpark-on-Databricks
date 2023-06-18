# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion Races

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
# MAGIC ### Step 1 - Import CSV data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp, substring

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                            StructField("year", IntegerType(), True),
                            StructField("round", IntegerType(), True),
                            StructField("circuitId", IntegerType(), True),
                            StructField("name", StringType(), True),
                            StructField("date", DateType(), True),
                            StructField("time", StringType(), True),
                            StructField("url", StringType(), True),
                           
                           ])

# COMMAND ----------

df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", header=True, schema=race_schema)

# COMMAND ----------

# display(df)
df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Transform data

# COMMAND ----------

transformed_df = df.withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId", "circuit_id").withColumn("env", lit(env)).withColumn("ingestion_date", current_timestamp()).drop("url")

# COMMAND ----------

# display(transformed_df)
transformed_df.show(5)

# COMMAND ----------

final_df = transformed_df.select(transformed_df.race_id, transformed_df.race_year, transformed_df.round, transformed_df.circuit_id, transformed_df.name, to_timestamp(concat(transformed_df.date, lit(' '), substring(to_timestamp(transformed_df.time), 12, 9))).alias("race_timestamp"), transformed_df.ingestion_date).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# display(final_df)
final_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Load data into Data Lake

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/processed/races", True)
# final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races.parquet")
# final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")
final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;