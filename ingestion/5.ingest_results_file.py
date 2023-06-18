# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

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
# MAGIC ### Step 1 - Import data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, IntegerType, FloatType
from pyspark.sql import functions as f

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("constructorId", IntegerType(), False),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), False),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), False),
                                    StructField("positionOrder", IntegerType(), False),
                                    StructField("points", FloatType(), False),
                                    StructField("laps", IntegerType(), False),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", StringType(), True),
                                    StructField("statusId", IntegerType(), False),
                                   
                                   ])

# COMMAND ----------

df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# display(df)
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Perform Data transformations

# COMMAND ----------

# Rename columns
renamed_df = df.withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("positionText", "position_text").withColumnRenamed("positionOrder", "position_order").withColumnRenamed("fastestLap", "fastest_lap").withColumnRenamed("fastestLapTime", "fastest_lap_time").withColumnRenamed("fastestLapSpeed", "fastest_lap_speed").withColumnRenamed("statusId", "status_id")

# COMMAND ----------

# Drop unwanted column and add Ingestion Date column
transformed_df = renamed_df.drop("status_id").withColumn("env", f.lit(env)).withColumn("ingestion_date", f.current_timestamp()).withColumn("file_date", f.lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### De-dupe the dataframe

# COMMAND ----------

result_dedupped_df = transformed_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Load data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1

# COMMAND ----------

# for race_id in transformed_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id.race_id})")

# COMMAND ----------

# # transformed_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results.parquet")
# transformed_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2

# COMMAND ----------

# overwrite_partition(transformed_df, db_name='f1_processed', table_name='results', partition_column='race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(result_dedupped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists f1_processed.results;
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.results
# MAGIC group by 1
# MAGIC order by 1 desc;