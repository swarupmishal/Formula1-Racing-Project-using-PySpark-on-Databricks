# Databricks notebook source
# MAGIC %md
# MAGIC # Prepare Presentation layer

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Import all required tables

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers.parquet")
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnRenamed("number", "driver_number").withColumnRenamed("name", "driver_name").withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results").filter(f"file_date = '{v_file_date}'").withColumnRenamed("time", "race_time").withColumnRenamed("race_id", "result_race_id").withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# display(results_df.select('result_file_date').distinct().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select required columns after joining tables

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

final_df = results_df.join(races_df, results_df.result_race_id == races_df.race_id, "left") \
                .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "left") \
                .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "left") \
                .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "left") \
                .withColumn("created_date", f.current_timestamp()) \
                .select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_nationality", "team", "grid", "fastest_lap",
                        "race_time", "points", "position", "result_file_date") \
                .withColumn("created_date", current_timestamp()) \
                .withColumnRenamed("result_file_date", "file_date")


# COMMAND ----------

# display(final_df)
final_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write data to the presentation layer

# COMMAND ----------

# Prepare the data for the visualization used on the site - https://www.bbc.com/sport/formula1/2020/abu-dhabi-grand-prix/results
# display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results.parquet")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
# overwrite_partition(final_df, db_name='f1_presentation', table_name='race_results', partition_column='race_id')
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_presentation.race_results
# MAGIC select race_id, COUNT(1)
# MAGIC from f1_presentation.race_results
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 DESC