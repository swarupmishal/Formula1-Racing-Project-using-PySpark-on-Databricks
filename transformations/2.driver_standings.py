# Databricks notebook source
# MAGIC %md
# MAGIC # Produce Driver Standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results.parquet")
# race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_results_df.show(5)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as f

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "team") \
                        .agg(f.sum("points").alias("total_points"), f.count(f.when(f.col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(f.desc("total_points"), f.desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("rank", f.rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings.parquet")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_id')