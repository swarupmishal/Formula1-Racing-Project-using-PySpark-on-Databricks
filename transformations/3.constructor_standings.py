# Databricks notebook source
# MAGIC %md
# MAGIC # Produce Constructor Standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results.parquet")

# COMMAND ----------

race_results_df.show(5)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as f

# COMMAND ----------

constructor_standings_df = race_results_df.groupBy("race_year", "team") \
                        .agg(f.sum("points").alias("total_points"), f.count(f.when(f.col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(f.desc("total_points"), f.desc("wins"))

# COMMAND ----------

final_df = constructor_standings_df.withColumn("rank", f.rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings.parquet")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_id')