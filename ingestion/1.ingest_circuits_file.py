# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()

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
# MAGIC ### Step 1 - Read the CSV file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
                                    
                                    ])

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header=True, schema=circuits_schema)

# COMMAND ----------

# display(circuits_df)
circuits_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# display(circuits_selected_df)
circuits_selected_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumn("env", lit(env)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# display(circuits_renamed_df)
circuits_renamed_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Adding ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# display(circuits_final_df)
circuits_final_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write data to datalake as Parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.circuits;

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/processed/circuits", True)
# dbutils.fs.ls("dbfs:/FileStore/tables/processed/circuits/*")

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits.parquet")
# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

display(circuits_final_df.select('file_date').distinct().collect())

# COMMAND ----------

