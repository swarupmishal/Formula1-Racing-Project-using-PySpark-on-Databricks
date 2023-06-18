# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file

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
# MAGIC ### Step 1 - Extract file from filestore

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema, True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                                   ])

# COMMAND ----------

df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")
# .schema(drivers_schema.json("/FileStore/tables/drivers.json")

# COMMAND ----------

# display(df)
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns, drop unwanted columns and add new columns

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

transformed_df = df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref").withColumn("ingestion_date", f.current_timestamp()).drop("url").withColumn("name(transformed)", f.concat(df['name']['forename'], f.lit(' '), df['name']['surname'])).drop("name").withColumn("env", f.lit(env)).withColumn("file_date", f.lit(v_file_date))

# COMMAND ----------

final_df = transformed_df.select("driver_id", "driver_ref", "number", "code", "name(transformed)", "dob", "nationality", "env", "ingestion_date", "file_date").withColumnRenamed("name(transformed)", "name")

# COMMAND ----------

# display(final_df)
final_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Load file

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/processed/drivers", True)
# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers.parquet")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")
final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")