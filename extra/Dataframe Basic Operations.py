# Databricks notebook source
# MAGIC %md
# MAGIC # Spark DataFrame Basic Operations

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('ops').getOrCreate()

# COMMAND ----------

df = spark.read.table('people', inferschema = True,

# COMMAND ----------

df.show()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/appl_stock.csv', inferSchema=True, header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.filter("close < 200").select(["open", "close"]).show()

# COMMAND ----------

df1 = df.filter(df["close"] < 200).filter(df["open"] > 200).collect()

# COMMAND ----------

print(df1)

# COMMAND ----------

