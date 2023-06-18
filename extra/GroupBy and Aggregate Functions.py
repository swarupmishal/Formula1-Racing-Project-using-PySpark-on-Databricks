# Databricks notebook source
# MAGIC %md
# MAGIC # GroupBy and Aggregate Functions

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('aggs').getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/sales_info-1.csv', header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.groupBy("company")

# COMMAND ----------

df.groupBy("company").mean().show()

# COMMAND ----------

df.agg({'sales': 'count'}).show()

# COMMAND ----------

group_data = df.groupBy("company")

# COMMAND ----------

group_data.agg({'sales':'max'}).show()

# COMMAND ----------

group_data

# COMMAND ----------

from pyspark.sql.functions import countDistinct, avg, stddev

# COMMAND ----------

df.select(avg('sales').alias('Average Sales')).show()

# COMMAND ----------

df.select(stddev('sales')).show()

# COMMAND ----------

from pyspark.sql.functions import format_number

# COMMAND ----------

sales_std = df.select(stddev("sales").alias('std'))

# COMMAND ----------

sales_std.select(format_number('std', 2).alias('std')).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.orderBy('sales').show()

# COMMAND ----------

df.orderBy(df['sales'].desc()).show()

# COMMAND ----------

