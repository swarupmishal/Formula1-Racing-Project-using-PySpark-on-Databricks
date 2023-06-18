# Databricks notebook source
# MAGIC %md
# MAGIC # Spark DataFrame
# MAGIC ## Missing Data

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('miss').getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/ContainsNull.csv', header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.na.drop(thresh=2).show()

# COMMAND ----------

df.na.drop(subset=['sales']).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.na.fill('No Name', subset=['Name']).show()

# COMMAND ----------

df.na.fill(0, subset=['Sales']).show()

# COMMAND ----------

from pyspark.sql.functions import mean

# COMMAND ----------

mean_val = df.select(mean(df['sales'])).collect()

# COMMAND ----------

mean_sales = mean_val[0][0]

# COMMAND ----------

print(mean_val[0][0])

# COMMAND ----------

df.na.fill(mean_sales, subset=['sales']).show()

# COMMAND ----------

df.na.fill(df.select(mean(df['sales'])).collect()[0][0], ['sales']).show()

# COMMAND ----------

