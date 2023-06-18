# Databricks notebook source
# MAGIC %md
# MAGIC # Dates and Timestamps

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('dates').getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/appl_stock.csv', header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.head(1)

# COMMAND ----------

df.select(['date', 'open']).show()

# COMMAND ----------

from pyspark.sql.functions import to_date, to_timestamp

# COMMAND ----------

df = df.withColumn('Date', to_timestamp(df['Date'], 'yyyy-MM-dd'))

# COMMAND ----------

df.select(['date', 'open']).show()

# COMMAND ----------

from pyspark.sql.functions import (dayofmonth, hour, dayofyear, month, year, weekofyear, format_number, date_format)

# COMMAND ----------

df.select(dayofmonth(df['date'])).show()

# COMMAND ----------

df.select(hour(df['date'])).show()

# COMMAND ----------

df.select(month(df['date'])).show()

# COMMAND ----------

df.select(year(df['date'])).show()

# COMMAND ----------

df.withColumn('Year', year(df['date'])).show()

# COMMAND ----------

new_df = df.withColumn('Year', year(df['date']))

# COMMAND ----------

new_df.groupBy('Year').mean().show()

# COMMAND ----------

result = new_df.groupBy('Year').mean().select('year','avg(close)')

# COMMAND ----------

new = result.withColumnRenamed('avg(close)', 'Average Closing Price')

# COMMAND ----------

new.show()

# COMMAND ----------

new.select(['Year', format_number('Average Closing Price', 2).alias('Average Closing Price')]).show()

# COMMAND ----------

