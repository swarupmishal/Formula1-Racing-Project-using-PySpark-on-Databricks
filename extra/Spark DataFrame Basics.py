# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('Basics').getOrCreate()

# COMMAND ----------

df = sqlContext.sql("select * from people")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql.types import StructField, StringType, IntegerType, StructType

# COMMAND ----------

data_schema = [StructField('age', IntegerType(), True),
              StructField('name', StringType(), True)]

# COMMAND ----------

final_struc = StructType(fields=data_schema)

# COMMAND ----------



# COMMAND ----------

df.show()

# COMMAND ----------

type(df.select('age'))

# COMMAND ----------

df.head(2)[0]

# COMMAND ----------

df.select(['age', 'name']).show()

# COMMAND ----------

df.withColumn('double_age', df['age']*2).show()

# COMMAND ----------

df.withColumnRenamed('age', 'my_new_age').show()

# COMMAND ----------

df.createOrReplaceGlobalTempView('people3')

# COMMAND ----------

results = spark.sql("select * from people where age=30")

# COMMAND ----------

results.show()

# COMMAND ----------

