# Databricks notebook source
# PySpark RDD/DataFrame collect() function is used to retrieve all the elements 
# of the dataset (from all nodes) to the driver node.

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("learnSpark").getOrCreate()

data = [
  ("Finance",10), \
  ("Marketing",20), \
  ("Sales",30), \
  ("IT",40) \
  ]

columns =  ["dept_name","dept_id"]
dept_df = spark.createDataFrame(data = data, schema = columns)
dept_df.printSchema()
dept_df.show(truncate=False)

# root
#  |-- dept_name: string (nullable = true)
#  |-- dept_id: long (nullable = true)

# +---------+-------+
# |dept_name|dept_id|
# +---------+-------+
# |Finance  |10     |
# |Marketing|20     |
# |Sales    |30     |
# |IT       |40     |
# +---------+-------+

dataCollect = dept_df.collect()
print(dataCollect)

for row in dataCollect:
  print(str(row['dept_id']) + ' ' + row['dept_name'])

# [Row(dept_name='Finance', dept_id=10), Row(dept_name='Marketing', dept_id=20), Row(dept_name='Sales', dept_id=30), Row(dept_name='IT', dept_id=40)]
# 10 Finance
# 20 Marketing
# 30 Sales
# 40 IT

# Usually, collect() is used to retrieve the action output when you have very 
# small result set and calling collect() on an RDD/DataFrame with a bigger result
# set causes out of memory as it returns the entire dataset (from all workers) 
# to the driver hence we should avoid calling collect() on a larger dataset.

dataCollect = dept_df.select("dept_name").collect()
print(dataCollect)
for row in dataCollect:
  print(row['dept_name'])
