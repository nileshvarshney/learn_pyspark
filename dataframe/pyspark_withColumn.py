# Databricks notebook source
# PySpark withColumn() is a transformation function of DataFrame 
# which is used to change or update the value, convert the datatype
# of an existing DataFrame column, add/create a new column, and many-core.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

spark = SparkSession.builder.appName("learnSpark").getOrCreate()

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.printSchema()

# root
#  |-- firstname: string (nullable = true)
#  |-- middlename: string (nullable = true)
#  |-- lastname: string (nullable = true)
#  |-- dob: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: long (nullable = true)

# Change column DataType using PySpark withcolumn
df2 = df.withColumn("salary", col("salary").cast("Integer"))
df2.printSchema()

# root
#  |-- firstname: string (nullable = true)
#  |-- middlename: string (nullable = true)
#  |-- lastname: string (nullable = true)
#  |-- dob: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: integer (nullable = true)

# Update the value of an existing column
df3 = df2.withColumn("salary", col("salary") * 100)
df3.show(truncate=False)

# +---------+----------+--------+----------+------+------+
# |firstname|middlename|lastname|dob       |gender|salary|
# +---------+----------+--------+----------+------+------+
# |James    |          |Smith   |1991-04-01|M     |300000|
# |Michael  |Rose      |        |2000-05-19|M     |400000|
# |Robert   |          |Williams|1978-09-05|M     |400000|
# |Maria    |Anne      |Jones   |1967-12-01|F     |400000|
# |Jen      |Mary      |Brown   |1980-02-17|F     |-100  |
# +---------+----------+--------+----------+------+------+

# Create a new column from an existing
df4 = df.withColumn("copiedColumn", col("salary")* -1)
df4.printSchema()

# root
#  |-- firstname: string (nullable = true)
#  |-- middlename: string (nullable = true)
#  |-- lastname: string (nullable = true)
#  |-- dob: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: long (nullable = true)
#  |-- copiedColumn: long (nullable = true)

#Add a new column using withColumn()
df5 = df.withColumn("country", lit("USA"))
df5.printSchema()

# root
#  |-- firstname: string (nullable = true)
#  |-- middlename: string (nullable = true)
#  |-- lastname: string (nullable = true)
#  |-- dob: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: long (nullable = true)
#  |-- country: string (nullable = false)

# Rename column name
df6 = df.withColumnRenamed(existing="gender",new="sex")
df6.printSchema()

# root
#  |-- firstname: string (nullable = true)
#  |-- middlename: string (nullable = true)
#  |-- lastname: string (nullable = true)
#  |-- dob: string (nullable = true)
#  |-- sex: string (nullable = true)
#  |-- salary: long (nullable = true)

# Drop a column from PySpark DataFrame
df4.drop(col("copiedColumn")).show(truncate=False)

# +---------+----------+--------+----------+------+------+
# |firstname|middlename|lastname|dob       |gender|salary|
# +---------+----------+--------+----------+------+------+
# |James    |          |Smith   |1991-04-01|M     |3000  |
# |Michael  |Rose      |        |2000-05-19|M     |4000  |
# |Robert   |          |Williams|1978-09-05|M     |4000  |
# |Maria    |Anne      |Jones   |1967-12-01|F     |4000  |
# |Jen      |Mary      |Brown   |1980-02-17|F     |-1    |
# +---------+----------+--------+----------+------+------+

