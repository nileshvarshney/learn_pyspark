# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

spark = SparkSession.builder.appName("learnSpark").getOrCreate()

data = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

schema = StructType([
  StructField('name', StructType([
    StructField('firstname',StringType(), True),
    StructField('middlename',StringType(), True),
    StructField('lastname',StringType(), True)
  ])),
  StructField('dob', StringType(), True),
  StructField('gender', StringType(), True),
  StructField('salary', IntegerType(), True)
])


df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()


# root
#  |-- name: struct (nullable = true)
#  |    |-- firstname: string (nullable = true)
#  |    |-- middlename: string (nullable = true)
#  |    |-- lastname: string (nullable = true)
#  |-- dob: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: integer (nullable = true)


# To rename DataFrame column name
# withColumnRenamed(existingName, newNam)#
# rename dob to DateOfBirth
df2 = df.withColumnRenamed("dob","DateOfBirth")
df2.printSchema()

# root
#  |-- name: struct (nullable = true)
#  |    |-- firstname: string (nullable = true)
#  |    |-- middlename: string (nullable = true)
#  |    |-- lastname: string (nullable = true)
#  |-- DateOfBirth: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: integer (nullable = true)

# rename multiple columns
df3 = df.withColumnRenamed("dob","DateOfBirth")\
        .withColumnRenamed("salary","salary_amount")

df3.printSchema()

# root
#  |-- name: struct (nullable = true)
#  |    |-- firstname: string (nullable = true)
#  |    |-- middlename: string (nullable = true)
#  |    |-- lastname: string (nullable = true)
#  |-- DateOfBirth: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary_amount: integer (nullable = true)

#rename a nested column in Dataframe
schema2 = StructType([
  StructField('fname',StringType(), True),
  StructField('mname',StringType(), True),
  StructField('lname',StringType(), True)
])

df.select(col("name").cast(schema2), col("dob"), col("gender"),col("salary")).printSchema()

# root
#  |-- name: struct (nullable = true)
#  |    |-- fname: string (nullable = true)
#  |    |-- mname: string (nullable = true)
#  |    |-- lname: string (nullable = true)
#  |-- dob: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: integer (nullable = true)

#To rename nested elements
df4 = df.select(
  col("name.firstname").alias("fname"),
  col("name.middlename").alias("mname"),
  col("name.lastname").alias("lname"),
  col("dob"), col("gender"),col("salary")
)

df4.show()

# +-------+-----+--------+----------+------+------+
# |  fname|mname|   lname|       dob|gender|salary|
# +-------+-----+--------+----------+------+------+
# |  James|     |   Smith|1991-04-01|     M|  3000|
# |Michael| Rose|        |2000-05-19|     M|  4000|
# | Robert|     |Williams|1978-09-05|     M|  4000|
# |  Maria| Anne|   Jones|1967-12-01|     F|  4000|
# |    Jen| Mary|   Brown|1980-02-17|     F|    -1|
# +-------+-----+--------+----------+------+------+

df5 = df.withColumn("fname", col("name.firstname"))\
  .withColumn("mname", col("name.middlename"))\
  .withColumn("lname", col("name.lastname"))\
.drop("name")

df5.printSchema()

# root
#  |-- dob: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: integer (nullable = true)
#  |-- fname: string (nullable = true)
#  |-- mname: string (nullable = true)
#  |-- lname: string (nullable = true)

#To change all columns in a PySpark 
newColumns = ["DOB","Sex","Salary","FName","MName","LName"]
df5.toDF(*newColumns).printSchema()

# root
#  |-- DOB: string (nullable = true)
#  |-- Sex: string (nullable = true)
#  |-- Salary: integer (nullable = true)
#  |-- FName: string (nullable = true)
#  |-- MName: string (nullable = true)
#  |-- LName: string (nullable = true)



