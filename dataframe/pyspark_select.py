# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]

spark = SparkSession.builder.appName("learnSpark").getOrCreate()

# create dataframe and check its contents
df = spark.createDataFrame(data=data, schema=columns)
df.show(truncate=False)
df.printSchema()

# use select to select specific columns
df.select("firstname", "lastname").show(truncate=False)

#Using Dataframe object name
df.select(df.firstname, df.lastname, df.country).show(truncate=False)

# +---------+--------+-------+-----+
# |firstname|lastname|country|state|
# +---------+--------+-------+-----+
# |James    |Smith   |USA    |CA   |
# |Michael  |Rose    |USA    |NY   |
# |Robert   |Williams|USA    |CA   |
# |Maria    |Jones   |USA    |FL   |
# +---------+--------+-------+-----+

# root
#  |-- firstname: string (nullable = true)
#  |-- lastname: string (nullable = true)
#  |-- country: string (nullable = true)
#  |-- state: string (nullable = true)

# +---------+--------+
# |firstname|lastname|
# +---------+--------+
# |James    |Smith   |
# |Michael  |Rose    |
# |Robert   |Williams|
# |Maria    |Jones   |
# +---------+--------+

# +---------+--------+-------+
# |firstname|lastname|country|
# +---------+--------+-------+
# |James    |Smith   |USA    |
# |Michael  |Rose    |USA    |
# |Robert   |Williams|USA    |
# |Maria    |Jones   |USA    |
# +---------+--------+-------+

# Using col function
df.select(col("firstname"), col("lastname")).show(truncate=False)

# +---------+--------+
# |firstname|lastname|
# +---------+--------+
# |James    |Smith   |
# |Michael  |Rose    |
# |Robert   |Williams|
# |Maria    |Jones   |
# +---------+--------+

# Nested Data
data_nested = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

schema_nested = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data_nested, schema = schema_nested)

df2.printSchema()
df2.select("name").show(truncate=False)

# root
#  |-- name: struct (nullable = true)
#  |    |-- firstname: string (nullable = true)
#  |    |-- middlename: string (nullable = true)
#  |    |-- lastname: string (nullable = true)
#  |-- state: string (nullable = true)
#  |-- gender: string (nullable = true)

# +----------------------+
# |name                  |
# +----------------------+
# |[James,, Smith]       |
# |[Anna, Rose, ]        |
# |[Julia, , Williams]   |
# |[Maria, Anne, Jones]  |
# |[Jen, Mary, Brown]    |
# |[Mike, Mary, Williams]|
# +----------------------+

df2.select("name.firstname","name.lastname").show(truncate=False)

# +---------+--------+
# |firstname|lastname|
# +---------+--------+
# |James    |Smith   |
# |Anna     |        |
# |Julia    |Williams|
# |Maria    |Jones   |
# |Jen      |Brown   |
# |Mike     |Williams|
# +---------+--------+

df2.select("name.*").show(truncate=False)

# +---------+----------+--------+
# |firstname|middlename|lastname|
# +---------+----------+--------+
# |James    |null      |Smith   |
# |Anna     |Rose      |        |
# |Julia    |          |Williams|
# |Maria    |Anne      |Jones   |
# |Jen      |Mary      |Brown   |
# |Mike     |Mary      |Williams|
# +---------+----------+--------+
