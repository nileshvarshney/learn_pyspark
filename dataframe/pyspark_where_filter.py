# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains
from pyspark.sql.types import StringType, StructField, StructType, ArrayType


data = [
        (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
        (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
        (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
        (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
        (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
        (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
        ]

schema = StructType([
  StructField('name', StructType([
    StructField('firstname', StringType(), nullable=True),
    StructField('middlename', StringType(), nullable=True),
    StructField('lastname', StringType(), nullable=True)
  ])),
  StructField('language', ArrayType(StringType()), nullable=True),
  StructField('state', StringType(), nullable=True),
  StructField('gender',StringType(), nullable=True)
])

spark = SparkSession.builder.appName('learnSpark').getOrCreate()
df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
df.show(truncate=False)


# root
#  |-- name: struct (nullable = true)
#  |    |-- firstname: string (nullable = true)
#  |    |-- middlename: string (nullable = true)
#  |    |-- lastname: string (nullable = true)
#  |-- language: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- state: string (nullable = true)
#  |-- gender: string (nullable = true)

# +----------------------+------------------+-----+------+
# |name                  |language          |state|gender|
# +----------------------+------------------+-----+------+
# |[James, , Smith]      |[Java, Scala, C++]|OH   |M     |
# |[Anna, Rose, ]        |[Spark, Java, C++]|NY   |F     |
# |[Julia, , Williams]   |[CSharp, VB]      |OH   |F     |
# |[Maria, Anne, Jones]  |[CSharp, VB]      |NY   |M     |
# |[Jen, Mary, Brown]    |[CSharp, VB]      |NY   |M     |
# |[Mike, Mary, Williams]|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+

# filter
df.filter(df.state == 'OH').show()

# +--------------------+------------------+-----+------+
# |                name|          language|state|gender|
# +--------------------+------------------+-----+------+
# |    [James, , Smith]|[Java, Scala, C++]|   OH|     M|
# | [Julia, , Williams]|      [CSharp, VB]|   OH|     F|
# |[Mike, Mary, Will...|      [Python, VB]|   OH|     M|
# +--------------------+------------------+-----+------+

df.filter(col("gender") == "M").show()

# +--------------------+------------------+-----+------+
# |                name|          language|state|gender|
# +--------------------+------------------+-----+------+
# |    [James, , Smith]|[Java, Scala, C++]|   OH|     M|
# |[Maria, Anne, Jones]|      [CSharp, VB]|   NY|     M|
# |  [Jen, Mary, Brown]|      [CSharp, VB]|   NY|     M|
# |[Mike, Mary, Will...|      [Python, VB]|   OH|     M|
# +--------------------+------------------+-----+------+

df.filter("gender == 'M'") .show()

# +--------------------+------------------+-----+------+
# |                name|          language|state|gender|
# +--------------------+------------------+-----+------+
# |    [James, , Smith]|[Java, Scala, C++]|   OH|     M|
# |[Maria, Anne, Jones]|      [CSharp, VB]|   NY|     M|
# |  [Jen, Mary, Brown]|      [CSharp, VB]|   NY|     M|
# |[Mike, Mary, Will...|      [Python, VB]|   OH|     M|
# +--------------------+------------------+-----+------+

df.filter((col("gender") == 'M') & (col("state") != 'OH')).show()

# +--------------------+------------+-----+------+
# |                name|    language|state|gender|
# +--------------------+------------+-----+------+
# |[Maria, Anne, Jones]|[CSharp, VB]|   NY|     M|
# |  [Jen, Mary, Brown]|[CSharp, VB]|   NY|     M|
# +--------------------+------------+-----+------+

df.filter(array_contains(col("language"),"Java")).show()

# +----------------+------------------+-----+------+
# |            name|          language|state|gender|
# +----------------+------------------+-----+------+
# |[James, , Smith]|[Java, Scala, C++]|   OH|     M|
# |  [Anna, Rose, ]|[Spark, Java, C++]|   NY|     F|
# +----------------+------------------+-----+------+

df.filter(col("name.lastname") == 'Williams').show(truncate=False)

# +----------------------+------------+-----+------+
# |name                  |language    |state|gender|
# +----------------------+------------+-----+------+
# |[Julia, , Williams]   |[CSharp, VB]|OH   |F     |
# |[Mike, Mary, Williams]|[Python, VB]|OH   |M     |
# +----------------------+------------+-----+------+


