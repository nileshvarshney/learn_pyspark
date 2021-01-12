# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.appName('learnSpark').getOrCreate()

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)
print('Record Count ' + str(df.count()))

# root
#  |-- employee_name: string (nullable = true)
#  |-- department: string (nullable = true)
#  |-- salary: long (nullable = true)

# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |James        |Sales     |3000  |
# |Michael      |Sales     |4600  |
# |Robert       |Sales     |4100  |
# |Maria        |Finance   |3000  |
# |James        |Sales     |3000  |
# |Scott        |Finance   |3300  |
# |Jen          |Finance   |3900  |
# |Jeff         |Marketing |3000  |
# |Kumar        |Marketing |2000  |
# |Saif         |Sales     |4100  |
# +-------------+----------+------+


# get distinct row
df_distinct = df.distinct()
df_distinct.show(truncate=False)
print('Distinct count ' + str(df_distinct.count()))

# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |James        |Sales     |3000  |
# |Michael      |Sales     |4600  |
# |Robert       |Sales     |4100  |
# |Maria        |Finance   |3000  |
# |Scott        |Finance   |3300  |
# |Jen          |Finance   |3900  |
# |Jeff         |Marketing |3000  |
# |Kumar        |Marketing |2000  |
# |Saif         |Sales     |4100  |
# +-------------+----------+------+

# Distinct count 9

dropDisDF  = df.drop_duplicates(['department','salary'])
dropDisDF.show(truncate = False)
print('Distinct count ' + str(dropDisDF.count()))

# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |Maria        |Finance   |3000  |
# |Scott        |Finance   |3300  |
# |Jen          |Finance   |3900  |
# |Kumar        |Marketing |2000  |
# |Jeff         |Marketing |3000  |
# |James        |Sales     |3000  |
# |Robert       |Sales     |4100  |
# |Michael      |Sales     |4600  |
# +-------------+----------+------+

# Distinct count 8
