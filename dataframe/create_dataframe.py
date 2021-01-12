import pyspark
from pyspark.sql import  SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import  *

columns =  ["language", "user_counts"]
data = [("java", 20000), ("Python", 100000), ("Scala", 50000)]

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema =  StructType([
    StructField("first_name", StringType(), nullable=True),
    StructField("middle_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("id", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("salary", IntegerType(), nullable=True)
])

spark = SparkSession.builder.appName("learnSpark").getOrCreate()
rdd = spark.sparkContext.parallelize(data)

dfFromRDD = rdd.toDF()
dfFromRDD.printSchema()

# root
#  |-- _1: string (nullable = true)
#  |-- _2: long (nullable = true)

# by providing Schema
dfFromRDD1  = rdd.toDF(columns)
dfFromRDD1.printSchema()

# using createDataFrame
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.printSchema()

# using lambda
rowData = map(lambda x: Row(*x), data)
dfFromRDD3 = spark.createDataFrame(rowData, columns)
dfFromRDD3.printSchema()

# create dataframe with schema
df = spark.createDataFrame(
    data = data2,
    schema=schema
)
df.printSchema()
df.show(truncate=False)

# root
#  |-- first_name: string (nullable = true)
#  |-- middle_name: string (nullable = true)
#  |-- last_name: string (nullable = true)
#  |-- id: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: integer (nullable = true)

# +----------+-----------+---------+-----+------+------+
# |first_name|middle_name|last_name|id   |gender|salary|
# +----------+-----------+---------+-----+------+------+
# |James     |           |Smith    |36636|M     |3000  |
# |Michael   |Rose       |         |40288|M     |4000  |
# |Robert    |           |Williams |42114|M     |4000  |
# |Maria     |Anne       |Jones    |39192|F     |4000  |
# |Jen       |Mary       |Brown    |     |F     |-1    |
# +----------+-----------+---------+-----+------+------+

# Create dataframe from data source
df2 = spark.read.csv("./resources/small_zip.csv", header=True)
df2.printSchema()
df2.show(truncate=False)

# root
#  |-- id: string (nullable = true)
#  |-- zipcode: string (nullable = true)
#  |-- type: string (nullable = true)
#  |-- city: string (nullable = true)
#  |-- state: string (nullable = true)
#  |-- population: string (nullable = true)

# +---+-------+--------+-------------------+-----+----------+
# |id |zipcode|type    |city               |state|population|
# +---+-------+--------+-------------------+-----+----------+
# |1  |704    |STANDARD|null               |PR   |30100     |
# |2  |704    |null    |PASEO COSTA DEL SUR|PR   |null      |
# |3  |709    |null    |BDA SAN LUIS       |PR   |3700      |
# |4  |76166  |UNIQUE  |CINGULAR WIRELESS  |TX   |84000     |
# |5  |76177  |STANDARD|null               |TX   |null      |
# +---+-------+--------+-------------------+-----+----------+

# Create dataframe from json data source
df3 = spark.read.json("./resources/small_zip.json", multiLine=True)
df3.printSchema()
df3.show(truncate=False)

# root
#  |-- City: string (nullable = true)
#  |-- RecordNumber: long (nullable = true)
#  |-- State: string (nullable = true)
#  |-- ZipCodeType: string (nullable = true)
#  |-- Zipcode: long (nullable = true)

# +-------------------+------------+-----+-----------+-------+
# |City               |RecordNumber|State|ZipCodeType|Zipcode|
# +-------------------+------------+-----+-----------+-------+
# |PASEO COSTA DEL SUR|2           |PR   |STANDARD   |704    |
# |BDA SAN LUIS       |10          |PR   |STANDARD   |709    |
# +-------------------+------------+-----+-----------+-------+