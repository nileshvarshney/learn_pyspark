import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,StringType


spark = SparkSession.builder.appName("learnSpark").getOrCreate()

schema = StructType([
    StructField("first_name", StringType(), nullable=True),
    StructField("middle_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True)
])

df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)
df.printSchema()

# otherway
df1 = spark.sparkContext.parallelize([]).toDF(schema)
df1.printSchema()

df2 = spark.createDataFrame([],schema=schema)
df2.printSchema()

# root
#  |-- first_name: string (nullable = true)
#  |-- middle_name: string (nullable = true)
#  |-- last_name: string (nullable = true)