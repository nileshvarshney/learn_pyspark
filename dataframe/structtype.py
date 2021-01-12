import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import col, struct, when

spark = SparkSession.builder.appName("learnSpark").getOrCreate()

# nested structure data
dataStruct = [(("James","","Smith"),"36636","M",3000),
    (("Michael","Rose",""),"40288","M",4000),
    (("Robert","","Williams"),"42114","M",4000),
    (("Maria","Anne","Jones"),"39192","F",4000),
    (("Jen","Mary","Brown"),"","F",-1)
  ]

schemaStruct = StructType([
    StructField('name', StructType([
        StructField('first_name', StringType(), nullable=True),
        StructField('middle_name', StringType(), nullable=True),
        StructField('last_name', StringType(), nullable=True),
    ])),
    StructField('id', StringType(), nullable=True),
    StructField('gender', StringType(), nullable=True),
    StructField('salary', StringType(), nullable=True)
])

struct_df = spark.createDataFrame(data= dataStruct, schema=schemaStruct)
struct_df.printSchema()
struct_df.show(truncate=False)

# Modify existing schema to Struct Schema
updated_struct_df = struct_df.withColumn("OtherInfo",
    struct(
        col("id").alias("identifier"),
        col("gender").alias("gender"),
        col("salary").alias("salary"),
        when(col("salary").cast(IntegerType()) < 2000, "Low")
        .when(col("salary").cast(IntegerType()) < 4000 , "Medium")
        .otherwise("High").alias("salary_grade")
    )
).drop("id","gender","salary")

updated_struct_df.printSchema()
updated_struct_df.show(truncate=False)

# root
#  |-- name: struct (nullable = true)
#  |    |-- first_name: string (nullable = true)
#  |    |-- middle_name: string (nullable = true)
#  |    |-- last_name: string (nullable = true)
#  |-- OtherInfo: struct (nullable = false)
#  |    |-- identifier: string (nullable = true)
#  |    |-- gender: string (nullable = true)
#  |    |-- salary: string (nullable = true)
#  |    |-- salary_grade: string (nullable = false)

# Array and Map Schema
arrayStructureSchema =  StructType([
    StructField('name',StructType([
        StructField('first_name', StringType(), nullable=True),
        StructField('middle_name', StringType(), nullable=True),
        StructField('last_name', StringType(), nullable=True),
    ])),
    StructField('id', StringType(), nullable=True),
    StructField('gender', StringType(), nullable=True),
    StructField('salary', StringType(), nullable=True),
    StructField('hobbies', ArrayType(StringType()), nullable=True),
    StructField('properties', MapType(StringType(), StringType()),True)
])
array_map_df = spark.createDataFrame([], schema=arrayStructureSchema)
array_map_df.printSchema()

# root
#  |-- name: struct (nullable = true)
#  |    |-- first_name: string (nullable = true)
#  |    |-- middle_name: string (nullable = true)
#  |    |-- last_name: string (nullable = true)
#  |-- id: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- salary: string (nullable = true)
#  |-- hobbies: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- properties: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)


