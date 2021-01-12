import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("learnSpark").getOrCreate()

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]

pyspark_df = spark.createDataFrame(data = data, schema=columns)
pyspark_df.printSchema()
pyspark_df.show(truncate=False)

# convert to pandas
pandas_df = pyspark_df.toPandas()
print(pandas_df)

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
    StructField('dob', StringType(), nullable=True),
    StructField('gender', StringType(), nullable=True),
    StructField('salary', IntegerType(), nullable=True)
])

struct_df = spark.createDataFrame(data= dataStruct, schema=schemaStruct)
struct_df.printSchema()
struct_df.show(truncate=False)

####################################################
# Select                                           #
####################################################
#pyspark_df.select("first_name","last_name").show()