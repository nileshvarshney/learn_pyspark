#import pyspark
from pyspark.sql import Row, SparkSession

spark = SparkSession.builder.appName("learnSpark").getOrCreate()

data = [
    Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"), 
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")
]

rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

for row in rdd.collect():
    print(row.name + " " + str(row.lang))

df  = rdd.toDF()
df.printSchema()
df.show(truncate=False)