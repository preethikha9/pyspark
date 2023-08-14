from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create SparkSession 
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate() 
spark.sparkContext.setLogLevel("WARN")

#type 1
#list of tuples as a row
data = [(1, 'Preethi'),(2, 'Adi'), (3, 'Rathi'), (4, 'lal')]
df = spark.createDataFrame(data=data)
df.show()
df.printSchema()

# type 2 
# define data and column name 
data = [(1, 'Preethi'),(2, 'Adi'), (3, 'Rathi'), (4, 'lal')]
schema = ['id','name']
df = spark.createDataFrame(data=data, schema=schema)
df.show()
df.printSchema()

#  type 3
#define datatype for schema 

schema = StructType([StructField(name='id',dataType=IntegerType()),StructField(name='name',dataType=StringType()) ])
df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

# type 4 
# column name and data together 

data =[{'id':1, 'name': 'pree'},{'id':2, 'name': 'Adi'}]
df = spark.createDataFrame(data)
df.show()
df.printSchema()

#help functioncan be used . it gives detailed context
print(dir(spark))
help(spark.createDataFrame)