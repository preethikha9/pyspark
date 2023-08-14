from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create SparkSession 
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate() 
spark.sparkContext.setLogLevel("WARN")

#single csv file
df = spark.read.csv(path='data/example1.csv',header=True)
df.show()
df.printSchema()

#multiple csv file as list 

df = spark.read.csv(path=['data/example1.csv','data/example2.csv'],header=True)
df.show()
df.printSchema()


#all csv file 
#specify only the path 
df = spark.read.csv(path='data/',header=True)
df.show()
df.printSchema()


#different way instead of read.csv
df = spark.read.format('csv').option(key='header',value='True').load(path='data/example1.csv')
df.show()
df.printSchema()


#all the schema type is going to be string when read from csv 
# we can define as schema parameter 
#even if set nullable as false it is going to return true because official code has error
schema=StructType().add(field='id',data_type=IntegerType(),nullable=False).add(field='name', data_type=StringType(),nullable=False)
df = spark.read.csv(path='data/example1.csv',schema=schema, header=True)
df.show()
df.printSchema()





