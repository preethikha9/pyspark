from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate() 
spark.sparkContext.setLogLevel("WARN")
print(spark)
rdd=spark.sparkContext.parallelize([1,2,5,7,6])
print("count")
print(rdd.count())

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

df.printSchema()

df.show()