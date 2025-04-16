from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Day 22').getOrCreate()

data = [
 ('A1', 'ADULT', 54),
 ('A2', 'ADULT', 53),
 ('A3', 'ADULT', 52),
 ('A4', 'ADULT', 58),
 ('A5', 'ADULT', 54),
 ('C1', 'CHILD', 20),
 ('C2', 'CHILD', 19),
 ('C3', 'CHILD', 22),
 ('C4', 'CHILD', 15)
 ]

df = spark.createDataFrame(data).toDF("person","type","age")
df.show()