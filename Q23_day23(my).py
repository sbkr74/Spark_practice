from pyspark.sql import SparkSession
from datetime import date

spark = SparkSession.builder.appName('Day 23').getOrCreate()

data = [
 (1, date(2017, 1, 1), 10),
 (2, date(2017, 1, 2), 109),
 (3, date(2017, 1, 3), 150),
 (4, date(2017, 1, 4), 99),
 (5, date(2017, 1, 5), 145),
 (6, date(2017, 1, 6), 1455),
 (7, date(2017, 1, 7), 199),
 (8, date(2017, 1, 8), 188),
 (9, date(2017, 1, 9), 90)
 ]

schema = "id int,visit_date date,people int"

df = spark.createDataFrame(data,schema=schema)
df.printSchema()
df.show()