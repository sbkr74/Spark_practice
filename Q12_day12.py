from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName('Day_12').getOrCreate()

# Reading text file and stored it into dataframe 
df = spark.read.text('data\\test.txt')
df.show()

df1 = df.withColumn("new_value",regexp_replace('value','(.*?\\-){3}',"$0,"))
df1 = df1.drop('value')
df2 = df1.withColumn("new_value",split("new_value",","))
df2 = df2.withColumn("new_value",explode("new_value"))
df2 = df2.withColumn("new_value",split("new_value","-"))
df2 = df2.withColumn("id",col('new_value').getItem(0))\
         .withColumn("name",col('new_value').getItem(1))\
         .withColumn("age",col('new_value').getItem(0))
df2 = df2.drop('new_value')
df2.show()