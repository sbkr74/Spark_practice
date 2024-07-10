from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName('Day_10')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation, G1 Concurrent GC") \
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation, G1 Concurrent GC") \
        .getOrCreate()

sampleData=[(1,"Van dijk",23),
 (2,"NULL",32),
 (3,"Fabinho","NULL"),
 (4,"NULL","NULL"),
 (5,"Kaka","NULL")]

df = spark.createDataFrame(sampleData).toDF("id","name","age")
df.show()

###################################################################
# SQL Approach
df.createOrReplaceTempView('data')
query = """SELECT 
  (SELECT COUNT(*) FROM data WHERE id = 'NULL') AS id,
  (SELECT COUNT(*) FROM data WHERE name = 'NULL') AS name,
  (SELECT COUNT(*) FROM data WHERE age = 'NULL') AS age;
"""
sql_df = spark.sql(query)
sql_df.show()