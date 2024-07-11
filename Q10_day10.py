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
# # SQL Approach
# df.createOrReplaceTempView('data')
# # query = """SELECT 
# #   (SELECT COUNT(*) FROM data WHERE id = 'NULL') AS id,
# #   (SELECT COUNT(*) FROM data WHERE name = 'NULL') AS name,
# #   (SELECT COUNT(*) FROM data WHERE age = 'NULL') AS age;
# # """
# another_query = """
# SELECT 
#   COUNT(CASE WHEN id = 'NULL' THEN 1 END) AS id,
#   COUNT(CASE WHEN name = 'NULL' THEN 1 END) AS name,
#   COUNT(CASE WHEN age = 'NULL' THEN 1 END) AS age
# FROM data;
# """
# sql_df = spark.sql(another_query)
# sql_df.show()

#################################################################
# spark_df = df.select("id","age").filter(col('age')!='NULL')
spark_df = df.select([count(when(col(i)=='NULL',i)).alias(i) for i in df.columns])

spark_df.show()