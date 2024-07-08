from pyspark.sql import SparkSession

# Create a SparkSession with the desired GC configurations
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation, G1 Concurrent GC") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation, G1 Concurrent GC") \
    .getOrCreate()

_data = [(1,'Abbot'),(2,'Doris'),(3,'Emerson'),(4,'Green'),(5,'Jeames')]
_schema = ['id', 'student']

df = spark.createDataFrame(_data,_schema)
df.show()

#########################################################
# SQL approach
df.createOrReplaceTempView('students')
query = 'select id,case when id = 1 then 2 when id = 3 then 4  else id end as new_id,student  from students where id in(1,2,3,4,5)'
sql_df = spark.sql(query)
sql_df.show()
