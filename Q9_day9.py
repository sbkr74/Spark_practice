from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

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
# sql_df.show()

#########################################################
# Spark DataFrame Approach
df_lead = df.withColumn('next_val',lead(col('student')).over(Window.orderBy(col('id'))))
df_lag = df_lead.withColumn('prev_val',lag(col('student')).over(Window.orderBy(col('id'))))
spark_df = df_lag.withColumn('Exchange_seat',when(col('id')%2==1,coalesce(col('next_val'),col('student'))).otherwise(col('prev_val')))
spark_df.select("id","student","Exchange_seat").show()