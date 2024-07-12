from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data
sampleData=[(1,"A",20,"31|32|34"),
 (2,"B",21,"21|32|43"),
 (3,"C",22,"21|32|11"),
 (4,"D",23,"10|12|12")]

# SparkSession
spark = SparkSession.builder.appName('Day_13')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

# Dataframe
df = spark.createDataFrame(sampleData).toDF("ID","NAME","AGE","MARKS")
df1 = df.withColumn("MARKS",split("MARKS","\\|")\
        .cast("array<int>")\
        .alias("marks"))\
        .withColumn("Physics",col("marks").getItem(0).cast("int"))\
        .withColumn("Chemistry",col("marks").getItem(1).cast("int"))\
        .withColumn("Maths",col("marks").getItem(2).cast("int"))\
        .drop("MARKS")
df1.show()
        