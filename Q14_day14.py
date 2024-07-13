from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Day_14')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

data = [
 (1, "Gaurav",["Pune", "Bangalore", "Hyderabad"]),
 (2, "Rishabh",["Mumbai", "Bangalore", "Pune"])
]

df = spark.createDataFrame(data).toDF("EmpId","Name","Locations")
df1 = df.withColumn("Location",explode(col("Locations"))).drop("Locations")
df1.show()

