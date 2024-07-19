from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName('Day_17')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

data = [( 
'3000' , '22-may'), 
('5000' , '23-may'),
('5000' , '25-may'),
('10000' , '22-june'), 
('1250' , '03-july')]
schema = ['revenue','date']

df = spark.createDataFrame(data,schema)
df.show()