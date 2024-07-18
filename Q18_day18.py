from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName('Day_17')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

data = [
  { "ID": 1, "movie": "War", "description": "great 3D", "rating": 8.9 },
  { "ID": 2, "movie": "Science", "description": "fiction", "rating": 8.5 },
  { "ID": 3, "movie": "irish", "description": "boring", "rating": 6.2 },
  { "ID": 4, "movie": "Ice song", "description": "Fantacy", "rating": 8.6 },
  { "ID": 5, "movie": "House card", "description": "Interesting", "rating": 9.1 }
]

df = spark.createDataFrame(data)
df.show()
