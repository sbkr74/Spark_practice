from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *


spark = SparkSession.builder.appName('Day_17')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

orders = spark.read.option("delimiter", "|").csv("data\orders.csv",header=True,inferSchema = True)
orders.show()

order_items = spark.read.option("delimiter","|").csv("data\order_items.csv",header=True,inferSchema=True)
order_items.show()