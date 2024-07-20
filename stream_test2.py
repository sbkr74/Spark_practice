from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("StreamExample") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

input_path = r"D:\Learning\Spark_practice\files"
checkpoint_path = r"D:\Learning\Spark_practice\checkpoint"

df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .load(input_path)

query = df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()
