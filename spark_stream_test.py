# from pyspark.sql import SparkSession
# from pyspark.sql.types import *

# spark = SparkSession.builder \
#     .appName("FileStream") \
#     .config("spark.hadoop.io.native.lib.available", "false") \
#     .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
#     .getOrCreate()

# input_path = r"D:\Learning\Spark_practice\data\test\sample1.json"

# # Define the schema for the JSON files
# schema = StructType([
#     StructField("fruit", StringType(), True),
#     StructField("size", StringType(), True),
#     StructField("color", StringType(), True),
#     StructField("quantity", IntegerType(), True)
#     # Add more fields as necessary
# ])

# # Read the JSON files as a streaming DataFrame
# df = spark.readStream \
#     .schema(schema) \
#     .format("json") \
#     .option("maxFilesPerTrigger", 1) \
#     .load(input_path)

# # Example processing: Select specific columns
# processed_df = df.select("quantity", "size")

# query = processed_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()

######################################################################################
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Set Hadoop home and add winutils to the PATH
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

# Create Spark session with necessary configurations
spark = SparkSession.builder \
    .appName("FileStream") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()

input_path = r"D:\Learning\Spark_practice\data\test2"
checkpoint_path = r"D:\Learning\Spark_practice\checkpoint"
spark.conf.set("spark.sql.streaming.schemaInference",True)
# Define the schema for the JSON files
schema = StructType([
    StructField("fruit", StringType(), True),
    StructField("size", StringType(), True),
    StructField("color", StringType(), True),
    StructField("quantity", IntegerType(), True)
])

# Read the JSON files as a streaming DataFrame
df = spark.readStream \
    .schema(schema) \
    .format("json") \
    .option("cleanSource","archive")\
    .option("sourceArchiveDir","archive_dir")\
    .option("maxFilesPerTrigger", 1) \
    .load(input_path)

# Example processing: Select specific columns
processed_df = df.select("quantity", "size")

query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()
