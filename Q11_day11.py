from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Day_11')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

# Define the schema for the DataFrame
schema1=StructType([
 StructField("ID", IntegerType()),
 StructField("Name", StringType()),
 StructField("PhoneNo", StringType())
])

# Data to be inserted into the DataFrame
sampleData1=[(1,"Martinez","U795342iy"),
 (2,"Rodri","7903280317"),
 (3,"Mane","sh987122e9"),]

df = spark.createDataFrame(sampleData1,schema1)
# df.show()

################################################
# Spark SQL approach
df.createOrReplaceTempView('records')
# query = "SELECT * FROM records WHERE LEN(PhoneNo) = 10 and PhoneNo REGEXP '^[0-9]+$' "
query = "select * from records where PhoneNo rlike '^[0-9]{10}$'"
sql_df = spark.sql(query)
sql_df.show()

################################################
# Spark DataFrame approach
spark_df = df.filter(col('PhoneNo').rlike('^[0-9]{10}$'))
spark_df.show()