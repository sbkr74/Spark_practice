from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data
sampleData=[(1,"A",20,"31|32|34"),
 (2,"B",21,"21|32|43"),
 (3,"C",22,"21|32|11"),
 (4,"D",23,"10|12|12")]

# SparkSession
spark = SparkSession.builder.appName('Day_13').getOrCreate()
df = spark.createDataFrame(sampleData).toDF("ID","NAME","AGE","MARKS")
df.show()
df1 = df.withColumn("MARKS",split("MARKS","\\|"))
df1.show()
df2 = df1.withColumn("Physics",col('MARKS').getItem(0))\
         .withColumn("Chemistry",col("MARKS").getItem(1))\
         .withColumn("Maths",col("MARKS").getItem(2))
df2 = df2.drop("MARKS")
df2.show()