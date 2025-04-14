from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lag,when,lead
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Day 21").getOrCreate()

data = [(1, 'Flight2', 'Goa', 'Kochi'),
(1, 'Flight1', 'Delhi', 'Goa'),
(1, 'Flight3', 'Kochi', 'Hyderabad'),
(2, 'Flight1', 'Pune', 'Chennai'),
(2, 'Flight2', 'Chennai', 'Pune'),
(3, 'Flight1', 'Mumbai', 'Bangalore'),
(3, 'Flight2', 'Bangalore', 'Ayodhya'),
(4, 'Flight1', 'Ahmedabad', 'Indore'),
(4, 'Flight2', 'Indore', 'Kolkata'),
(4, 'Flight3', 'Ranchi', 'Delhi'),
(4, 'Flight4', 'Delhi', 'Mumbai')]

df = spark.createDataFrame(data).toDF("cust_id","flight_id","origin","destination")

# adding a column which contains prev destination
windowspec = Window.partitionBy(col("cust_id")).orderBy(col("flight_id"))

df1 = df.withColumn("prev_destination",lag(col("destination")).over(windowspec))
df2 = df1.withColumn("partition",when(col("origin") == col("prev_destination"),1).otherwise(0))

df3 = df2.withColumn("new_origin",when(col("partition")==0,col("origin")).otherwise(lead(col("partition"),1,0).over(windowspec)))
df3.show()

df4 = df3.withColumn("new_destination",when(col("new_origin")==0,col("destination")).otherwise(col("new_origin")))
df4.show()