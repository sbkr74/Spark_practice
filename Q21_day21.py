from pyspark.sql import SparkSession
from pyspark.sql.functions import col,rank,min,max
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

df1 = df.withColumn("order",rank().over(Window.partitionBy(col("cust_id")).orderBy(col("flight_id"))))
df2 = df1.groupBy(col("cust_id")).agg(min(col("order")).alias('start'),max(col("order")).alias('end'))

df_join = df2.join(df1,on=(df1.cust_id==df2.cust_id))
df_join.show()