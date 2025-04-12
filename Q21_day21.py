from pyspark.sql import SparkSession
from pyspark.sql.functions import col,rank,min,max,when
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

# adding column which will have rank based on cust_id 
df1 = df.withColumn("order",rank().over(Window.partitionBy(col("cust_id")).orderBy(col("flight_id"))))

# grouped on cust_id column and getting min and max order no of each grouped item.  
df2 = df1.groupBy(col("cust_id")).agg(min(col("order")).alias('start'),max(col("order")).alias('end'))

# joining both dataframe and remove common column
df_join = df2.join(df1,on=(df1.cust_id==df2.cust_id)).drop(df2.cust_id)

# again grouped by cust_id column but getting Origin and destination based on condition of min and max.
df_result = df_join.groupBy(col("cust_id")).agg(min(when(col('order') == col('start'),col("origin"))).alias("Orign"),max(when(col("order") == col("end"),col("destination"))).alias("Destination"))
df_result.show()


'''
Problem: Discontinuity in flight origin and destination.
solution coming up next.
'''