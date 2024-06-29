from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Day_7').getOrCreate()

flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),  
 (1,'Flight2' , 'Hyderabad' , 'Kochi'),  
 (1,'Flight3' , 'Kochi' , 'Mangalore'),  
 (2,'Flight1' , 'Mumbai' , 'Ayodhya'),  
 (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')]

_schema = "cust_id int, flight_id string , origin string , destination string"

spark_df = spark.createDataFrame(data=flights_data,schema=_schema)
spark_df.show()

