from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName('Day_7').getOrCreate()

flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),  
 (1,'Flight2' , 'Hyderabad' , 'Kochi'),  
 (1,'Flight3' , 'Kochi' , 'Mangalore'),  
 (2,'Flight1' , 'Mumbai' , 'Ayodhya'),  
 (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')]

_schema = "cust_id int, flight_id string , origin string , destination string"

spark_df = spark.createDataFrame(data=flights_data,schema=_schema)
# spark_df.show()

# df1 = spark_df.withColumn("order", row_number().over(Window.partitionBy(col("cust_id")).orderBy(col("flight_id"))))
# df1.show()

###################################################################
# pandas dataframe
import pandas as pd
sch = tuple(_schema.split(','))
print(sch)
pandas_df = pd.DataFrame(data=flights_data,columns=sch)
print(pandas_df)
