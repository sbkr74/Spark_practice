from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce,when

spark = SparkSession.builder.appName('vid2').getOrCreate()

data = [("Goa","","AP"),("","AP",None),(None,"","Bglr")]
column = ["city1","city2","city3"]

df = spark.createDataFrame(data,column)
df.show()

df1 = df.withColumn("city",coalesce(\
    when(df.city1=="",None).otherwise(df.city1) ,\
    when(df.city2=="",None).otherwise(df.city2),\
    when(df.city3=="",None).otherwise(df.city3)))

df1.select(df1.city).show()