from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Day_15')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

data = [
    ('A','2023-01-15','100.0'),
    ('B','2023-01-20','150.0'),
    ('A','2023-02-10','120.0'),
    ('B','2023-02-15','180.0'),
    ('C','2023-03-05','200.0'),
    ('A','2023-03-10','250.0')
]

df = spark.createDataFrame(data).toDF('product_id','sales_date','sales_amount')


df = df.withColumn("sales_date",to_date(df['sales_date'],'yyyy-MM-dd'))\
    .withColumn("sales_amount",col('sales_amount').cast("float"))\
    .withColumn("month",date_format('sales_date',"yyyy-MM"))
df.show()
