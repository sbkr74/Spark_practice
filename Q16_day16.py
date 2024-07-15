from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Day_16')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

# Create data as a list of dictionaries (key-value pairs)
data = [
    {"customer_id": 1, "name": "John Doe", "email": "john.doe@gmail.com", "phone": "123-456-7890", "registration_date": "2022-01-15"},
    {"customer_id": 2, "name": "Jane Smith", "email": "jane.smith@hotmail.com", "phone": "(987)654-3210", "registration_date": "2021-11-30"},
    {"customer_id": 3, "name": "Alice Lee", "email": "alice.lee@yahoo.com", "phone": "555-5555", "registration_date": "2023-03-10"},
    {"customer_id": 4, "name": "Bob Brown", "email": "bob.brown@gmail.com", "phone": None, "registration_date": "2022-05-20"}
]

df = spark.createDataFrame(data)

# cleaning
df = df.withColumn("phone",regexp_replace("phone","[^0-9]",""))

# fill null values
df = df.fillna({"phone":"N/A"})

# renaming column
df = df.withColumnRenamed("name","full_name")

# changing datatype of column
df1 = df.withColumn("registration_date",to_date(df['registration_date'],"yyyy-MM-dd"))

df.show()
