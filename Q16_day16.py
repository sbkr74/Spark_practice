from pyspark.sql import SparkSession,Window
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
df = df.withColumn("registration_date",to_date(df['registration_date'],"yyyy-MM-dd"))

# adding column with condition
df1 = df.withColumn("year",year("registration_date"))

# df1 = df1.withColumn("age",row_number().over(Window.partitionBy('year').orderBy(df1['year'].desc())))
# Find the maximum year
max_year = df1.select(max(col("year"))).collect()[0][0]
df1 = df1.withColumn('age',max_year-col("year")).drop('year')
df1.show()
################################################################################################
# Load the source data into a Spark DataFrame
customer_data = spark.createDataFrame([
 (1, "John Doe", "john.doe@gmail.com", "123-456-7890", "2022-01-15"),
 (2, "Jane Smith", "jane.smith@hotmail.com", "(987)654-3210", "2021-11-30"),
 (3, "Alice Lee", "alice.lee@yahoo.com", "555-5555", "2023-03-10"),
 (4, "Bob Brown", "bob.brown@gmail.com", None, "2022-05-20")
], ["customer_id", "name", "email", "phone", "registration_date"])

from pyspark.sql.functions import regexp_replace, when, col
from pyspark.sql.types import IntegerType

# Clean the phone numbers to remove non-numeric characters
customer_data = customer_data.withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))

# Fill in missing values in the phone column with "N/A"
customer_data = customer_data.withColumn("phone", when(customer_data["phone"] == "", "N/A").otherwise(customer_data["phone"]))

# Rename the columns for clarity
customer_data = customer_data.withColumnRenamed("name", "full_name")

# Convert the registration_date column to a proper date format
customer_data = customer_data.withColumn("registration_date", col("registration_date").cast("date"))

customer_data = customer_data.withColumn(
    "age",
    when(
        customer_data["registration_date"].isNotNull(),
        year(current_date()) - year(customer_data["registration_date"])
    ).otherwise(None)
)
# Show the transformed data
customer_data.show()