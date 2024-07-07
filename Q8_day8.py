from pyspark.sql import SparkSession
from pyspark.sql.types import *
schema = StructType([
 StructField("emp_id", IntegerType(), True),
 StructField("emp_name", StringType(), True),
 StructField("emp_gender", StringType(), True),
 StructField("emp_age", IntegerType(), True),
 StructField("emp_salary", IntegerType(), True),
 StructField("emp_manager", StringType(), True)
])

data = [
 (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
 (2, "Aarav Sharma", "Male", 28, 55000, "Zara Singh"),
 (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel"),
 (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
 (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
 (6, "Naina Verma", "Female", 31, 72000, "Arjun Patel"),
 (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
 (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
 (5, "Aditya Kapoor", "Male", 28, 58000, "Zara Singh"),
 (10, "Anaya Joshi", "Female", 27, 59000, "Aarav Sharma"),
 (11, "Rohan Malhotra", "Male", 36, 73000, "Zara Singh"),
 (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel")
]

spark = SparkSession.builder.appName('Day_8').getOrCreate()
df = spark.createDataFrame(data,schema)
df.show()

#######################################################################################
# SQL approach
# finding unique values
df.createOrReplaceTempView('employees')
query1 = 'SELECT DISTINCT(*) FROM employees'
sql_df = spark.sql(query1)
sql_df.show()

#######################################################################################
# Spark DataFrame Approach
# finding unique values
spark_df = df.dropDuplicates()
spark_df.show()