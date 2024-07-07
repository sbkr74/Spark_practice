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

#######################################################################################
# SQL approach
# finding duplicate values
df.createOrReplaceTempView('employees')
query1 = 'SELECT emp_id,emp_name,emp_gender,emp_age,emp_salary,emp_manager FROM employees group by emp_id,emp_name,emp_gender,emp_age,emp_salary,emp_manager having count(*)>1 '
sql_df = spark.sql(query1)
sql_df.show()

#######################################################################################
