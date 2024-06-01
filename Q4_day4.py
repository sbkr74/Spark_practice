from pyspark.sql import SparkSession
from pyspark.sql.functions import col , count ,when, sum
# Creating Spark Session
spark = SparkSession.builder.appName('day4').getOrCreate()

employee = [(1, 'Sagar' ,23),(2, None , 34),(None ,'John' , 46),(5,'Alex', None) , (4,'Alice',None)]
employee_schema = "emp_id int,name string,age int"
emp_df = spark.createDataFrame(data = employee,schema =employee_schema)


emp_df.createOrReplaceTempView('employees')
 
query = """
SELECT 
    SUM(CASE WHEN emp_id IS NULL THEN 1 ELSE 0 END) AS count_emp_id_null,
    SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS count_name_null,
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS count_age_null
FROM employees
"""


sql_df = spark.sql(query)
sql_df.show()

# Create a DataFrame to hold the count of NULL values for each column
null_counts = emp_df.select(
    sum(col("emp_id").isNull().cast("int")).alias("count_emp_id_null"),\
    sum(col("name").isNull().cast("int")).alias("count_name_null"),\
    sum(col("age").isNull().cast("int")).alias("count_age_null")\
)

null_counts.show()

sp_df = emp_df.select([count(when(col(i).isNull(),1)).alias(i) for i in emp_df.columns])
sp_df.show()

spark.stop()