import pandas as  pd
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, date_format, sum, lag, round

# Creating spark sesseion
spark = SparkSession.builder.appName("day3").getOrCreate()

# Function to generate sample data based on the provided schema
def generate_data(num_records):
    data = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    for i in range(num_records):
        # Generate a random purchase value
        value = random.randint(500, 1500)
        # Generate a random date within the year range
        created_at = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        # Append data to the list
        data.append({
            'id': i + 1,
            'created_at': created_at,
            'value': value,
            'purchase_id': (i +100)
        })    
    return data

# Generate sample data
sample_data = generate_data(500)
pandas_df = pd.DataFrame(sample_data)
####################################################################################################################
####################################################################################################################
# Spark SQL approach
# pandas dataframe to spark dataframe
# Creating spark sesseion
spark = SparkSession.builder.appName("day3").getOrCreate()
spark_df = spark.createDataFrame(pandas_df)

# creating Temporary View to work with SPARK_SQL
spark_df.createOrReplaceTempView("revenue")
query = """WITH 
CTE AS (
    SELECT *, date_format(created_at, 'yyyy-MM') AS year_month 
    FROM revenue
),
CTE2 AS (
    SELECT sum(value) AS total, year_month,date_part('month',year_month) as month
    FROM CTE 
    GROUP BY year_month 
    ORDER BY year_month
)
SELECT t1.*,round((((t1.total-t2.total)/t2.total)*100),2) as diff_count 
FROM CTE2 t1 LEFT JOIN CTE2 t2 on t1.month = t2.month+1 
ORDER BY year_month
"""
df = spark.sql(query)
df.show()
# =================================================================================================================
# Spark Dataframe Approach

# Suppress the warning message
spark.sparkContext.setLogLevel("ERROR")

sp_df = spark_df.withColumn('year_month',date_format('created_at','yyyy-MM'))\
        .groupBy('year_month')\
        .agg(sum('value').alias('month_revenue'))\
        .withColumn('last_month_revenue',lag(col('month_revenue'))\
        .over(Window.orderBy('year_month')))\
        .withColumn('revenue_diff',round((col('month_revenue')-col('last_month_revenue'))*100/col('last_month_revenue'),2))\
        .select(col('year_month'),col('revenue_diff'))
sp_df.show()
spark.stop()