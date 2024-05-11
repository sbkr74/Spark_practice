import pandas as  pd
import random
from datetime import datetime, timedelta

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
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("day3").getOrCreate()
spark_df = spark.createDataFrame(pandas_df)
# spark_df.show(spark_df.count())

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
print(df.count())