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
df = spark.sql("select *, date_part('month',created_at) as month from revenue order by created_at,month")
df.show(df.count())
print(df.count())

df1 = spark.sql("select sum(value) as total, date_part('month',created_at) as month from revenue group by month order by month")
df1.show(df1.count())
print(df1.count())

df1.createOrReplaceTempView("monthly_revenue")
df2 = spark.sql("select t1.*,round((((t1.total-t2.total)/t2.total)*100),2) as diff_per from monthly_revenue t1 left join monthly_revenue t2 on t1.month=t2.month+1 order by month")
df2.show()