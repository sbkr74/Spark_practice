import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import random

# Create SparkSession
spark = SparkSession.builder \
    .appName("Day1_practice") \
    .getOrCreate()

# Dummy data for instacart_reviews
reviews_data = {
    "id": list(range(1, 501)),
    "customer_id": [random.randint(1, 100) for _ in range(500)],
    "store_id": [random.randint(1, 100) for _ in range(500)],
    "score": [random.randint(1, 10) for _ in range(500)]
}

# Convert to Pandas DataFrame
reviews_df_pandas = pd.DataFrame(reviews_data)

# Convert to Spark DataFrame
reviews_df_spark = spark.createDataFrame(reviews_df_pandas)

# Dummy data for instacart_stores
stores_data = {
    "id": list(range(1, 101)),
    "name": [f"Store_{i}" for i in range(1, 101)],
    "zipcode": [random.randint(10000, 99999) for _ in range(100)],
    "opening_date": []
}

# Generate opening dates with different time ranges
for _ in range(100):
    year = random.choice([2020,2021,2022,2023])
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour_range = random.choice([(8, 11), (12, 16), (17, 20)])
    opening_hour = random.randint(hour_range[0], hour_range[1])
    opening_date = datetime(year, month, day, opening_hour)
    stores_data["opening_date"].append(opening_date)

# Convert to Pandas DataFrame
stores_df_pandas = pd.DataFrame(stores_data)

# Convert to Spark DataFrame
stores_df_spark = spark.createDataFrame(stores_df_pandas)

# Create temporary views for Spark DataFrames
reviews_df_spark.createOrReplaceTempView("instacart_reviews")
stores_df_spark.createOrReplaceTempView("instacart_stores")

query = """with CTE as (
            select s.name,r.score,case when r.score>5 then 'positive' else 'negative' end as review from instacart_stores s join instacart_reviews r on s.id = r.store_id 
            where date_part('month',s.opening_date)>=6 and date_part('year',s.opening_date)=2021
) , CTE2 as(
select name,sum(case when review = 'positive' then 1 else 0 end) as pos_count,
sum(case when review = 'negative' then 1 else 0 end) as neg_count,count(1) as group_count from CTE
group by name)
select name,round((pos_count*100/group_count),2) as pos_ratio,round((neg_count*100/group_count),2) as neg_ratio from CTE2
where round((neg_count*100/group_count),2) >20
"""
result = spark.sql(query)
print("Records in result:")
result.show(result.count())
print("Values in result:",result.count())