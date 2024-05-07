import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import random
import pyspark.sql.functions as F

# Create SparkSession
spark = SparkSession.builder \
    .appName("Day1_practice") \
    .getOrCreate()

# ==========================================================================================================================
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
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Create temporary views for Spark DataFrames
reviews_df_spark.createOrReplaceTempView("instacart_reviews")
stores_df_spark.createOrReplaceTempView("instacart_stores")
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
# ==========================================================================================================================
# Approach 2:
# step 1: joined two dfs(inner)
joined_df = reviews_df_spark.join(stores_df_spark,reviews_df_spark.store_id == stores_df_spark.id,"inner")

#stpe 2: filtered df(year = `2021`,month = 2nd half of the year)
filtered_df = joined_df.filter((F.year('opening_date')==F.lit('2021')) & (F.month('opening_date')>=F.lit('6')))

#step 3: added a column `review`(if rating is below 5 then `Negative` else `Positive`)
review_df = filtered_df.withColumn('review',F.when((filtered_df.score>5),F.lit("Positive")).otherwise(F.lit("Negative")))

# step 4: grouped and aggregiated (getting sum of positive and negative and total sum of both when group together based on `name`)
review_rate_df= review_df.withColumn('pos',F.when((review_df.review == 'Positive'), F.lit(1)).otherwise(F.lit(0)))\
    .withColumn('neg',F.when((review_df.review == 'Negative'), F.lit(1)).otherwise(F.lit(0)))\
    .groupby('name')\
    .agg(F.sum('pos').alias('pos_sum'),F.sum('neg').alias('neg_sum'))\
    .withColumn('group_count',F.expr("pos_sum+neg_sum"))

# getting ratio((positive/total) and (negative/total))
review_ratio_df = review_rate_df.withColumn('pos_ratio',F.round(F.col('pos_sum')*100/F.col('group_count'),2))\
                    .withColumn('neg_ratio',F.round(F.col('neg_sum')*100/F.col('group_count'),2))

# filtering(show results only greater than 20% of negative ratio)
review_ratio_df = review_ratio_df.filter(F.col('neg_ratio')>20).select('name','pos_ratio','neg_ratio')

# Display the final output
review_ratio_df.show(review_ratio_df.count())
print(review_ratio_df.count())