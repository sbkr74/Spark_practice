import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
# Function to generate random match results
def generate_match_result():
    return random.choice(['Win', 'Loss'])

# Function to generate dataset
def generate_dataset(num_players, num_matches):
    player_ids = []
    match_dates = []
    match_results = []
    opponents = []

    for _ in range(num_matches):
        for player_id in range(1, num_players + 1):
            player_ids.append(player_id)
            match_dates.append((datetime.now() - timedelta(days=random.randint(1, 30))).date())
            match_results.append(generate_match_result())
            opponents.append(random.randint(1, num_players))

    df = pd.DataFrame({
        'player_id': player_ids,
        'match_date': match_dates,
        'match_result': match_results,
        'opponent_id': opponents
    })

    return df

# Generate dataset with 10 players and 20 matches per player
df = generate_dataset(10, 20)
# ================================================================================================
# ================================================================================================
spark =  SparkSession.builder.appName("day2").getOrCreate()

player_df = spark.createDataFrame(df)
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Using Spark-SQL

# Create temporary views for Spark DataFrames
player_df.createOrReplaceTempView("tennis_player")

# Writing SQL CTE query to fetch max consecutive wins by a player.
query = '''with cte as(select * ,row_number() over(partition by player_id order by match_date) as rn
from tennis_player), cte2 as(select * ,rn - row_number() over(partition by player_id order by match_date) as rn_diff
from cte 
where match_result = 'Win'
) ,cte3 as (select player_id,count(1) as group_count from cte2 group by rn_diff,player_id)
,cte4 as (select player_id,max(group_count) as win_streak from cte3 group by player_id)
select player_id, win_streak from cte4 where win_streak = 
(select max(win_streak) from cte4)
'''
result = spark.sql(query)

# Displaying the desired output i.e  max consecutive wins by a player and his/her player_id
result.show()
print(result.count())
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Using Spark Dataframe
