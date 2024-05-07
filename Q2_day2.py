import pandas as pd
import random
from datetime import datetime, timedelta

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
# print(df)
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++