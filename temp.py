import pandas as pd

# Sample data
data = {
    'cust_id': [1, 1, 1, 2, 2, 3],
    'rank': [1, 2, 3, 1, 2, 1],
    'origin': ['A', 'B', 'C', 'D', 'E', 'F'],
    'destination': ['X', 'Y', 'Z', 'W', 'V', 'U']
}

df = pd.DataFrame(data)

# Group by cust_id
grouped = df.groupby('cust_id', group_keys=False)

# Function to get first origin
def get_first_origin(group):
    return group.loc[group['rank'].idxmin(), 'origin']

# Function to get last destination
def get_last_destination(group):
    return group.loc[group['rank'].idxmax(), 'destination']

# Apply the functions
first_origins = grouped.apply(get_first_origin).reset_index(name='first_origin')
last_destinations = grouped.apply(get_last_destination).reset_index(name='last_destination')

# Merge the results
result = pd.merge(first_origins, last_destinations, on='cust_id')

print(result)
