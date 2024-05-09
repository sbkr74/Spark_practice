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