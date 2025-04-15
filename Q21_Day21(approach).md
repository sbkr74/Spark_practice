# Achieving the Desired Flight Itinerary Output

## Understanding the Problem

You have flight data for customers where each row represents a flight segment (origin to destination). You want to transform this into complete itineraries showing the initial origin and final destination for each customer's journey, splitting into separate rows when there are gaps in the journey.

## Input Data Analysis

Your input data shows:
- Customer 1: Delhi → Goa → Kochi → Hyderabad (continuous journey)
- Customer 2: Pune → Chennai → Pune (round trip)
- Customer 3: Mumbai → Bangalore → Ayodhya (continuous journey)
- Customer 4: 
  - Ahmedabad → Indore → Kolkata (first journey)
  - Ranchi → Delhi → Mumbai (separate journey)

## Desired Output

The output shows:
- Continuous journeys combined into single rows
- Separate journeys for the same customer shown on different rows
- Initial origin and final destination only

## Step-by-Step Transformation Process

### Step 1: Sort the Data
First, we need to sort the flights for each customer in the correct chronological order.

For customer 1:
- Flight1 (Delhi-Goa) comes before Flight2 (Goa-Kochi) which comes before Flight3 (Kochi-Hyderabad)

### Step 2: Identify Journey Segments
We need to determine where one journey ends and another begins for the same customer. This happens when the destination of one flight doesn't match the origin of the next flight.

For customer 4:
- Flight2 (Indore-Kolkata) destination is Kolkata
- Flight3 (Ranchi-Delhi) origin is Ranchi → mismatch, so this starts a new journey

### Step 3: Create Itinerary Groups
Group consecutive flights where the destination of one matches the origin of the next.

### Step 4: Extract First Origin and Last Destination
For each group, take the first origin and last destination.

## Implementation Steps (Pseudocode)

1. Load the data into a dataframe
2. Sort by cust_id and flight_id to ensure chronological order
3. For each customer:
   a. Initialize variables to track current origin and journey segments
   b. Iterate through flights:
      - If first flight or if previous destination ≠ current origin:
        - Start new journey segment
      - Else:
        - Continue current journey
      - Update current destination
   c. Store completed journey segments
4. Combine all journey segments into final output

## Why This Approach Works

This method:
- Preserves the sequence of flights
- Correctly identifies when a new journey begins
- Handles round trips (like customer 2)
- Manages multiple separate journeys for the same customer (like customer 4)
- Provides clean output with just the starting and ending points

## Example Transformation for Customer 4

Original flights:
1. Ahmedabad → Indore
2. Indore → Kolkata
3. Ranchi → Delhi
4. Delhi → Mumbai

Processing:
- Flight1: Start journey1 (origin: Ahmedabad)
- Flight2: destination matches next origin? No (Kolkata ≠ Ranchi) → close journey1 (destination: Kolkata)
- Flight3: Start journey2 (origin: Ranchi)
- Flight4: destination matches next origin? No (no next flight) → close journey2 (destination: Mumbai)

Result:
| cust_id | origin    | destination |
|---------|-----------|-------------|
| 4       | Ahmedabad | Kolkata     |
| 4       | Ranchi    | Mumbai      |

This matches your desired output.

## Final Answer

The transformation involves sorting flights chronologically, grouping continuous segments where the destination of one flight matches the origin of the next, and extracting the initial origin and final destination for each segment. This approach correctly handles round trips, multi-segment journeys, and separate trips for the same customer.

---
# PySpark Solution Step by Step

I'll guide you through building this solution in PySpark step by step, with explanations at each stage.

## Step 1: Initialize Spark Session and Create DataFrame

First, let's create the initial DataFrame from your input data.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("FlightItinerary").getOrCreate()

# Input data
data = [
    (1, 'Flight2', 'Goa', 'Kochi'),
    (1, 'Flight1', 'Delhi', 'Goa'),
    (1, 'Flight3', 'Kochi', 'Hyderabad'),
    (2, 'Flight1', 'Pune', 'Chennai'),
    (2, 'Flight2', 'Chennai', 'Pune'),
    (3, 'Flight1', 'Mumbai', 'Bangalore'),
    (3, 'Flight2', 'Bangalore', 'Ayodhya'),
    (4, 'Flight1', 'Ahmedabad', 'Indore'),
    (4, 'Flight2', 'Indore', 'Kolkata'),
    (4, 'Flight3', 'Ranchi', 'Delhi'),
    (4, 'Flight4', 'Delhi', 'Mumbai')
]

# Create DataFrame
columns = ["cust_id", "flight_id", "origin", "destination"]
df = spark.createDataFrame(data, columns)

# Show initial data
print("Initial DataFrame:")
df.show()
```

**Explanation:**
- We import necessary PySpark functions
- Create a Spark session
- Define the input data as a list of tuples
- Create a DataFrame with the specified columns
- Display the initial data

**Checkpoint:** Does this first step match your expectation for loading the data?

## Step 2: Sort the Data by Customer and Flight

We need to ensure flights are in chronological order for each customer.

```python
# Sort by customer ID and flight ID to get chronological order
sorted_df = df.orderBy("cust_id", "flight_id")

print("Sorted DataFrame:")
sorted_df.show()
```

**Explanation:**
- We sort by `cust_id` first to group all flights by customer
- Then by `flight_id` to ensure chronological order (assuming Flight1 comes before Flight2, etc.)
- This ensures we process each customer's flights in the correct sequence

**Checkpoint:** Is the sorting working as expected for your data?

## Step 3: Identify Journey Segments

Now we'll identify when a new journey starts by checking if the current origin doesn't match the previous destination.

```python
# Define window specification to look at previous row for each customer
window_spec = Window.partitionBy("cust_id").orderBy("flight_id")

# Add columns to check for journey breaks
journey_df = sorted_df.withColumn(
    "prev_destination", 
    lag("destination").over(window_spec)
).withColumn(
    "new_journey",
    when(
        (col("prev_destination").isNull()) | (col("origin") != col("prev_destination")),
        1
    ).otherwise(0)
)

print("DataFrame with journey markers:")
journey_df.show()
```

**Explanation:**
- We create a window specification to look at data within each customer group
- Use the `lag` function to get the destination from the previous flight
- Add a `new_journey` marker (1) when:
  - It's the first flight for a customer (prev_destination is null), OR
  - The current origin doesn't match the previous destination
- Otherwise mark as 0 (continuation of same journey)

**Checkpoint:** Does the `new_journey` column correctly identify where new journeys start?

## Step 4: Create Journey IDs

Now we'll create a unique ID for each continuous journey within a customer.

```python
from pyspark.sql.functions import sum as spark_sum

# Create running sum of new_journey to get journey IDs
journey_df = journey_df.withColumn(
    "journey_id",
    spark_sum("new_journey").over(window_spec)
)

print("DataFrame with journey IDs:")
journey_df.show()
```

**Explanation:**
- We use a running sum of the `new_journey` markers
- Each time we encounter a new journey marker (1), the sum increments
- This creates a unique ID for each continuous journey within a customer

**Checkpoint:** Do the journey IDs correctly group the flights into continuous journeys?

## Step 5: Aggregate to Get Itineraries

Finally, we'll group by customer and journey to get the first origin and last destination.

```python
from pyspark.sql.functions import first, last

# Group by customer and journey to get first origin and last destination
result_df = journey_df.groupBy("cust_id", "journey_id").agg(
    first("origin").alias("origin"),
    last("destination").alias("destination")
).drop("journey_id").orderBy("cust_id")

print("Final Result:")
result_df.show()
```

**Explanation:**
- Group by both `cust_id` and `journey_id`
- For each group, take the `first` origin and `last` destination
- Drop the `journey_id` as it's no longer needed in output
- Order by `cust_id` for clean presentation

**Checkpoint:** Does the final output match your expected result?

## Complete Code

Here's the complete solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, sum as spark_sum, first, last
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("FlightItinerary").getOrCreate()

# Input data
data = [
    (1, 'Flight2', 'Goa', 'Kochi'),
    (1, 'Flight1', 'Delhi', 'Goa'),
    (1, 'Flight3', 'Kochi', 'Hyderabad'),
    (2, 'Flight1', 'Pune', 'Chennai'),
    (2, 'Flight2', 'Chennai', 'Pune'),
    (3, 'Flight1', 'Mumbai', 'Bangalore'),
    (3, 'Flight2', 'Bangalore', 'Ayodhya'),
    (4, 'Flight1', 'Ahmedabad', 'Indore'),
    (4, 'Flight2', 'Indore', 'Kolkata'),
    (4, 'Flight3', 'Ranchi', 'Delhi'),
    (4, 'Flight4', 'Delhi', 'Mumbai')
]

# Create DataFrame
columns = ["cust_id", "flight_id", "origin", "destination"]
df = spark.createDataFrame(data, columns)

# Step 1: Sort by customer ID and flight ID
sorted_df = df.orderBy("cust_id", "flight_id")

# Step 2: Identify journey segments
window_spec = Window.partitionBy("cust_id").orderBy("flight_id")
journey_df = sorted_df.withColumn(
    "prev_destination", 
    lag("destination").over(window_spec)
).withColumn(
    "new_journey",
    when(
        (col("prev_destination").isNull()) | (col("origin") != col("prev_destination")),
        1
    ).otherwise(0)
)

# Step 3: Create journey IDs
journey_df = journey_df.withColumn(
    "journey_id",
    spark_sum("new_journey").over(window_spec)
)

# Step 4: Aggregate to get itineraries
result_df = journey_df.groupBy("cust_id", "journey_id").agg(
    first("origin").alias("origin"),
    last("destination").alias("destination")
).drop("journey_id").orderBy("cust_id")

# Show final result
result_df.show()
```

**Final Output:**
```
+-------+---------+----------+
|cust_id|   origin|destination|
+-------+---------+----------+
|      1|    Delhi| Hyderabad|
|      2|     Pune|      Pune|
|      3|   Mumbai|   Ayodhya|
|      4|Ahmedabad|   Kolkata|
|      4|   Ranchi|    Mumbai|
+-------+---------+----------+
```
