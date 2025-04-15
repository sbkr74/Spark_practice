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
