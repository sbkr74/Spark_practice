# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName('Day_7').getOrCreate()

# flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),  
#  (1,'Flight2' , 'Hyderabad' , 'Kochi'),  
#  (1,'Flight3' , 'Kochi' , 'Mangalore'),  
#  (2,'Flight1' , 'Mumbai' , 'Ayodhya'),  
#  (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')]

# _schema = "cust_id int, flight_id string , origin string , destination string"

# spark_df = spark.createDataFrame(data=flights_data,schema=_schema)
# spark_df.show()
# ###################################################################
# # SQL apprach
# spark_df.createOrReplaceTempView('flight')

# query = "SELECT *,RANK() OVER(PARTITION BY cust_id ORDER BY flight_id) as rank FROM flight"
# sql_df = spark.sql(query)
# # sql_df.show()
# sql_df.createOrReplaceTempView('flight_rank')
# # query1 = "SELECT cust_id,max(case when rank=1 then origin end) as flight_origin FROM flight_rank group by cust_id"
# query1 = '''SELECT 
#     cust_id,
#     MAX(CASE WHEN rank = 1 THEN origin END) AS first_origin,
#     MAX(CASE WHEN rank = (SELECT MAX(rank) FROM flight_rank fr2 WHERE fr2.cust_id = fr1.cust_id) THEN destination END) AS last_destination
# FROM 
#     flight_rank fr1
# GROUP BY 
#     cust_id;
# '''
# sql_df1 = spark.sql(query1)
# sql_df1.show()

###################################################################
flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),  
 (1,'Flight2' , 'Hyderabad' , 'Kochi'),  
 (1,'Flight3' , 'Kochi' , 'Mangalore'),  
 (2,'Flight1' , 'Mumbai' , 'Ayodhya'),  
 (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')]

cols = "cust_id,flight_id,origin,destination"

# pandas dataframe
import pandas as pd
sch = tuple(cols.split(','))
# print(sch)
pandas_df = pd.DataFrame(data=flights_data,columns=sch)
print(pandas_df)
print()
# pandas_df['num'] = pandas_df['cust_id'].rank(method='min')
pandas_df['rn'] = pandas_df.groupby('cust_id').cumcount() +1
print(pandas_df)

####################################################################################
print("++++++++++++++++++++++++++++++++++++++")
# Group by cust_id
grouped = pandas_df.groupby('cust_id')

# Function to get first origin
def get_first_origin(group):
    return group.loc[group['rn'].idxmin(), 'origin']

# Function to get last destination
def get_last_destination(group):
    return group.loc[group['rn'].idxmax(), 'destination']

# Apply the functions
first_origins = grouped.apply(get_first_origin,include_groups=False).reset_index(name='first_origin')
last_destinations = grouped.apply(get_last_destination,include_groups=False).reset_index(name='last_destination')

# # Apply the functions
# first_origins = grouped.apply(lambda x: get_first_origin(x.drop(columns=['cust_id']))).reset_index(name='first_origin')
# last_destinations = grouped.apply(lambda x: get_last_destination(x.drop(columns=['cust_id']))).reset_index(name='last_destination')


# Merge the results
result = pd.merge(first_origins, last_destinations, on='cust_id')

print(result)