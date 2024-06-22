from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
# from pyspark import SparkConf, SparkContext

# # Create a Spark configuration and Spark context
# conf = SparkConf().setAppName("LogLevelExample")
# sc = SparkContext(conf=conf)

# # Set log level
# sc.setLogLevel("OFF")  # Options: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

spark = SparkSession.builder.appName('Day6').getOrCreate()
data1 = [(1,"Sagar" , "CSE" , "UP" , 80),(2,"Shivani" , "IT" , "MP", 86),(3,"Muni", "Mech" , "AP", 70)] 
data1_schema = ("ID" , "Student_Name" ,"Department_Name" , "City" , "Marks")
df1 = spark.createDataFrame(data=data1,schema=data1_schema)
# df1.show()

data2 = [(4, "Raj" , "CSE" , "HP") ,(5 , "Kunal" , "Mech" , "Rajasthan")]
data2_scehma = ("ID" , "Student_Name" , "Department_Name" , "City")
df2 = spark.createDataFrame(data2,data2_scehma)
# df2.show()

# merge
df2 = df2.withColumn('Marks',lit(None))

df = df1.union(df2)
df.show()

# pandas approach 
import pandas as pd
pd_df1 = pd.DataFrame(data1,columns=data1_schema)

# Setting "ID" as the index
pd_df1.set_index('ID', inplace=True)

# Displaying DataFrame
# print(pd_df1)

pd_df2 = pd.DataFrame(data2,columns=data2_scehma)

# Setting "ID" as the index
pd_df2.set_index('ID', inplace=True)

# Displaying DataFrame
# print(pd_df2)

pd_df = pd.concat([pd_df1,pd_df2])
print(pd_df)