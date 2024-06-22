from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName('Day6').getOrCreate()
data1 = [(1,"Sagar" , "CSE" , "UP" , 80),(2,"Shivani" , "IT" , "MP", 86),(3,"Muni", "Mech" , "AP", 70)] 
data1_schema = ("ID" , "Student_Name" ,"Department_Name" , "City" , "Marks")
df1 = spark.createDataFrame(data=data1,schema=data1_schema)
df1.show()

data2 = [(4, "Raj" , "CSE" , "HP") ,(5 , "Kunal" , "Mech" , "Rajasthan")]
data2_scehma = ("ID" , "Student_Name" , "Department_name" , "City")
df2 = spark.createDataFrame(data2,data2_scehma)
df2.show()

# merge
df2 = df2.withColumn('Marks',lit(None))

df = df1.union(df2)
df.show()