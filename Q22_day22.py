from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number,col,desc,max,min
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Day 22').getOrCreate()

data = [
 ('A1', 'ADULT', 54),
 ('A2', 'ADULT', 53),
 ('A3', 'ADULT', 52),
 ('A4', 'ADULT', 58),
 ('A5', 'ADULT', 54),
 ('C1', 'CHILD', 20),
 ('C2', 'CHILD', 19),
 ('C3', 'CHILD', 22),
 ('C4', 'CHILD', 15)
 ]

df = spark.createDataFrame(data).toDF("person","type","age")

# Adult Dataframe
df_adult = df.filter(col("type") == "ADULT").withColumn("rnk",row_number().over(Window.partitionBy(col("type")).orderBy(desc("age"))))

# Child Dataframe
df_child = df.filter(col("type") == "CHILD").withColumn("rnk",row_number().over(Window.partitionBy(col("type")).orderBy(col("age"))))

# joined using full so nothing will miss out from both side
result_df = df_adult.alias("A").join(df_child.alias("C"),on=(df_adult.rnk==df_child.rnk),how="full").select(col('A.person').alias("ADULT"),col('C.person').alias("CHILD"))
result_df.show()