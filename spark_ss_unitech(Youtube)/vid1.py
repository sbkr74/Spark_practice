from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode
spark = SparkSession.builder.appName("vid1").getOrCreate()

data = [("alice","Badminton,Tennis"),("bob","Tennis,Cricket"),("julie","cricket,tennis")]
col = ["name","hobbies"]
df = spark.createDataFrame(data,col)
df.show()

df.select(df.name,explode(split(df.hobbies,",")).alias("hobbies")).show()