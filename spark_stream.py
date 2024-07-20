from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode,count
spark = SparkSession.builder.master("local[*]").appName('test').getOrCreate()
df = spark.read.text('./data/input/example.txt')
df_words = df.withColumn("words",split("value"," ")).drop("value")
df_explode = df_words.withColumn("words",explode("words"))
df_grp = df_explode.groupBy('words').agg(count("words").alias("cnt"))
df_grp.show()

# stream
df_strem = spark.readStream.format("socket").option("host","localhost").option("port","9999") .load()
df_words = df_strem.withColumn("words",split("value"," ")).drop("value")
df_explode = df_words.withColumn("words",explode("words"))
df_grp = df_explode.groupBy('words').agg(count("words").alias("cnt"))
df_grp.writeStream.format("console").outputMode("complete").start().awaitTermination()