from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('Day 20') \
        .master("local[*]") \
        .getOrCreate()

df = spark.read.text("Spark_practice/files/Q20.txt")
df.show()

header = df.first()[0]
schema = header.split('~|')

df_final = df.filter(df['value'] != header).rdd.map(lambda x:x[0].split('~|')).toDF(schema)
df_final.show()