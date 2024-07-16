from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *


spark = SparkSession.builder.appName('Day_17')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

# orders = spark.read.option("delimiter", "|").csv("data\orders.csv",header=True,inferSchema = True)
orders = spark.read.option("delimiter", "|").csv("data\orders.csv",header=True)

# order_items = spark.read.option("delimiter","|").csv("data\order_items.csv",header=True,inferSchema=True)
order_items = spark.read.option("delimiter","|").csv("data\order_items.csv",header=True)

# List the original column names
orders_original_columns = orders.columns

# Rename columns to remove leading/trailing spaces
trimmed_columns = [col(c).alias(c.strip()) for c in orders_original_columns]
orders = orders.select(*trimmed_columns)

orders_df = orders.withColumn("order_id",col("order_id").cast("int"))\
                .withColumn("customer_id",col("customer_id").cast("int"))\
                .withColumn("order_date",col("order_date").cast("date"))

# List the original column names
orderItem_original_columns = order_items.columns

# Rename columns to remove leading/trailing spaces
trimmed_columns = [col(c).alias(c.strip()) for c in orderItem_original_columns]
order_items = order_items.select(*trimmed_columns)

order_items_df = order_items.withColumn("order_id",col("order_id").cast("int"))\
                .withColumn("product_id",col("product_id").cast("int"))\
                .withColumn("quantity",col("quantity").cast("int"))

df = orders.join(order_items,orders["order_id"] == order_items["order_id"],"inner")\
                .groupBy("product_id","customer_id")\
                .agg(sum("quantity").alias("Total_quantity"))\
                .orderBy(desc("Total_quantity"))
df.show()