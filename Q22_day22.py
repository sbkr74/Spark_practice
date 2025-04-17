import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class AdultChildPair:
 def create_data(self):
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
    columns = ['person', 'type', 'age']
    return spark.createDataFrame(data, columns)

 def get_pair(self, input_df):
    adult_df = input_df.filter(col('type').__eq__('ADULT')) \
    .withColumn('rnk', row_number().over(Window.partitionBy(col("type")).orderBy(desc('age'))))

    child_df = input_df.filter(col('type').__eq__('CHILD'))\
    .withColumn('rnk', row_number().over(Window.partitionBy(col("type")).orderBy('age')))

    result_df = adult_df.alias('A').join(child_df.alias('C'), on='rnk', how='full')\
    .select(col('A.person'),
    col('C.person'))
    return result_df

ob = AdultChildPair()
input_df = ob.create_data()

result_df = ob.get_pair(input_df)
result_df.show()