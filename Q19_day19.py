from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format,to_date,desc,sum


spark = SparkSession.builder.appName('Day_17')\
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors","G1 Young Generation, G1 Concurrent GC")\
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors","G1 Old Generation, G1 Concurrent GC")\
        .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
data = [( 
'3000' , '22-may'), 
('5000' , '23-may'),
('6000' , '25-may'),
('10000' , '22-june'), 
('1250' , '03-july')]
schema = ['revenue','date']

df = spark.createDataFrame(data,schema)
df1 = df.withColumn("month",date_format(to_date(col("date"),'dd-MMM'),'MMM'))
df1.groupby('month').agg(sum(col('revenue')).alias("total")).orderBy(desc("month")).show()

df2 = df.withColumn('month', date_format(to_date(col('date'), 'dd-MMM'), 'MMM'))
df2.createOrReplaceTempView('temp')
qry = '''select revenue,month,sum(revenue) 
OVER(PARTITION BY Month ORDER BY Revenue) as cumulative_sum
from temp 
'''

result = spark.sql(qry).show()