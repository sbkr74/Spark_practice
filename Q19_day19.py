from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import col, date_format,to_date,desc,sum as cum_sum


spark = SparkSession.builder.appName('Day_19')\
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
df2 = df1.withColumn("cumulative_sum",cum_sum('revenue').over(Window.partitionBy('month').orderBy('date',desc('month')))).drop('date')
df2.show()

##############################################################
# Create window specification for cumulative sum
window_spec = Window.partitionBy('month').orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate cumulative sum
another_df = df1.withColumn('cumulative_sum', cum_sum('revenue').over(window_spec))
# Select required columns and order the result
result_df = another_df.select('revenue', 'month', 'cumulative_sum').orderBy('month','revenue')

# Show result
result_df.show()

##############################################################
# SparkSQL approach
df1.createOrReplaceTempView('temp')
qry = '''select revenue,month,sum(revenue) 
OVER(PARTITION BY Month ORDER BY Revenue) as cumulative_sum
from temp 
'''

result = spark.sql(qry).show()