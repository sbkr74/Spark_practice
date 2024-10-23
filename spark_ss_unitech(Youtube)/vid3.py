from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,count,when,col

spark = SparkSession.builder.appName('vid3').getOrCreate()
std_details = [(1,"steve"),(2,"David"),(3,"Aryan")]
std_col = ["student_id","student_name"]
std_marks = [(1,"pyspark",90),(1,"sql",100),(2,"sql",70),(2,"pyspark",60),(3,"sql",30),(3,"pyspark",20)]
marks_col = ["student_id","subject","marks"]

std_df = spark.createDataFrame(std_details,std_col)
marks_df = spark.createDataFrame(std_marks,marks_col)

df = std_df.join(marks_df,std_df.student_id==marks_df.student_id).drop(marks_df.student_id)
df1 = df.groupBy('student_id','student_name').agg((sum('marks')/count('*')).alias("percentage"))
df1 = df1.withColumn("Result",when(col("percentage")>75,"Distinction").when((col("percentage")<75) & (col("percentage")>30),"Pass").otherwise("Fail"))
df1.show()