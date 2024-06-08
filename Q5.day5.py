from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('day5').getOrCreate()

student = [(1,'Steve'),(2,'David'),(3,'Aryan')] 
student_schema = "student_id int , student_name string"

student_df = spark.createDataFrame(data = student,schema = student_schema)
# student_df.show()

marks = [(1,'pyspark',90), (1,'sql',100), (2,'sql',70), (2,'pyspark',60), (3,'sql',30), (3,'pyspark',20) ] 
marks_schema = "student_id int , subject_name string , marks int"

marks_df = spark.createDataFrame(data = marks, schema = marks_schema)
# marks_df.show()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# SQL approach
student_df.createOrReplaceTempView('students')
marks_df.createOrReplaceTempView('marks')


#  1. % Marks greater than or equal to 70 then 'Distinction'
#  2. % Marks range between 60-69 then 'First Class'
#  3. % Marks range between 50-59 then 'Second Class'
#  4. % Marks range between 40-49 then 'Third Class'
#  5. % Marks Less than or equal to 39 then 'Fail'
query = ''' select s.student_id,student_name,marks,
(case when marks>= 70 then 'Distinction'
    when marks<=69 and marks>=60 then 'First class'
    when marks<=59 and marks>=50 then 'Second class'
    when marks<=49 and marks>=40 then 'Third class'
 else 'Fail' end) as grade
from students s inner join marks m 
on s.student_id = m.student_id 
'''
sql_df = spark.sql(query)
sql_df.show()
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Spark DataFrame approach
joined_df = student_df.join(marks_df,student_df.student_id==marks_df.student_id,"inner")
grade_df = joined_df.withColumn('grade',F.when(F.col('marks')>=70,"Distinction")
                                .when((F.col('marks')<=69)&(F.col('marks')>=60),"First class")
                                .when((F.col('marks')<=59)&(F.col('marks')>=50),"Second class")
                                .when((F.col('marks')<=49)&(F.col('marks')>=40),"Third class")
                                .otherwise("Fail"))
grade_df.show()