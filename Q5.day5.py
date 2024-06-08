from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('day5').getOrCreate()

student = [(1,'Steve'),(2,'David'),(3,'Aryan')] 
student_schema = "student_id int , student_name string"

student_df = spark.createDataFrame(data = student,schema = student_schema)
student_df.show()

marks = [(1,'pyspark',90), (1,'sql',100), (2,'sql',70), (2,'pyspark',60), (3,'sql',30), (3,'pyspark',20) ] 
marks_schema = "student_id int , subject_name string , marks int"

marks_df = spark.createDataFrame(data = marks, schema = marks_schema)
marks_df.show()

