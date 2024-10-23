from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('vid4').getOrCreate()

data = [
(1,"A",1000,"IT"),
(2,"B",1500,"IT"),
(3,"C",2500,"IT"),
(4,"D",3000,"HR"),
(5,"E",2000,"HR"),
(6,"F",1000,"HR"),
(7,"G",4000,"Sales"),
(8,"H",4000,"Sales"),
(9,"I",1000,"Sales"),
(10,"J",2000,"Sales")]

column = ["EmpId","EmpName","Salary","DeptName"]

df = spark.createDataFrame(data,column)
df.show()
########################################
# used groupBy as I am used to using groupBy in this type condition
df1 = df.groupBy(df.DeptName).agg((F.max(df.Salary)).alias("Salary"))
# but result want to be as it that's why joined to original table, although it is resource consuming task
df2 = df.join(df1,(df1.Salary == df.Salary) & (df1.DeptName == df.DeptName),"inner").drop(df1.Salary,df1.DeptName)
df2.show()

# optimized my previous mistake for resource management
df_rank = df.withColumn("rank",F.rank().over(Window.partitionBy(df.DeptName).orderBy(F.desc(df.Salary))))
df_max_sal = df_rank.filter(df_rank.rank == 1).drop(df_rank.rank)
df_max_sal.orderBy(df_max_sal.EmpId).show()