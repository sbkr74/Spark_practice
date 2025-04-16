Challenge: Given a dataset with columns PERSON, TYPE, and AGE,
create an output where the oldest adult is paired with the youngest child, producing pairs of ADULT and CHILD while ensuring appropriate data matching.

💡 Check out the input and output in the table below!

Input:--->

| PERSON | TYPE | AGE |
| ------ | ------ | --- |
| A1 | ADULT | 54 |
| A2 | ADULT | 53 |
| A3 | ADULT | 52 |
| A4 | ADULT | 58 |
| A5 | ADULT | 54 |
| C1 | CHILD | 20 |
| C2 | CHILD | 19 |
| C3 | CHILD | 22 |
| C4 | CHILD | 15 |


Expected Output:--->

| ADULT | CHILD |
| ----- | ----- |
| A4 | C4 |
| A5 | C2 |
| A1 | C1 |
| A2 | C3 |
| A3 | NULL |

```py
import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class AdultChildPair:
 def createData(self):
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

 def getPair_pyspark(self, inputDf):
 adultDf = inputDf.filter(col('type').__eq__('ADULT')) \
 .withColumn('rnk', row_number().over(Window.orderBy(desc('age')).orderBy(desc('person'))))

 childDf = inputDf.filter(col('type').__eq__('CHILD'))\
 .withColumn('rnk', row_number().over(Window.orderBy('age').orderBy('person')))

 resultDf = adultDf.alias('A').join(childDf.alias('C'), on='rnk', how='full')\
 .select(col('A.person'),
 col('C.person'))
 return resultDf

ob = AdultChildPair()
inputDf = ob.createData()

resultDf = ob.getPair(inputDf)
resultDf.show()

resultDf = ob.getPair_pyspark(inputDf)
resultDf.show()
```