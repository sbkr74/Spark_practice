## Question 5: Calculate the % Marks for each student. Each subject is worth 100 marks. Create a result column by following the below condition 

 1. % Marks greater than or equal to 70 then 'Distinction'
 2. % Marks range between 60-69 then 'First Class'
 3. % Marks range between 50-59 then 'Second Class'
 4. % Marks range between 40-49 then 'Third Class'
 5. % Marks Less than or equal to 39 then 'Fail'

Input:

**student** = [(1,'Steve'),(2,'David'),(3,'Aryan')]  
**student_schema** = "student_id int , student_name string"
```
+----------+------------+
|student_id|student_name|
+----------+------------+
|         1|       Steve|
|         2|       David|
|         3|       Aryan|
+----------+------------+
```

**marks** = [(1,'pyspark',90),(1,'sql',100),(2,'sql',70),(2,'pyspark',60),(3,'sql',30),(3,'pyspark',20)]  
**marks_schema** = "student_id int , subject_name string , marks int"
```
+----------+------------+-----+
|student_id|subject_name|marks|
+----------+------------+-----+
|         1|     pyspark|   90|
|         1|         sql|  100|
|         2|         sql|   70|
|         2|     pyspark|   60|
|         3|         sql|   30|
|         3|     pyspark|   20|
+----------+------------+-----+
```