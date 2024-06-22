## Question: The task is to merge two data frames using PySpark.

**Input:**  
**data1** = [(1,"Sagar" , "CSE" , "UP" , 80),(2,"Shivani" , "IT" , "MP", 86),(3,"Muni", "Mech" , "AP", 70)]  
**data1_schema** = ("ID" , "Student_Name" ,"Department_Name" , "City" , "Marks")
```
+---+------------+---------------+----+-----+
| ID|Student_Name|Department_Name|City|Marks|
+---+------------+---------------+----+-----+
| 1| Sagar       | CSE           | UP | 80|
| 2| Shivani     | IT            | MP | 86|
| 3| Muni        | Mech          | AP | 70|
+---+------------+---------------+----+-----+
```
**INPUT:**  
**data2** = [(4, "Raj" , "CSE" , "HP") ,(5 , "Kunal" , "Mech" , "Rajasthan")]  
**data2_scehma** = ("ID" , "Student_Name" , "Department_name" , "City")

```
+---+------------+---------------+----------+
| ID|Student_Name|Department_name| City     |
+---+------------+---------------+----------+
| 4| Raj         | CSE           | HP       |
| 5| Kunal       | Mech          |Rajasthan |
+---+------------+---------------+----------+
```