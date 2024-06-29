## Question: Find the origin and the destination of each customer.

**Input:**  
**flights_data** = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),  
 (1,'Flight2' , 'Hyderabad' , 'Kochi'),  
 (1,'Flight3' , 'Kochi' , 'Mangalore'),  
 (2,'Flight1' , 'Mumbai' , 'Ayodhya'),  
 (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')]

**_schema** = "cust_id int, flight_id string , origin string , destination string"

```
+-------+---------+---------+-----------+
|cust_id|flight_id| origin  |destination|
+-------+---------+---------+-----------+
| 1     | Flight1 | Delhi   | Hyderabad |
| 1     | Flight2 |Hyderabad| Kochi     |
| 1     | Flight3 | Kochi   | Mangalore |
| 2     | Flight1 | Mumbai  | Ayodhya   |
| 2     | Flight2 | Ayodhya | Gorakhpur |
+-------+---------+---------+-----------+
```