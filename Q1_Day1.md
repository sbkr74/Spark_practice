## Problem: Find stores that were opened in the second half of 2021 with more than 20% of their reviews being negative. A review is considered negative when the score given by a customer is below 5. Output the names of the stores together with the ratio of negative reviews to positive ones.

Schema : 
======
Table 1: instacart_reviews

id: int
customer_id : int
store_id : int
score: int
------------------------------------------------------------------------------------
Table 2 : instacart_stores

id: int
name: varchar
zipcode: int
opening_date : DateTime