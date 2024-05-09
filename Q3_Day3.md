# Spark Day 3

Let's try solving this amazing "Monthly Percentage Difference" interview 
## Challenge from StrataScratch in SQL,Pandas and PySpark which was asked in the Amazon coding interview.

## Problem: Given a table of purchases by date, calculate the month-over-month percentage change in revenue. The output should include the year-month date (YYYY-MM) and percentage change, rounded to the 2nd decimal point, and sorted from the beginning to the end of the year.
### The percentage change column will be populated from the 2nd month forward and calculated as ((this month's revenue - last month's revenue) / last month's revenue)*100.

Table Schema : 
==========
id : int
created_at : datetime
value : int
purchase_id : int