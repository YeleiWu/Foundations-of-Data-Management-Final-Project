# Foundations-of-Data-Management-Final-Project

To help people learn different data models and query languages, we want to develop a web application called ML2X which provides 
a friendly and form-based interface for people who don’t know SQL or other query language to query online, including complex 
query(group by, aggregation, …). We help them to get the queries in Spark,SQL,Python Dataframe and Mongodb and execute them to show the results. 
Since there are some geography data in our dataset ,We also have linked our dataset with GIS system to show them on the map.

Our dataset is too big, therefore we only upload list.csv and host.csv.

Prepare Airbnb data:
Listing.csv, host.csv, review.csv

Set up environment
1.	Install MySQL(8.0), MongoDB(4.4), Spark-3.0.1-hadoop2.7(Hadoop)
2.	Import Airbnb data into MySQL, MongoDB, Spark (sql schema: db.sql)
3.	Install python packages: flask(1.1.2), sqlalchemy(1.3.19), pandas(1.1.3), findspark(1.3.0), pymongo(3.11.0)

Run project
1.	Open project in IDE(PyCharm)
2.	Run app.py
