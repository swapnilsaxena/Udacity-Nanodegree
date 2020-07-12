# Data Modelling with Postgres

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, my task was to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are 
listening to.

## Objective

Application of Data warehouse and AWS to build an ETL Pipeline for a database hosted on Redshift which will need to load data from S3 to staging tables on Redshift and execute SQL Statements that create fact and dimension tables from these staging tables to create analytics tables

## Project Files

Song Data Path --> s3://udacity-dend/song_data 
Log Data Path --> s3://udacity-dend/log_data 
Log Data JSON Path --> s3://udacity-dend/log_json_path.json

## Project Template

> - Project 4 Data Warehousing.ipynb: This notebook is used to create AWS user, connect to redshift cluster
> - Running_code.ipynb: This notebook is used to test sql_queries.py,create_tables.py and elt.py 
> - create_tables.py: Is where I have created the staging, fact and dimension tables for the star schema on Redshift. 
> - elt.py:  Is where I have loaded data from S3 into our staging tables on Redshift and then process that data into our analytics (fact & dimension) tables on Redshift.
> - sql_queries.py:  Is where I have defined our SQL statements, which will be imported into the two other files above.


## Schema for Song Play Analysis

A Star Schema would be required for optimized queries on song play queries

## Fact Table

songplays - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables

users - users in the app user_id, first_name, last_name, gender, level

songs - songs in music database song_id, title, artist_id, year, duration

artists - artists in music database artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

## Create Table Schema

1. Write a SQL CREATE statement for each of these tables in sql_queries.py
> - I defined the drop and create table statement for each of our staging, fact and dimension tables

2. Complete the logic in create_tables.py to connect to the database and create these tables
> - In order to connect to Redshift Cluster Database a set of configuration variables were required. For this task a python library named configparser was required.
> - All the configurations were defined on a file named dwh.cfg . Detail of the same is as follows:

    [CLUSTER]
    HOST=<THE REDSHIFT HOST ENDPOINT>
    DB_NAME=<DATABASE NAME>
    DB_USER=<DATABASE USERNAME>
    DB_PASSWORD=<DATABASE PASSWORD>
    DB_PORT=<DATABASE_PORT>

    [IAM_ROLE]
    ARN=<AWS IDENTITY AND ACCESS MANAGMENT Role to allow Redshift to connect to the S3 bucket>

    [S3]
    LOG_DATA='s3://udacity-dend/log_data'
    LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
    SONG_DATA='s3://udacity-dend/song_data'


3. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
> - I defined the drop and create table statement for each of our staging, fact and dimension tables

4. Launch a redshift cluster and create an IAM role that has read access to S3.
> - Redshift cluster was launched using the value defined in step 2. Also IAM role was created which has read access to S3.

5. Add redshift database and IAM role info to dwh.cfg.
> - The defined values were added in the dwh.cfg to make the connection.

6. Test by running create_tables.py and checking the table schemas in your redshift database.
> - I ran the create_tables.py using Running_code.ipnnb notebook. This notebook has the required logs generated.

## Build ETL Pipeline

1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
>- I ran the etl.py using Running_code.ipnnb notebook. This notebook has the required logs generated.

2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
>- I ran the etl.py using Running_code.ipnnb notebook. This notebook has the required logs generated.

3. Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
>- I ran few queries in "Query Editor" and also in Project 4 Data Warehousing.ipynb to cross check everything is working fine.

4. Delete your redshift cluster when finished.
> - I deleted the Redshift cluster after verification.






