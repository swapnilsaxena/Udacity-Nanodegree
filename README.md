# Data Modelling with Postgres

## Introduction

A startup called **Sparkify** want to analyze the data they have been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, their data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This does not provide an easy way to query the data.

## Objective

The aim is to create a Postgres Database Schema and ETL pipeline to optimize queries to help Sparkify's analytics team.

## Files

> - etl.ipynb: This notebook is used to create the ETL process for each tables
> - test.ipynb: This notebook is used to test sql_queries.py and elt.ipynb
> - create_tables.py: This is used to create database and tables
> - elt.py: This is used to define the ETL process
> - sql_queries.py: This is used to define the SQL queries
> - Running_py_files.ipynb : This is used to run the 3 python files i.e. create_tables.py , sql_queries.py and etl.py


## Database & ETL pipeline

I have created an star schema using the song and log datasets which includes
> - One fact table: songplays
> - Four dimension tables: users, songs, artists and time.







