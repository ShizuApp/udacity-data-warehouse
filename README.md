# Project: Data Warehouse

## Intro

This project consists in an ELT process, it extracts data from AWS S3(Cloud Storage) collected by a music
streaming app, Sparkify, copies and stages the data into tables in AWS Redshift(Data Warehouse), and models
the data into a star schema for running optimaze queries to meet the goals of the analytics team of the app.

---

## Files

* **sql_queries.py**: All required SQL queries to create, drop and fill the tables. Others scripts will import this file in order to run the queries.

* **dwh.cfg**: Redshift, IAM and S3 configuration file. 

### Data Warehouse Setup

* Set up a IAM user in AWS with AdministratorAccess
* Use access key and secret key to create clients for EC2, S3, IAM, and Redshift
* Create and add an IAM role into Redshift with read access to S3
* Create a RedShift cluster and fill the DWH_ENDPOINT(Host) and DWH_ROLE_ARN in the config file.

### Running the scripts:

* Install required packages listed in requirements.txt or run:

    `pip install -r requitements.txt`

* run **create_tables.py** to creates/resets tables

* run **etl.py** to load and insert the data

---

## Schema Design

Project executes an ETL pipeline (Extract/Transform/Load) to fetch, process and insert the data from S3 to the Redshift warehouse in a star schema optimized for queries to analyze the song play data.

### Fact Table

- **songplays** records in log data associated with song plays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables

- **users** (user_id, first_name, last_name, gender, level)
- **songs** (song_id, title, artist_id, year, duration)
- **artists** (artist_id, name, location, latitude, longitude)
- **time** timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)