# CrimeData

This is a project for a simple data pipeline using python, a MySQL database, and Dagster. The data source is from https://data.buffalony.gov/Public-Safety/Crime-Incidents/d6g9-xbgu/about_data.

## About the Data
This dataset details reported crime incidents across the city of Buffalo, New York from 2009 to the present, updated on a daily cadence.

## About the Pipeline
The pipeline uses an API call to the dataset, writes the data to a temporary .csv file, then reads the data into a MySQL database from the .csv file. Post insert to the database, there is an additional transformation step which groups each crime incident type into more generic categories and assigns the generic category to a new column. The records typically do not get updated over time - so a full drop and re-create process was chosen for the destination table. 

The pipeline is setup to run on a weekly schedule (can be easily configured to run daily) using Dagster. The refresh job has two steps:
1. Fill step
2. Transformation Step (Only if step 1 has ran without error.)

## Where's the code?

You can view the source code by navigating to CrimeData > assets.py. To run the code, an .env file will need to be created with the following info included:

- db_username = `mysqlusername`
- db_password = `mysqlpassword`
- database = `mysql://mysqlusername:mysqlpassword@localhost/Buffalo_data`
- port = `mysqlport --This is usually 3306 by default` 
- token = `apitoken --This is retrieved from the Buffalo Open Data website. Create a free account and generate your own personal token.`