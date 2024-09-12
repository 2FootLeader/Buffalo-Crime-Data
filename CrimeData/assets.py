## Import packages ##
import pandas as pd
import requests
import mysql.connector
import json
from datetime import datetime, timedelta, date
import time
import csv
from sqlalchemy import create_engine, types, text
import sqlalchemy
import os
import base64
from io import BytesIO
import sys

# 1. Import the config object from decouple. 


from dagster import AssetExecutionContext, MetadataValue, asset, MaterializeResult # import the `dagster` library
from decouple import config  # Used for storing credentials used in this file.
    
# Grab the credentials and app token from the .env file
db_username = config('db_username',  default='localhost')  
db_password = config('db_password', default='gfg') 
db_password = config('db_password')  
port = config('port', default=25, cast=int)
token = config('token',default='noTokenGiven')

# establish the connection to the mysql database
db = mysql.connector.connect(
user = config('db_username',  default='localhost')  
, password = config('db_password', default='gfg')
, host = 'localhost')

@asset # add the asset decorator to tell Dagster this is an asset
def CrimeDataFull() -> None: 

    current_day = date.today()
    data_load_start_date = current_day - timedelta(days=7) # Retrieve data from the past week

    dbCursor = db.cursor()

    dbCursor.execute("DROP TABLE IF EXISTS buffalo_crime_report_data") # If the table exists already, drop it

    queryResult = dbCursor.fetchall() # show the results of the query

    for x in queryResult:
        print(x)

    # Function that grabs the buffalo crime report data based on the start date
    current_day = date.today()
    data_load_start_date = current_day - timedelta(days=7) # Retrieve data from the past week
    def getdata(data_load_start_date): 
    
        url = f'https://data.buffalony.gov/resource/d6g9-xbgu.json?$$app_token='+ token + '&$offset=1000&$limit=500000'
        payload = requests.get(url).json()
        data_statuscode = requests.get(url)
        status_code = data_statuscode.status_code
        
        if status_code == 200:
            status = print('Status Code 200: Connection to https://data.buffalony.gov/resource/d6g9-xbgu.json successful!\n \nLoading data for ' + str(data_load_start_date) + ' to ' + str(current_day) +'...')
            time.sleep(2) # Wait 2 seconds

        # Load results of the JSON string to a dataframe
            data = pd.DataFrame(payload)
            data['ETL_Inserted_At'] = date.today() # Add the ETL field to STG

        # Load the results of the dataframe to a temporary .csv file.
            data.to_csv('crime_incident_data_' + str(data_load_start_date) , encoding='utf-8', index=False, header=True)
            print('\nTemporary load file created for load date: ' + str(data_load_start_date))
            data_statuscode.close() # close connection

        # If response code is not successful, display the error message
        else:
            status = print('400: Connection unsuccessful for URL: data_load_start_date' + url)
            data_statuscode.close() # close connection
            sys.exit() # Exit the script
            
        return status

    getdata(data_load_start_date) # Calls the main getData function.

    # create the mysql engine 
    engine = create_engine('mysql+pymysql://' + db_username + ':' + db_password + '@localhost:' + str(port))
    with engine.connect() as conn:
        conn.execute(text("CREATE DATABASE IF NOT EXISTS Buffalo_data")) # create the database
        engine = create_engine('mysql+pymysql://' + db_username + ':' + db_password + '@localhost:' + str(port) + '/Buffalo_data' )

    # read the data from the temporary csv into a dataframe then load it into sql
    data = pd.read_csv('crime_incident_data_' + str(data_load_start_date) ,sep=',',encoding='utf8')
    data.to_sql('buffalo_crime_report_data',con=engine,index=False,if_exists='append', chunksize = 150)

    # Remove the temporary .csv file used for loading
    os.remove('crime_incident_data_' + str(data_load_start_date))
    print('\nTemporary load file removed for load date: ' + str(data_load_start_date))

@asset(deps=[CrimeDataFull]) # This asset is dependent on the full data load
def CategoryTransform():
    def bundleCategory():
        query = "alter table buffalo_crime_report_data add incident_category nvarchar(255)" # Add a new field to categorize the incidents
        dbCursor = db.cursor()
        dbCursor.execute(query)

        # New query
        query = ("""update buffalo_crime_report_data 
        set incident_category = case when incident_type_primary in ('LARCENY/THEFT', 'ROBBERY', 'BURGLARY', 'THEFT OF SERVICES', 'Theft')  then 'Theft' 
        when incident_type_primary in ('Theft of Vehicle', 'UUV') then 'Vehicle Theft' 
        when incident_type_primary in ('AGGR ASSAULT', 'ASSAULT','AGG ASSAULT ON P/OFFICER') then 'Assault' 
        when incident_type_primary in ('SEXUAL ABUSE', 'RAPE', 'SODOMY', 'Sexual Assault','Other Sexual Offense') then 'Sexual Offense' 
        when incident_type_primary in ('HOMICIDE', 'CRIM NEGLIGENT HOMICIDE', 'MURDER') then 'Homicide' 
        else incident_type_primary end """)
        dbCursor.execute(query)
        db.commit() # Need to explicitly commit the update statement for the db to actually apply the update
    bundleCategory() # Calls Category Transform