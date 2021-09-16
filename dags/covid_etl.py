# ETL Script for daily scraping of covid data
# API from coronatracker.com - we'll collect data from 3 countries (UK, USA, Malaysia)
# Full URL: https://documenter.getpostman.com/view/11073859/Szmcbeho
# Data is daily covid cases per country (cumulative)

# Package Imports
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, timedelta
import sqlite3

# Define helper function to validate data for TRANSOFRM stage:
def validity_check(df: pd.DataFrame) -> bool:
    """ Validity checks for your DataFrame. """
    
    # Checks if df is empty
    if df.empty:
        print("No Data downloaded.")
        return False
    
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null Values Found")
    
    # Check for primary key (with datetime timestamp 'last_updated' col)
    # Will also potentially check for overlaps in timestamps
    # Be sure to convert the column to datetime first before calling this check
    if pd.Series(df['last_updated']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check Violated")
    
    return True


# Define core ETL function
def run_covid_etl():

    # Create automated weekly date ranges for API call

    now = datetime.now() # provides current date

    # Formatting suitable for API string
    today = now.strftime("%Y-%m-%d")
    
    # If you want to modify date range (instead of daily, place in startDate)
    start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    
    # Constants
    DATABASE_LOCATION = "sqlite:///covid_daily_data.sqlite"

    URL_UK = f"http://api.coronatracker.com/v5/analytics/trend/country?countryCode=GB&startDate={today}&endDate={today}"
    URL_US = f"http://api.coronatracker.com/v5/analytics/trend/country?countryCode=US&startDate={today}&endDate={today}"
    URL_MY = f"http://api.coronatracker.com/v5/analytics/trend/country?countryCode=MY&startDate={today}&endDate={today}"

 

    payload={}
    headers = {}

    response_uk = requests.request("GET", URL_UK, headers=headers, data=payload)
    response_us = requests.request("GET", URL_US, headers=headers, data=payload)
    response_my = requests.request("GET", URL_MY, headers=headers, data=payload)

    # Save raw json file
    data_uk = json.loads(response_uk.text)
    data_us = json.loads(response_us.text)
    data_my = json.loads(response_my.text)

    with open('data_uk.json', 'w') as f:
        json.dump(data_uk, f)

    with open('data_us.json', 'w') as f:
        json.dump(data_us, f)

    with open('data_my.json', 'w') as f:
        json.dump(data_my, f)

    # Convert to pandas dataframe
    # You want 3 cleaned tables that can be placed in both the s3 bucket as well as a database/data warehouse for loading
    data_df_uk = pd.DataFrame(data_uk)
    data_df_us = pd.DataFrame(data_us)
    data_df_my = pd.DataFrame(data_my)

    # Define helper function to clean dataframe
    def clean_df(df):
        """ Cleans your dataframe for staging & loading.
            1. Converts 'last_updated' column to datetime (unique) so it can be used as your PRIMARY KEY.
            2. Drop 'country_code' column as 'country' will suffice.
            3. Create 2 new columns ['recovered_ratio', 'death_ratio'] to show ratios. """

        df1 = df.copy()
        # Drop 'country_code'
        if 'country_code' in df1.columns:
            df1.drop(columns='country_code', inplace=True)
        else:
            raise Exception('No country_code column found')

        # create new cols and rearrange order
        if 'total_deaths' and 'total_recovered' in df1.columns:
            df1['death_rate'] = df1['total_deaths'] / df1['total_confirmed']
            df1['recovery_rate'] = df1['total_recovered'] / df1['total_confirmed']

            df1 = df1[['last_updated', 'country', 'total_confirmed',
                    'total_deaths', 'total_recovered', 'death_rate', 'recovery_rate']]
        else:
            raise Exception('No applicable columns for death_rate and recovery_rate')
        
        # Check and clean your date (PK) column 'last_updated'
        if 'last_updated' in df1.columns:
            df1['last_updated'] = pd.to_datetime(df1['last_updated']).dt.normalize()
        else:
            raise Exception('Rename your datetime column to last_updated')

        return df1
    
    # Clean DataFrames
    data_df_uk_cln = clean_df(data_df_uk)
    data_df_us_cln = clean_df(data_df_us)
    data_df_my_cln = clean_df(data_df_my)

    # Validate
    if validity_check(data_df_uk_cln):
        print("UK Data Valid, able to stage and load.")

    if validity_check(data_df_us_cln):
        print("US Data Valid, able to stage and load.")

    if validity_check(data_df_my_cln):
        print("MY Data Valid, able to stage and load.")

    # Load
    engine= sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('covid_daily_data.sqlite')
    cursor = conn.cursor()

    sql_query = """
                CREATE TABLE IF NOT EXISTS covid_daily_data_uk(
                    last_updated TIMESTAMP PRIMARY KEY,
                    country VARCHAR(200),
                    total_confirmed INTEGER,
                    total_deaths INTEGER,
                    total_recovered INTEGER,
                    death_rate INTEGER,
                    recovery_rate INTEGER
                );

                CREATE TABLE IF NOT EXISTS covid_daily_data_us(
                    last_updated TIMESTAMP PRIMARY KEY,
                    country VARCHAR(200),
                    total_confirmed INTEGER,
                    total_deaths INTEGER,
                    total_recovered INTEGER,
                    death_rate INTEGER,
                    recovery_rate INTEGER
                );
                
                CREATE TABLE IF NOT EXISTS covid_daily_data_my(
                    last_updated TIMESTAMP PRIMARY KEY,
                    country VARCHAR(200),
                    total_confirmed INTEGER,
                    total_deaths INTEGER,
                    total_recovered INTEGER,
                    death_rate INTEGER,
                    recovery_rate INTEGER
                )

                """

    cursor.executescript(sql_query)
    print("Created database")

    try:
        data_df_uk_cln.to_sql('covid_daily_data_uk', engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database.')

    try:
        data_df_us_cln.to_sql('covid_daily_data_us', engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database.')

    try:
        data_df_my_cln.to_sql('covid_daily_data_my', engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database.')

    conn.close()
    print('Closed database successfully.')