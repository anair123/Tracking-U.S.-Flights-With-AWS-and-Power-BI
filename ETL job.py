#!/usr/bin/env python
# coding: utf-8

# import libraries
import pandas as pd
import boto3
import io 

''' Notes
# arr_flights = Number of Flights
# arr_del15 = Number of delayed flights (15 min after schedule)
# arr_diverted = number of diverte lights
# num_cancelled = number of cancelled flights
# arr_delay = total delay in minutes
# carrier_delay = carrier delay in minutes
# weather_delay = weather delay in minutes
# nas_delay = national air system delay in minutes
# security_delay = security delay in minutes
# late_aircraft_delay = late aircraft delay in minutes
'''


# create client object
s3 = boto3.client('s3',
                region_name='us-east-2')


# list buckets
response = s3.list_buckets()


# read raw data in a polars data frame
obj = s3.get_object(Bucket='flights-data-raw', Key='Airline_Delay_Cause.csv')
flights = pd.read_csv(io.BytesIO(obj['Body'].read()))

# drop unwanted columns 
flights = flights.drop(['carrier_ct','weather_ct','nas_ct','security_ct','late_aircraft_ct'], axis=1)


# extract city, state, and airport_name from the airport_name field
flights['city_state']=flights['airport_name'].apply(lambda x: x.split(': ')[0])
flights['airport_name']=flights['airport_name'].apply(lambda x: x.split(': ')[1])
flights['city']=flights['city_state'].apply(lambda x: x.split(', ')[0])
flights['state']=flights['city_state'].apply(lambda x: x.split(', ')[1])
flights = flights.drop('city_state', axis=1)


# ------------------------------------------------- create  carrier table ---------------------------------------

carrier = flights[['carrier', 'carrier_name']].drop_duplicates(['carrier', 'carrier_name'], keep='first').sort_values('carrier')
# create an id column for carrier table
id_ = [i for i in range(1, len(carrier)+1)]
carrier['id'] = id_
carrier = carrier[['id', 'carrier', 'carrier_name']]

# replace the carrier and carrier_name columns with a foreign key
flights = flights.merge(carrier, on=['carrier', 'carrier_name']).drop(['carrier', 'carrier_name'], axis=1)
flights = flights.rename(columns={'id': 'carrier_id'})
flights.head(2)
flights.shape


# ---------------------------------------- Create Airport Table ------------------------------------------- 
airport = flights[['airport', 'airport_name', 'city', 'state']].drop_duplicates('airport', keep='first').sort_values('airport')
id_ = [i for i in range(1, len(airport)+1)]
airport['id'] = id_
airport = airport[['id', 'airport', 'airport_name', 'city', 'state']]


# create a foreign key in flights connecting to airport table and remove columns from airport table
flights = flights.merge(airport, on=['airport', 'airport_name', 'city', 'state']).drop(['airport', 'airport_name', 'city', 'state'], axis=1)
flights = flights.rename(columns={'id': 'airport_id'})


# ----------------------------------------------- create date table -----------------------------------

# Create date column using year and month columns
flights['date'] = pd.to_datetime(flights[['year', 'month']].assign(day=1))
flights = flights.sort_values('date')

# Create date ID column
flights['date_id'] = pd.factorize(flights['date'])[0] + 1

# Create a new table with unique year and month data
date = flights[['date_id', 'date', 'year', 'month']].drop_duplicates().reset_index(drop=True)

# delete the year and month rows from the flights table
flights = flights.drop(['date','year', 'month'], axis=1)


# reorder columns
flights['id'] = flights.reset_index().index+1
flights = flights[['id', 'date_id','carrier_id','airport_id','arr_flights','arr_del15','arr_cancelled','arr_diverted','arr_delay','carrier_delay','weather_delay','nas_delay','security_delay','late_aircraft_delay']]

# fill missing values
for col in ['arr_flights','arr_del15','arr_cancelled','arr_diverted','arr_delay','carrier_delay','weather_delay','nas_delay','security_delay','late_aircraft_delay']:
    flights[col] = flights[col].fillna(0)
    
# convert columns to integers
for col in flights.columns:
    flights[col] = flights[col].astype(int)


# ---------------------------------- Export Tables to S3 bucket ----------------------------------------

# upload files to bucket
csv_buffer = io.StringIO()
flights_pd = pd.DataFrame(flights.to_numpy(), columns=flights.columns)
flights_pd.to_csv(csv_buffer, index=False)
s3.put_object(Bucket='flights-data-processed', 
              Body=csv_buffer.getvalue(), 
              Key='flights.csv')


# uploade files to bucket
csv_buffer = io.StringIO()
carrier_pd = pd.DataFrame(carrier.to_numpy(), columns=carrier.columns)
carrier_pd.to_csv(csv_buffer, index=False)
s3.put_object(Bucket='flights-data-processed', 
              Body=csv_buffer.getvalue(), 
              Key='carriers.csv')


# uploade files to bucket
csv_buffer = io.StringIO()
airport_pd = pd.DataFrame(airport.to_numpy(), columns=airport.columns)
airport_pd.to_csv(csv_buffer, index=False)
s3.put_object(Bucket='flights-data-processed', 
              Body=csv_buffer.getvalue(), 
              Key='airports.csv')
              
# uploade files to bucket
csv_buffer = io.StringIO()
date_pd = pd.DataFrame(date.to_numpy(), columns=date.columns)
date_pd.to_csv(csv_buffer, index=False)
s3.put_object(Bucket='flights-data-processed', 
              Body=csv_buffer.getvalue(), 
              Key='date.csv')

