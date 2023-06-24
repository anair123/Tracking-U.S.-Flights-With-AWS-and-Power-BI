# %%
import pandas as pd
import polars as pl
import aws_keys
import boto3

# %%
# load access keys
access_key, secret_key = aws_keys.aws_keys()

# %%
# create client object
s3 = boto3.client('s3',
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                region_name='us-east-2')

# %%
# list buckets
response = s3.list_buckets()
response['Buckets']

# %%
import io 

# read raw data in a polars data frame
obj = s3.get_object(Bucket='flights-data-raw', Key='Airline_Delay_Cause.csv')
flights = pl.read_csv(io.BytesIO(obj['Body'].read()))

# %%
# preview of raw data
flights.head(2)

# %%
# drop unwanted columns 
flights = flights.drop(['carrier_ct','weather_ct','nas_ct','security_ct','late_aircraft_ct'])

# %%

# create a city_state column
flights = flights.with_columns([
    pl.col("airport_name").apply(lambda x: x.split(": ")[0]).alias('city_state')
])


# create an airport_name column
flights = flights.with_columns([
    pl.col("airport_name").apply(lambda x: x.split(": ")[1]),
])


# create a city column
flights =flights.with_columns([
    pl.col("city_state").apply(lambda x: x.split(", ")[0]).alias('city'),

])


# create a state column
flights = flights.with_columns([
    pl.col("city_state").apply(lambda x: x.split(", ")[1]).alias('state'),
])

# drop the city state column
flights = flights.drop('city_state')


# %%
# preview of data
flights.head(2)

# %% [markdown]
# ### Create Carrier Table

# %%
# create a carrier table
carrier = flights.select(['carrier', 'carrier_name']).unique(subset=['carrier', 'carrier_name']).sort('carrier')

# create an id column 
id_ = [i for i in range(1, len(carrier)+1)]
carrier = carrier.with_columns(pl.Series(name="id", values=id_))[['id', 'carrier', 'carrier_name']] 
carrier.head(2)

# %%

# replace the carrier and carrier_name columns with a foreign key
flights = flights.join(carrier, on=['carrier', 'carrier_name']).drop(['carrier', 'carrier_name'])
flights = flights.rename({'id': 'carrier_id'})
flights.head(2)

# %% [markdown]
# ### Create Airport Table

# %%
airport = flights.select(['airport', 'airport_name', 'city', 'state']).unique(subset=['airport']).sort('airport')

id_ = [i for i in range(1, len(airport)+1)]
airport = airport.with_columns(pl.Series(name="id", values=id_))[['id', 'airport', 'airport_name', 'city', 'state']] 
airport.head(2)

# %%
airport.head(3)

# %%
flights = flights.join(airport, on=['airport', 'airport_name', 'city', 'state']).drop(['airport', 'airport_name', 'city', 'state'])
flights = flights.rename({'id': 'airport_id'})

# %% [markdown]
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
# 

# %%
flights.head(5)

# %%
# reorder columns
flights = flights[['year','month','carrier_id','airport_id','arr_flights','arr_del15','arr_cancelled','arr_diverted','arr_delay','carrier_delay','weather_delay','nas_delay','security_delay','late_aircraft_delay']]

# %%
flights.head()

# %%
carrier.head()

# %%
airport.head()

# %% [markdown]
# ### Export Tables to S3

# %%
# uploade files to bucket
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