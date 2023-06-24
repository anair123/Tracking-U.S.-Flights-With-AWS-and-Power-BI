#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import aws_keys
import boto3


# In[4]:


# load access keys
access_key, secret_key = aws_keys.aws_keys()


# In[5]:


# create client object
s3 = boto3.client('s3',
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                region_name='us-east-2')


# In[20]:


# list buckets
response = s3.list_buckets()
response['Buckets']


# In[29]:


import io 

# read raw data in a polars data frame
obj = s3.get_object(Bucket='flights-data-raw', Key='Airline_Delay_Cause.csv')
flights = pd.read_csv(io.BytesIO(obj['Body'].read()))
flights.shape


# In[30]:


# preview of raw data
flights.head(2)


# In[31]:


# drop unwanted columns 
flights = flights.drop(['carrier_ct','weather_ct','nas_ct','security_ct','late_aircraft_ct'], axis=1)


# In[32]:



flights['city_state']=flights['airport_name'].apply(lambda x: x.split(': ')[0])
flights['airport_name']=flights['airport_name'].apply(lambda x: x.split(': ')[1])
flights['city']=flights['city_state'].apply(lambda x: x.split(', ')[0])
flights['state']=flights['city_state'].apply(lambda x: x.split(', ')[1])
flights = flights.drop('city_state', axis=1)



# In[33]:


# preview of data
flights.head(2)


# ### Create Carrier Table

# In[34]:


# create a carrier table

carrier = flights[['carrier', 'carrier_name']].drop_duplicates(['carrier', 'carrier_name'], keep='first').sort_values('carrier')
# create an id column 
id_ = [i for i in range(1, len(carrier)+1)]
carrier['id'] = id_
carrier = carrier[['id', 'carrier', 'carrier_name']]
carrier.head(2)


# In[35]:



# replace the carrier and carrier_name columns with a foreign key
flights = flights.merge(carrier, on=['carrier', 'carrier_name']).drop(['carrier', 'carrier_name'], axis=1)
flights = flights.rename(columns={'id': 'carrier_id'})
flights.head(2)
flights.shape


# ### Create Airport Table

# In[37]:


airport = flights[['airport', 'airport_name', 'city', 'state']].drop_duplicates('airport', keep='first').sort_values('airport')

id_ = [i for i in range(1, len(airport)+1)]
airport['id'] = id_
airport = airport[['id', 'airport', 'airport_name', 'city', 'state']]
airport.head(2)


# In[39]:
###  Create Data table

date = flights['date'].drop_duplicates('date', keep='first').sort_values()



# In[40]:


flights = flights.merge(airport, on=['airport', 'airport_name', 'city', 'state']).drop(['airport', 'airport_name', 'city', 'state'], axis=1)
flights = flights.rename(columns={'id': 'airport_id'})


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

# In[41]:


flights.head(5)


# In[42]:


# reorder columns
flights = flights[['year','month','carrier_id','airport_id','arr_flights','arr_del15','arr_cancelled','arr_diverted','arr_delay','carrier_delay','weather_delay','nas_delay','security_delay','late_aircraft_delay']]


# In[307]:


flights.head()


# In[308]:


carrier.head()


# In[309]:


airport.head()


