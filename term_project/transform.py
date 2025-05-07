#!/usr/bin/env python
# coding: utf-8

# In[1]:


from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


import requests 
import pandas as pd  
import sqlite3 
import json  
import matplotlib.pyplot as plt  
import os 


# Display confirmation
print("Libraries imported successfully!")


# In[2]:


import pandas as pd


df = pd.read_json('data/api_data.json')


print(df.head())


# In[3]:


df.info()


# In[4]:


df.drop(columns=[
    'location',
    ':@computed_region_awaf_s7ux',
    ':@computed_region_6mkv_f3dw',
    ':@computed_region_vrxf_vc4k',
    ':@computed_region_bdys_3d7i',
    ':@computed_region_43wa_7qmu'
], inplace=True)


# In[5]:


df.info()


# inspection_id

# In[6]:


df = df[df['inspection_type'].notnull()]
df['inspection_id'] = pd.to_numeric(df['inspection_id'], errors='coerce')
df = df.dropna(subset=['inspection_id'])
df['inspection_id'] = df['inspection_id'].astype(int)
df = df.drop_duplicates(subset='inspection_id', keep='first')



# dba_name & aka_name

# In[7]:


df['dba_name'] = df['aka_name'].combine_first(df['dba_name'])

# Fill aka_name with updated dba_name if it's missing
df['aka_name'] = df['aka_name'].fillna(df['dba_name'])

# Make both columns fully uppercase
df['dba_name'] = df['dba_name'].str.upper()
df['aka_name'] = df['aka_name'].str.upper()


# liscene_

# In[8]:


df = df[df['license_'].notnull()]
df = df[df['license_'] != 0]
df['license_'] = pd.to_numeric(df['license_'], errors='coerce', downcast='integer')


# facility_type

# In[9]:


# Remove rows with missing facility_type


# In[10]:


df = df.dropna(subset=['facility_type'])


# In[11]:


import re

def map_facility_type(val):
    if pd.isna(val): return 'unknown'
    val = val.lower()
    if re.search(r'restaurant|restu|restaurant bar', val): return 'restaurant'
    if re.search(r'daycare|after school|children.*services|1023|combo', val): return 'daycare'
    if re.search(r'school|cafeteria|university|college', val): return 'school'
    if re.search(r'grocery|taqueria|butcher', val): return 'grocery store'
    if re.search(r'bakery|pastry', val): return 'bakery'
    if re.search(r'coffee|tea|cafe', val): return 'coffee shop'
    if re.search(r'ice cream|paleteria|frozen dessert|gelato', val): return 'ice cream'
    if re.search(r'tavern|bar|liquor|lounge', val): return 'tavern/bar'
    if re.search(r'mobile|push cart|vending machine', val): return 'mobile vendor'
    if re.search(r'chur|church', val): return 'church'
    if re.search(r'banquet|cater|event|venue', val): return 'event/catering'
    if re.search(r'shared kitchen|commissary', val): return 'shared kitchen'
    if re.search(r'hospital|nursing|rehab|senior|care', val): return 'healthcare'
    if re.search(r'convenience|dollar|store|retail', val): return 'retail store'
    if re.search(r'gym|fitness|health club|nutrition', val): return 'fitness'
    return 'other'


# In[12]:


df['facility_type_clean'] = (
    df['facility_type']
    .str.lower()
    .str.strip()
    .str.replace(r'[-_/]', ' ', regex=True)
)


# risk

# In[37]:


df = df[df['risk'].notnull()]
print("\nUnique risk values:")
print(df['results'].unique())


# address

# In[ ]:





# state

# In[14]:


df = df[df['state'] == 'IL']


# city

# In[15]:


df['city'] = df['city'].str.upper()


# In[16]:


unique_counts = df['city'].value_counts()
print(unique_counts)


# In[17]:


valid_cities = [
    'Chicago', 'Grayslake', 'Brookfield', 'Skokie', 'Evanston', 'Merrillville',
    'Burbank', 'Evergreen Park', 'Naperville', 'Matteson', 'Berwyn', 'Oak Park',
    'Highland Park', 'Plainfield', 'Western Springs', 'Schaumburg', 'Torrance',
    'Summit', 'Lake Zurich', 'Whiting', 'Glen Ellyn', 'Los Angeles', 'Calumet City',
    'Burnham', 'Oak Lawn', 'Morton Grove', 'Bridgeview', 'Griffith', 'New York',
    'Elmhurst', 'New Holstein', 'Algonquin', 'Niles', 'Lansing', 'Wadsworth',
    'Wilmette', 'Wheaton', 'Rosemont', 'Palos Park', 'Elk Grove Village', 'Cicero',
    'Maywood', 'Lake Bluff', 'Schiller Park', 'Bannockburn', 'Bloomingdale',
    'Norridge', 'Charles A Hayes', 'Chicago Heights', 'Justice', 'Tinley Park',
    'Lombard', 'East Hazel Crest', 'Country Club Hills', 'Streamwood',
    'Bolingbrook', 'Des Plaines', 'Olympia Fields', 'Alsip', 'Blue Island',
    'Glencoe', 'Frankfort', 'Broadview', 'Worth'
]


# In[18]:


from rapidfuzz import process, fuzz
import pandas as pd

def clean_and_standardize_cities(df, valid_cities, threshold=80):
    print("Unique cities before cleaning:")
    print(df['city'].value_counts())

    # Normalize valid cities and create a map
    valid_cities_normalized = [city.lower().strip() for city in valid_cities]
    valid_city_map = dict(zip(valid_cities_normalized, valid_cities))  


    df['city_normalized'] = df['city'].str.lower().str.strip()


    def best_match(city):
        result = process.extractOne(city, valid_cities_normalized, scorer=fuzz.ratio)
        if result:
            match, score, _ = result
            if score >= threshold:
                return valid_city_map[match]
        return None

    # Apply matching
    df['city_cleaned'] = df['city_normalized'].apply(best_match)


    df_cleaned = df[df['city_cleaned'].notnull()].copy()
    df_cleaned['city'] = df_cleaned['city_cleaned']
    df_cleaned.drop(columns=['city_normalized', 'city_cleaned'], inplace=True)

    print("\nUnique cities after cleaning:")
    print(df_cleaned['city'].value_counts())

    return df_cleaned


# In[19]:


df = clean_and_standardize_cities(df, valid_cities)


# zip

# In[20]:


df = df[df['zip'].notnull()]
df['zip'] = pd.to_numeric(df['zip'], errors='coerce', downcast='integer')


# latitude and longitude

# In[21]:


from mapbox import Geocoder
import pandas as pd
import time
from tqdm import tqdm


MAPBOX_TOKEN = "pk.eyJ1IjoiZW1pbHlqdWFyZXoiLCJhIjoiY205dDVjeHZlMDhhZDJqb3QwanU2YTl3cyJ9.K6u54UVcXIlN7RZFj-cNLQ" #default public token, not a secret key
# mapbox public keys according to their webpage "they can be safely exposed in web browsers, mobile apps, and other client environments" so we chose to leave it in the source code
geocoder = Geocoder(access_token=MAPBOX_TOKEN)

tqdm.pandas()


def mapbox_geocode(row):
    if pd.notna(row['latitude']) and pd.notna(row['longitude']):
        return row['latitude'], row['longitude']
    
    address = f"{row['address']}, {row['city']}, {row['state']} {int(row['zip']) if pd.notna(row['zip']) else ''}"
    try:
        response = geocoder.forward(address, limit=1)
        geojson = response.geojson()
        if geojson['features']:
            coords = geojson['features'][0]['geometry']['coordinates']
            # Mapbox returns [longitude, latitude]
            return coords[1], coords[0]
    except Exception as e:
        print(f"Error geocoding: {address} -> {e}")
    
    time.sleep(0.5)  # Throttle just in case
    return None, None

# Filter rows where lat/lon are missing
missing_coords = df[df['latitude'].isna() | df['longitude'].isna()]

# Apply Mapbox geocoding
coords = missing_coords.progress_apply(mapbox_geocode, axis=1)

# Fill in the lat/lon
df.loc[coords.index, ['latitude', 'longitude']] = list(coords)


# inspection_date

# In[22]:


df['inspection_date'] = pd.to_datetime(df['inspection_date'], format="%Y-%m-%dT%H:%M:%S.%f", errors='coerce')
df['inspection_date'][:3]


# inspection_type

# In[23]:


df = df[df['inspection_type'].notnull()]


# results

# In[24]:


df = df[df['results'] != 'Out of Business']
df = df[df['results'] != 'Business Not Located']


# violations

# In[25]:


# Extracting the violation number from the violation

df["violations"] = df["violations"].fillna("")
violations_list = []
for violation in df["violations"]:
    violations_list += [violation.split(' | ')]

df["violations_list"] = violations_list
violations_list


# In[26]:


violation_number_list = []
import re

for violation in df["violations_list"]:
    number_list = []
    for v in violation:
        match = re.match(r"^\d+", v)  # Extract leading numbers
        if match:
            number_list.append(match.group())  # Append extracted violation number
    
    violation_number_list.append(number_list)  
df["violation_num_list"] = violation_number_list


# 

# In[27]:


# Check if there are any rows left with missing ZIP codes
missing_zip_count_after = df['license_'].isna().sum()
print(f"Number of missing License # after dropping rows: {missing_zip_count_after}")


# In[28]:


print("Null values count:")
print("dba_name:", df['inspection_type'].isnull().sum())


# In[29]:


unique_counts = df['results'].value_counts()
print(unique_counts)


# In[30]:


unique_counts = df['inspection_type'].value_counts()
print(unique_counts)


# In[31]:


# Display the first few rows of the DataFrame
print(df.head())


# In[32]:


null_counts = df.isnull().sum()
print("Null values in each column:")
print(null_counts)


# In[32]:


count = ((df['results'] == 'Fail') & (df['violations'].isnull())).sum()
print("Number of 'Fail' rows with missing violations:", count)


# In[34]:


df.to_csv('data/cleaned_chicago_data.csv', index=False)

