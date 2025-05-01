# This code was implemented in google Colab

import pandas as pd

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/DataEng/bc_trip259172515_230215.csv')

df.head()

# Using drop method
df_filtered = df.drop(columns=['EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP'])

# Using usecols on read_csv
columns_to_keep = ['EVENT_NO_TRIP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE']
df_usecols = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/DataEng/bc_trip259172515_230215.csv", usecols=columns_to_keep)

df_usecols.head()

from datetime import datetime, timedelta

# Parse OPD_DATE as datetime and ACT_TIME as seconds
def decode_timestamp(row):
    base_date = datetime.strptime(row['OPD_DATE'].split(':')[0], '%d%b%Y')
    time_offset = timedelta(seconds=int(row['ACT_TIME']))
    return base_date + time_offset

df_usecols['TIMESTAMP'] = df_usecols.apply(decode_timestamp, axis=1)

df_usecols = df_usecols.drop(columns=['OPD_DATE', 'ACT_TIME'])

df_usecols_filtered = df_usecols

# Calculate dMETERS and dTIMESTAMP (in seconds)
df_usecols_filtered['dMETERS'] = df_usecols_filtered['METERS'].diff()
df_usecols_filtered['dTIMESTAMP'] = df_usecols_filtered['TIMESTAMP'].diff().dt.total_seconds()

# Compute SPEED
df_usecols_filtered['SPEED'] = df_usecols_filtered.apply(
    lambda row: row['dMETERS'] / row['dTIMESTAMP'] if row['dTIMESTAMP'] and row['dTIMESTAMP'] > 0 else 0, axis=1
)

# Drop intermediate columns
df_usecols_filtered = df_usecols_filtered.drop(columns=['dMETERS', 'dTIMESTAMP'])

# Speed stats
print("Min:", df_usecols_filtered['SPEED'].min())
print("Max:", df_usecols_filtered['SPEED'].max())
print("Mean:", df_usecols_filtered['SPEED'].mean())

import pandas as pd
from datetime import datetime, timedelta

# Load the multi-trip dataset
df_usecols = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/DataEng/bc_veh4223_230215.csv", usecols=[
    'EVENT_NO_TRIP', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'OPD_DATE',
    'GPS_LONGITUDE', 'GPS_LATITUDE'
])

# Decode TIMESTAMP from OPD_DATE and ACT_TIME
def decode_timestamp(row):
    base_date = datetime.strptime(row['OPD_DATE'].split(':')[0], '%d%b%Y')
    time_offset = timedelta(seconds=int(row['ACT_TIME']))
    return base_date + time_offset

df_usecols['TIMESTAMP'] = df_usecols.apply(decode_timestamp, axis=1)

# Drop OPD_DATE and ACT_TIME
df_usecols_filtered = df_usecols.drop(columns=['OPD_DATE', 'ACT_TIME'])

# Sort by EVENT_NO_TRIP and TIMESTAMP
df_usecols_filtered = df_usecols_filtered.sort_values(by=['EVENT_NO_TRIP', 'TIMESTAMP'])

# Compute dMETERS and dTIMESTAMP per trip
df_usecols_filtered['dMETERS'] = df_usecols_filtered.groupby('EVENT_NO_TRIP')['METERS'].diff()
df_usecols_filtered['dTIMESTAMP'] = df_usecols_filtered.groupby('EVENT_NO_TRIP')['TIMESTAMP'].diff().dt.total_seconds()

# Compute SPEED safely
df_usecols_filtered['SPEED'] = df_usecols_filtered.apply(
    lambda row: row['dMETERS'] / row['dTIMESTAMP'] if pd.notnull(row['dTIMESTAMP']) and row['dTIMESTAMP'] > 0 else 0,
    axis=1
)

# Drop helper columns
df_usecols_filtered = df_usecols_filtered.drop(columns=['dMETERS', 'dTIMESTAMP'])

# Extract max speed row
max_speed_row = df_usecols_filtered.loc[df_usecols_filtered['SPEED'].idxmax()]
max_speed = max_speed_row['SPEED']
max_time = max_speed_row['TIMESTAMP']
max_location = (max_speed_row['GPS_LATITUDE'], max_speed_row['GPS_LONGITUDE'])

# Compute median speed
median_speed = df_usecols_filtered['SPEED'].median()

# Output results
print(f"Max Speed: {max_speed:.2f} m/s")
print(f"Location: {max_location}")
print(f"Time: {max_time}")
print(f"Median Speed: {median_speed:.2f} m/s")



















