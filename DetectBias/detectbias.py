import pandas as pd
from bs4 import BeautifulSoup
import io

file_path = '/content/drive/MyDrive/Data Engineering - CS510/Lab Assignments for Xiangqian Zhang/trimet_stopevents_2022-12-07.html'

with open(file_path, 'r') as f:
    soup = BeautifulSoup(f, 'html.parser')

tables = soup.find_all('table')
print(f"Found {len(tables)} tables.")

# Read all tables into DataFrames, skip those that are empty or missing expected columns
dfs = []
for idx, table in enumerate(tables):
    try:
        df = pd.read_html(io.StringIO(str(table)))[0]
        # Check for required columns
        if 'trip_number' in df.columns and 'vehicle_number' in df.columns:
            dfs.append(df)
    except Exception as e:
        print(f"Skipping table {idx}: {e}")

# Concatenate all dataframes
all_df = pd.concat(dfs, ignore_index=True)
print("Concatenated shape:", all_df.shape)
print(all_df.columns)

# Rename 'trip_number' to 'trip_id'
all_df.rename(columns={'trip_number': 'trip_id'}, inplace=True)

# Convert arrive_time (seconds since midnight) to datetime
from datetime import datetime, timedelta
base_date = datetime(2022, 12, 7)
all_df['tstamp'] = all_df['arrive_time'].apply(lambda x: base_date + timedelta(seconds=int(x)))

# Select only required columns
stops_df = all_df[['trip_id', 'vehicle_number', 'tstamp', 'location_id', 'ons', 'offs']]

print(stops_df.head())
print("Final shape of stops_df:", stops_df.shape)

num_vehicles = stops_df['vehicle_number'].nunique()
print("A. Number of vehicles:", num_vehicles)

num_locations = stops_df['location_id'].nunique()
print("B. Number of stop locations:", num_locations)

min_tstamp = stops_df['tstamp'].min()
max_tstamp = stops_df['tstamp'].max()
print("C. Min tstamp:", min_tstamp)
print("   Max tstamp:", max_tstamp)

num_with_boarding = (stops_df['ons'] >= 1).sum()
print("D. Number of stop events with at least one boarding:", num_with_boarding)

percent_with_boarding = (num_with_boarding / len(stops_df)) * 100
print(f"E. Percentage of stop events with at least one boarding: {percent_with_boarding:.2f}%")

stops_at_6913 = stops_df[stops_df['location_id'] == 6913]
num_stops_6913 = len(stops_at_6913)
print("3A-i. Number of stops at location 6913:", num_stops_6913)

num_buses_6913 = stops_at_6913['vehicle_number'].nunique()
print("3A-ii. Number of unique vehicles at location 6913:", num_buses_6913)

num_boardings_6913 = (stops_at_6913['ons'] >= 1).sum()
percent_boardings_6913 = (num_boardings_6913 / num_stops_6913) * 100 if num_stops_6913 else 0
print(f"3A-iii. Percentage of stops at location 6913 with at least one boarding: {percent_boardings_6913:.2f}%")

stops_by_4062 = stops_df[stops_df['vehicle_number'] == 4062]
num_stops_4062 = len(stops_by_4062)
print("3B-i. Number of stops by vehicle 4062:", num_stops_4062)

total_boarded_4062 = stops_by_4062['ons'].sum()
print("3B-ii. Total passengers boarded vehicle 4062:", total_boarded_4062)

total_deboarded_4062 = stops_by_4062['offs'].sum()
print("3B-iii. Total passengers deboarded vehicle 4062:", total_deboarded_4062)

num_boarding_stops_4062 = (stops_by_4062['ons'] >= 1).sum()
percent_boarding_stops_4062 = (num_boarding_stops_4062 / num_stops_4062) * 100 if num_stops_4062 else 0
print(f"3B-iv. Percentage of stops with at least one boarding for vehicle 4062: {percent_boarding_stops_4062:.2f}%")

import pandas as pd
from scipy.stats import binomtest

# Assume stops_df is already created

# 1. Calculate overall boarding rate (system-wide proportion) from step 2E
p_system = (stops_df['ons'] >= 1).mean()
print(f"System-wide boarding rate (p_system): {p_system:.4f}")

# 2. Group by vehicle_number and compute required stats for each bus
results = []

for vehicle, group in stops_df.groupby('vehicle_number'):
    num_stops = len(group)  # i. count the number of stops events
    num_boarding_stops = (group['ons'] >= 1).sum()  # ii. stops with at least 1 boarding
    percent_boarding = (num_boarding_stops / num_stops) * 100 if num_stops else 0  # iii. %
    # iv. Binomial test: Is this bus's boarding rate different from system-wide rate?
    p_value = binomtest(num_boarding_stops, num_stops, p_system).pvalue
    results.append({
        'vehicle_number': vehicle,
        'num_stops': num_stops,
        'num_boarding_stops': num_boarding_stops,
        'percent_boarding': percent_boarding,
        'p_value': p_value
    })

bus_boarding_df = pd.DataFrame(results)

# Sort by vehicle_number for nice display
bus_boarding_df.sort_values('vehicle_number', inplace=True)

# Show the first few rows
display(bus_boarding_df.head(10))

# Filter for vehicles with significant boarding bias (p < 0.05)
biased_buses = bus_boarding_df[bus_boarding_df['p_value'] < 0.05].copy()

# Sort by p_value
biased_buses = biased_buses.sort_values('p_value')

# Show the most significant biases
display(biased_buses[['vehicle_number', 'num_stops', 'num_boarding_stops', 'percent_boarding', 'p_value']])

import pandas as pd

relpos_df = pd.read_csv('/content/drive/MyDrive/Data Engineering - CS510/Lab Assignments for Xiangqian Zhang/trimet_relpos_2022-12-07.csv')
print(relpos_df.columns)
print(relpos_df.head())

relpos_all = relpos_df['RELPOS'].values
print(f"Total RELPOS points: {len(relpos_all)}")

from scipy.stats import ttest_1samp

vehicle_results = []
for vehicle, group in relpos_df.groupby('VEHICLE_NUMBER'):
    relpos_values = group['RELPOS'].values
    # Null hypothesis: this vehicle's mean RELPOS is the same as the system's (which should be zero)
    # t-test vs. population mean (0.0)
    tstat, pvalue = ttest_1samp(relpos_values, 0.0, nan_policy='omit')
    vehicle_results.append({
        'vehicle_number': vehicle,
        'num_records': len(relpos_values),
        'mean_relpos': relpos_values.mean(),
        'p_value': pvalue
    })

vehicle_ttest_df = pd.DataFrame(vehicle_results)
vehicle_ttest_df.sort_values('p_value', inplace=True)

biased_gps = vehicle_ttest_df[vehicle_ttest_df['p_value'] < 0.005]
display(biased_gps[['vehicle_number', 'num_records', 'mean_relpos', 'p_value']])

from scipy.stats import chi2_contingency

# Step 6B: System totals
total_ons = stops_df['ons'].sum()
total_offs = stops_df['offs'].sum()
print(f"System total ons: {total_ons}, offs: {total_offs}")

# Step 6C: For each vehicle, compare its offs/ons to system with chi-squared test
results = []
for vehicle, group in stops_df.groupby('vehicle_number'):
    ons_vehicle = group['ons'].sum()
    offs_vehicle = group['offs'].sum()
    
    # Construct contingency table:
    #      | ons     | offs
    # --------------------------
    # bus  | x1      | y1
    # rest | total-x1| total-y1
    contingency = [
        [ons_vehicle, offs_vehicle],
        [total_ons - ons_vehicle, total_offs - offs_vehicle]
    ]
    chi2, p_value, dof, expected = chi2_contingency(contingency)
    results.append({
        'vehicle_number': vehicle,
        'ons': ons_vehicle,
        'offs': offs_vehicle,
        'p_value': p_value
    })

chi2_df = pd.DataFrame(results)
biased_chi2 = chi2_df[chi2_df['p_value'] < 0.05].sort_values('p_value')

display(biased_chi2)



