import pandas as pd

# Read CSV files
cases_df = pd.read_csv("/content/drive/MyDrive/Data Engineering - CS510/Lab Assignments for Xiangqian Zhang/covid_confirmed_usafacts.csv")
deaths_df = pd.read_csv("/content/drive/MyDrive/Data Engineering - CS510/Lab Assignments for Xiangqian Zhang/covid_deaths_usafacts.csv")
census_df = pd.read_csv("/content/drive/MyDrive/Data Engineering - CS510/Lab Assignments for Xiangqian Zhang/acs2017_county_data.csv")

# Trim the DataFrames
cases_df = cases_df[["County Name", "State", "2023-07-23"]]
deaths_df = deaths_df[["County Name", "State", "2023-07-23"]]
census_df = census_df[["County", "State", "TotalPop", "IncomePerCap", "Poverty", "Unemployment"]]

# Show column headers
print("cases_df columns:", cases_df.columns.tolist())
print("deaths_df columns:", deaths_df.columns.tolist())
print("census_df columns:", census_df.columns.tolist())

# Strip trailing spaces from 'County Name'
cases_df["County Name"] = cases_df["County Name"].str.strip()
deaths_df["County Name"] = deaths_df["County Name"].str.strip()

# Count how many rows have County Name == "Washington County"
num_washington_cases = (cases_df["County Name"] == "Washington County").sum()
num_washington_deaths = (deaths_df["County Name"] == "Washington County").sum()

print("Number of 'Washington County' in cases_df:", num_washington_cases)
print("Number of 'Washington County' in deaths_df:", num_washington_deaths)

# Remove "Statewide Unallocated" rows
cases_df = cases_df[cases_df["County Name"] != "Statewide Unallocated"]
deaths_df = deaths_df[deaths_df["County Name"] != "Statewide Unallocated"]

# Show the number of remaining rows
print("Remaining rows in cases_df:", len(cases_df))
print("Remaining rows in deaths_df:", len(deaths_df))

# Mapping of US state abbreviations to full names
us_state_abbrev = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
}

# Apply mapping to convert state abbreviations to full names
cases_df["State"] = cases_df["State"].map(us_state_abbrev)
deaths_df["State"] = deaths_df["State"].map(us_state_abbrev)

# Show the first few rows of cases_df
print(cases_df.head())

# Create 'key' for cases and deaths
cases_df["key"] = cases_df["County Name"] + cases_df["State"]
deaths_df["key"] = deaths_df["County Name"] + deaths_df["State"]

# Create 'key' for census
census_df["key"] = census_df["County"] + census_df["State"]

# Set 'key' as the index
cases_df.set_index("key", inplace=True)
deaths_df.set_index("key", inplace=True)
census_df.set_index("key", inplace=True)

# Show the first few rows of census_df
print(census_df.head())

# Rename the '2023-07-23' column
cases_df.rename(columns={"2023-07-23": "Cases"}, inplace=True)
deaths_df.rename(columns={"2023-07-23": "Deaths"}, inplace=True)

# Show column names
print("cases_df columns:", cases_df.columns.tolist())
print("deaths_df columns:", deaths_df.columns.tolist())

# Drop 'County Name' and 'State' from deaths_df and census_df before joining
deaths_df_cleaned = deaths_df.drop(columns=["County Name", "State"])
census_df_cleaned = census_df.drop(columns=["County", "State"])

# Join the dataframes
join_df = cases_df.join(deaths_df_cleaned).join(census_df_cleaned)

# Add per capita metrics
join_df["CasesPerCap"] = join_df["Cases"] / join_df["TotalPop"]
join_df["DeathsPerCap"] = join_df["Deaths"] / join_df["TotalPop"]

# Show number of rows
print("Number of rows in join_df:", len(join_df))

# Compute correlation matrix among numeric columns
correlation_matrix = join_df.corr(numeric_only=True)

# Show the matrix
print(correlation_matrix)

import seaborn as sns
import matplotlib.pyplot as plt

# Plot the correlation heatmap
plt.figure(figsize=(10, 8))  # Optional: set the figure size
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)

plt.title('Correlation Matrix Heatmap')
plt.tight_layout()
plt.show()

