# A. How large is this data set, specifically, how many bytes? How many records?
import numpy as np
import pandas as pd
import os

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/DataEng/employees.csv')
data = df.to_numpy()

file_path = '/content/drive/MyDrive/Colab Notebooks/DataEng/employees.csv'
file_size = os.path.getsize(file_path)

print(f"The size of employees.csv is: {file_size} bytes")

print(df.head())
print(data.nbytes)
print(data.shape)
 The size of employees.csv is: 3030111 bytes
     eid           name                            title  birth_date  \
0   4998  Steven Martin              Solicitor, Scotland  1981-12-25   
1   9579  Craig Sweeney         Technical sales engineer  1988-03-22   
2    972    Brian Myers                          Actuary  2001-02-17   
3  20236     Roy Romero           Embryologist, clinical  2003-07-22   
4  13706   Tracy Martin  Engineer, manufacturing systems  2005-01-09   

    hire_date              address            city        country postal_code  \
0  2016-05-26   278 Bradshaw Roads      Andrewview  United States       84011   
1  2022-02-09         175 Ray Port      Thomasview  United States       56647   
2  2016-01-13  09861 Anderson Burg     Leonardfort  United States       84361   
3  2019-01-30  358 Brown Junctions        Josefurt  United States       63198   
4  2022-04-16     3158 James Curve  New Philipland  United States       18843   

                   phone  salary  reports_to  
0           475.311.6571   60000         673  
1           951.536.1840  100000         524  
2        +1-834-655-3579   60000         890  
3  +1-258-757-7062x40380   60000         566  
4      (788)207-9141x701   60000         117  
1920000
(20000, 12)

# Existence Assertion
# Assertion: every record has a non-null name field

# Write a python program called emp_validate.py (or something similar) that validates this assertion.

# The program should just count rows that do not satisfy this assertion. No need to modify or delete the invalid records.

# How many records violate this assertion?

# Define a new, different Existence Assertion

# (Grad Students only) Add code to your program to validate your assertion

# (Grad Students only) How many records violate your new assertion?


# df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/DataEng/employees.csv')

missing_names = df['name'].isnull().sum()
print(f"Missing Names: {missing_names}")

print(f"Different Existence Assertion: Every employee record must have a non-null phone number.")
missing_phone_count = df['phone'].isnull().sum()
print(f"Records with missing phone number: {missing_phone_count}")

# Missing Names: 19
# Missing eid: 0
# Different Existence Assertion: Every employee record must have a non-null phone number.
# Records with missing phone number: 0

# Limit Assertion
# Assertion: every employee was hired no earlier than 2015

# Add code to your program to validate this assertion

# How many records violate this assertion?

# Define a new, different Limit Assertion

# (Grad Students only) Add code to your program to validate this assertion

# (Grad Students only) How many records violate your new assertion?

# Convert hire_date to datetime
df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')

# Find records where hire date is before Jan 1, 2015
early_hires = df[df['hire_date'] < pd.to_datetime('2015-01-01')]
early_hires_count = early_hires.shape[0]

print(f"Records with hire_date before 2015: {early_hires_count}")
print(early_hires[['eid', 'name', 'hire_date']])

print(f"Different Limit Assertion: No employee is paid more than $30,000,000.")
# Find records where salary exceeds 30,000,000
high_salary = df[df['salary'] > 30000000]
high_salary_count = high_salary.shape[0]

print(f"Records with salary > 30,000,000: {high_salary_count}")
print(high_salary[['eid', 'name', 'salary']])
#  Records with hire_date before 2015: 18
#          eid                 name  hire_date
# 559    19128       Patrick George 1983-09-11
# 2691   18579         Andrew Clark 1984-04-16
# 4152   12803          Lisa Gentry 1981-07-04
# 7405   31884     Gregory Gonzalez 1980-09-06
# 7651   29334        Andrew Hinton 1985-01-05
# 8544   13783    Kimberly Crawford 1979-06-03
# 9657   10920         Ryan Bridges 1976-03-02
# 9811   32720       Taylor Bennett 1976-12-04
# 9966   11186     Shannon Figueroa 1985-02-03
# 10183  23140        Karen Edwards 1979-08-23
# 10281   8910       Laura Martinez 1983-04-12
# 11852  18220         Jennifer Liu 1978-07-02
# 13114  30899           Kent Stout 1984-04-01
# 14408  31395      Tonya Dickerson 1978-12-22
# 14829   4129        Jeremy Bryant 1976-03-08
# 15230   8999         Gary Johnson 1982-03-19
# 16440  37419         Angela Jones 1976-05-18
# 17375  38964  Christopher Johnson 1977-08-25
# Different Limit Assertion: No employee is paid more than $30,000,000.
# Records with salary > 30,000,000: 5
#          eid               name     salary
# 361    15730     Charles Powers   47230000
# 6425   14186       Daniel Stone  370260000
# 14118  24661      Travis Jordan   47490000
# 15077  12042  Cassandra Shelton   47490000
# 19420   4998   Beverly Schaefer  152270000

# Intra-record Assertion
# Assertion: each employee was born before they were hired

# Add code to your program to validate this assertion

# How many records violate this assertion?

# Define a new, different Intra-record Assertion

# (Grad Students only) Add code to your program to validate this assertion

# (Grad Students only) How many records violate this assertion?

# Convert to datetime
df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')

# Find rows where birth_date is AFTER or EQUAL TO hire_date
invalid_birth_vs_hire = df[df['birth_date'] >= df['hire_date']]
invalid_birth_count = invalid_birth_vs_hire.shape[0]

print(f"Records where birth_date is after or equal to hire_date: {invalid_birth_count}")
print(invalid_birth_vs_hire[['eid', 'name', 'birth_date', 'hire_date']])

print(f"Different Intra-record Assertion: An employee’s salary must be greater than $0.")
invalid_salary = df[df['salary'] <= 0]
invalid_salary_count = invalid_salary.shape[0]

print(f"Records where salary <= 0: {invalid_salary_count}")
print(invalid_salary[['eid', 'name', 'salary']])
#  Records where birth_date is after or equal to hire_date: 13
#          eid                 name birth_date  hire_date
# 559    19128       Patrick George 1999-04-16 1983-09-11
# 4152   12803          Lisa Gentry 1985-04-19 1981-07-04
# 7405   31884     Gregory Gonzalez 2002-06-27 1980-09-06
# 7651   29334        Andrew Hinton 1986-06-14 1985-01-05
# 9657   10920         Ryan Bridges 1998-07-02 1976-03-02
# 9811   32720       Taylor Bennett 1991-04-23 1976-12-04
# 10183  23140        Karen Edwards 1999-01-18 1979-08-23
# 10281   8910       Laura Martinez 1995-07-07 1983-04-12
# 11852  18220         Jennifer Liu 1994-06-24 1978-07-02
# 14408  31395      Tonya Dickerson 2001-11-03 1978-12-22
# 14829   4129        Jeremy Bryant 2002-12-18 1976-03-08
# 16440  37419         Angela Jones 1999-01-27 1976-05-18
# 17375  38964  Christopher Johnson 1981-03-10 1977-08-25
# Different Intra-record Assertion: An employee’s salary must be greater than $0.
# Records where salary <= 0: 0
# Empty DataFrame
# Columns: [eid, name, salary]
# Index: []

# Inter-record Assertion
# Assertion: each employee has a manager who is a known employee

# Add code to your program to validate this assertion

# How many records violate this assertion? Define a new, different Inter-record Assertion

# (Grad Students only) Add code to your program to validate this assertion

# (Grad Students only) How many records violate your new assertion?


# Make sure 'reports_to' is not null AND the ID isn't in the 'eid' column
invalid_managers = df[~df['reports_to'].isnull() & ~df['reports_to'].isin(df['eid'])]
invalid_manager_count = invalid_managers.shape[0]

print(f"Records with unknown manager (reports_to not in eid): {invalid_manager_count}")
print(invalid_managers[['eid', 'name', 'reports_to']].head())

print(f"Different Inter-record Assertion: No employee reports to themselves.")
self_reporting = df[df['eid'] == df['reports_to']]
self_reporting_count = self_reporting.shape[0]

print(f"Records where employee reports to themselves: {self_reporting_count}")
print(self_reporting[['eid', 'name', 'reports_to']].head())
#  Records with unknown manager (reports_to not in eid): 7673
#       eid            name  reports_to
# 3   20236      Roy Romero         566
# 4   13706    Tracy Martin         117
# 5    1379   Andrea Miller         845
# 12  27178    Julie Tanner         901
# 14   6359  Roberto Monroe         193
# Different Inter-record Assertion: No employee reports to themselves.
# Records where employee reports to themselves: 1
#        eid            name  reports_to
# 15802  131  Jessica Mclean         131

# Summary Assertion
# Assertion: each city has more than one employee

# Add code to your program to validate this assertion

# Is the data set valid with respect to this assertion?

# Define a new, different Summary Assertion

# (Grad Students only) Add code to your program to validate this assertion

# (Grad Students only) Is the data set valid with respect to your new assertion?

# Count how many employees in each city
city_counts = df['city'].value_counts()

print(f"Number of cities: {city_counts.shape[0]}")

# Cities with only one employee violate the rule
invalid_cities = city_counts[city_counts == 1]
invalid_city_count = invalid_cities.shape[0]

print(f"Number of cities with only one employee: {invalid_city_count}")
print("Examples of violating cities:")
print(invalid_cities.head())

print(f"Different Summary Assertion: Each postal_code area must have at least 1 employees.")
# Count missing postal codes
missing_postal = df['postal_code'].isnull().sum()

# Count empty strings as well (e.g., "")
empty_postal = (df['postal_code'].astype(str).str.strip() == "").sum()

print(f"Records with missing postal_code: {missing_postal}")
print(f"Records with empty postal_code: {empty_postal}")
#  Number of cities with only one employee: 10024
# Examples of violating cities:
# city
# West Tylerside    1
# Farrellmouth      1
# Karlstad          1
# Austinport        1
# Annetteville      1
# Name: count, dtype: int64
# Different Summary Assertion: Each postal_code area must have at least 1 employees.
# Records with missing postal_code: 0
# Records with empty postal_code: 0

# Statistical Assertion
# Assertion: the salaries are normally distributed

# Add code to your program to validate this assertion

# Is the data set valid with respect to this assertion?

# For this show a screenshot of a histogram of salaries and state whether the histogram appears to resemble a normal distribution.

# Define a new, different Statistical Assertion (Grad Students only) validate this assertion

import matplotlib.pyplot as plt
import seaborn as sns

# Histogram of salaries
plt.figure(figsize=(10, 6))
sns.histplot(df['salary'], kde=True, bins=5)

plt.title("Histogram of Employee Salaries")
plt.xlabel("Salary")
plt.ylabel("Count")
plt.grid(True)
plt.tight_layout()

# Save the plot (in Colab it can be downloaded)
plt.savefig("salary_histogram.png")  # Optional for slides
plt.show()

print(f"from the plot, the data set is not valid with respect to this assertion")

print(f"Different Statistical Assertion: Most salaries are between $40,000 and $120,000.")
within_range = df[(df['salary'] >= 40000) & (df['salary'] <= 120000)]
within_range_count = within_range.shape[0]
total_count = df.shape[0]
percent_within_range = (within_range_count / total_count) * 100

print(f"Salaries between $40k–$120k: {within_range_count} of {total_count} "
      f"({percent_within_range:.2f}%)")

# from the plot, the data set is not valid with respect to this assertion
# Different Statistical Assertion: Most salaries are between $40,000 and $120,000.
# Salaries between $40k–$120k: 18414 of 20000 (92.07%)
