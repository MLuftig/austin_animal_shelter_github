
'''import area'''
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

'''

define functions

'''
def import_data(file_name, table_name):
    print(f'Beginning to load {table_name}:')

    df = None

    try:
        df = pd.read_csv(file_name, low_memory = False)
        print(f'Success, with {len(df)} rows')
    except FileNotFoundError:
        print(f"ERROR: File not found at path: {file_name}. Returning None.")
        return None
    except Exception as e:
        print(f"ERROR loading from {file_name}: {e}")
    return df
    
    
    


def tablecheck(df):
    """
    Checks and prints the count of null (NaN) values for every column in a Pandas DataFrame.
    """
    print(f"*** errorcheck for: {df.name if hasattr(df, 'name') else 'Unnamed DF'} ***")
    print(f'duplicates in line_id: {df['line_id'].duplicated().sum()}')
    
    for column in df.columns: 
        null_count = df[column].isna().sum() 
        print(f'{column} nulls: {null_count}')

    print("-" * 30)

#this function makes the column names snake case and the column information clean and lower case alerting if unable to do so
def imported_data_clean(df_raw): 
    df = df_raw.copy() #to ensure the raw data is not damaged
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex = False) 
    df = df[~df.duplicated(subset=['animal_id', 'datetime'])].copy() 

    for column in df.columns:
        if df[column].dtype == 'object':
            try:
                df[column] = (
                    df[column].astype(str) .str.strip().str.lower())
            except AttributeError:
                print(f"Skipping non-string/mixed column: {column}")
    
    return df 

#this function creates the lineid which is a unique identifier but taking the datetime (ensuring in right format) and animal_id
def datetime_and_lineid(df):
    if df['datetime'].astype(str).str.contains(r'AM|PM', case = False, regex = True).any():
        df['datetime'] = pd.to_datetime(df['datetime'], format = ('%m/%d/%Y %I:%M:%S %p'), errors = 'coerce') 
        #intake_data['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        df['datetime'] = pd.to_datetime(df['datetime'], format = '%Y-%m-%d %H:%M:%S', errors = 'coerce')

    df['std_date_time'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
   
    df['line_id'] = df['animal_id'].astype(str) + '_' + df['std_date_time'].astype(str)
    return df


#this function adds rows to specifically list information in datetime.  based off that information, season, week day, and shift are added as well
def datetime_extraction(df):
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month

    conditions = [
        df['month'].isin([3, 4, 5]), # spring
        df['month'].isin([6, 7, 8]), # summer
        df['month'].isin([9, 10, 11]), # fall
        df['month'].isin([12, 1, 2]) # winter
    ]

    choices = [
        'spring',
        'summer',
        'autumn',
        'winter'
    ]

    df['season'] = np.select(conditions, choices, default = 'Unknown')
   
    df['weekday'] = df['datetime'].dt.day_name()
    df['hour'] = df['datetime'].dt.hour


    conditions = [
        (df['hour'] >= 7) & (df['hour'] < 16), # 7 AM to 3:59 PM
        (df['hour'] >= 16) & (df['hour'] <= 23), # 4 PM to 11:59 PM (Note: Using <= 23 for clarity)
        (df['hour'] >= 0) & (df['hour'] < 7) # 12 AM to 6:59 AM
    ]

    choices = [
        'day',
        'swing',
        'overnight'
    ]

    df['shift'] = np.select(conditions, choices, default = 'Unknown')
    return df


#this function checks the length of all the data in the column animal_id and sees if they are all returning a length of 7
def check_id_length(df):
    length_series = intake_table['animal_id'].str.len()
    num_animal_id_dif = (length_series != 7).sum()
    print(f'\n\n\nthere are {num_animal_id_dif} lines with a length different than 7')






'''

Body

'''


#main data files
intake_data_raw = import_data('Austin_Animal_Center_Intakes.csv', 'intake_data_raw')
outcome_data_raw = import_data('Austin_Animal_Center_Outcomes.csv', 'outcome_data_raw')

#made in google sheets with info exported from this data
shelter_locations = import_data('austin_shelter_locations.csv', 'shelter_locations')
akc_groups = import_data('dog_info.csv.csv', 'akc_groups')
bird_groups = import_data('bird_info.csv', 'bird_groups')
rabbit_info = import_data('rabbit_info.csv', 'rabbit_info')

'''
******************************************************
first we will work on the data to be exported as intake_table which will contain:

line_id
animal_id
datetime
intake_condition
year
month
season
weekday
hour
shift
'''

intake_data = imported_data_clean(intake_data_raw)


intake_data = datetime_and_lineid(intake_data)
intake_table = intake_data[['line_id', 'animal_id', 'datetime', 'intake_condition']].copy()

intake_table = datetime_extraction(intake_table)

'''the first colummn is line_id which was created'''
'''the second column is animal_id which is given when the animal is first taken in.  check to see if the the length is the same for all'''
check_id_length(intake_table)


'''the next column is intake condition'''
#first look at how many uniques there are
print(f'\n\n\nunique conditions in intake condition: {intake_table['intake_condition'].unique()}')

'''
at this point i have:
   - ensured the is a unique identified for every line
   - ensured there are no null values
   - ensured datetime is in the correct format
   - extracted some information from datetime for easier analysis later
   - ensured intake condition is striped of white space and in all the same format

this table is what i wanted it to be at this point.  i will move on to the outcome table
'''

'''
*****************************************************************************
this section will begin to clean the data and get things ready
'''

outcome_data = imported_data_clean(outcome_data_raw)
outcome_data = datetime_and_lineid(outcome_data)

'''now to start making the outcome table
here is what i want in it:
   - line_id
   - animal_id
   - datetime
   - type
   - subtype
   - month
   - year
   - day
   - weekday
   - time
   - hour
   - shift
'''

outcome_table = outcome_data[['line_id', 'animal_id', 'datetime', 'outcome_type', 'outcome_subtype']].copy()
outcome_table = datetime_extraction(outcome_table)


'''i want to make sure the format of the datetime is the same for both'''
print(f'\n\n\n**********\nintake data')
print(intake_table['datetime'].head())

print(f'\n\n\n**********\noutcome data: ')
print(outcome_table['datetime'].head())

print('\n\n\n\n\n\n\n**********************************')
length_series = outcome_table['animal_id'].str.len()
num_animal_id_dif = (length_series != 7).sum()
print(f'\n\n\nthere are {num_animal_id_dif} lines with a length different than 7 in the outcome data')
print(f'\n\n***************')

print(f'list of unique values in type: {outcome_table['outcome_type'].unique()}')
print(outcome_table[outcome_table['outcome_type'] == 'nan'])
outcome_table['outcome_type'] = outcome_table['outcome_type'].astype(str).str.replace('nan', 'unknown', case = False, regex = False)
print(f'list of unique values in type: {outcome_table['outcome_type'].unique()}')

print(f'list of unique values in subtype: {outcome_table['outcome_subtype'].unique()}')
outcome_table['outcome_subtype'] = outcome_table['outcome_subtype'].astype(str).str.replace('nan', 'unknown', case = False, regex = False)
#there really isnt much to do here.  i decided to keep the table long rather than creating a pivot table because the goal is to take this to sql, and the catagorical values will be easier to handle
#make sure that is written in the README file

'''conditions = [
    outcome_table['datetime'].dt.month.isin([3, 4, 5]), # spring
    outcome_table['datetime'].dt.month.isin([6, 7, 8]), # summer
    outcome_table['datetime'].dt.month.isin([9, 10, 11]), # fall
    outcome_table['datetime'].dt.month.isin([12, 1, 2]) # winter
]

#define the corresponding values
choices = [
    'spring',
    'summer',
    'autumn',
    'winter'
]


#apply the conditions using np.select
outcome_table['season'] = np.select(conditions, choices, default = 'Unknown')
outcome_table['weekday'] = outcome_table['datetime'].dt.day_name()
#intake_table['hour'] = intake_table['datetime'].dt.hour


#define the conditions (Boolean Series for each shift)
conditions = [
    (outcome_table['datetime'].dt.hour >= 7) & (outcome_table['datetime'].dt.hour < 16), # 7 AM to 3:59 PM
    (outcome_table['datetime'].dt.hour >= 16) & (outcome_table['datetime'].dt.hour <= 23), # 4 PM to 11:59 PM (Note: Using <= 23 for clarity)
    (outcome_table['datetime'].dt.hour >= 0) & (outcome_table['datetime'].dt.hour < 7) # 12 AM to 6:59 AM
]

#define the corresponding values
choices = [
    'day',
    'swing',
    'overnight'
]

#apply the conditions using np.select
outcome_table['shift'] = np.select(conditions, choices, default = 'Unknown')
'''

'''
now for the animal table.  this will have

from in	            from out
	
animal_id	            animal_id
name	                name
name_given_at_intake	name_given_by shelter
spp	                    spp
intake_age	            age_num
gender	                gender
altered	                altered
primary_breed	
secondary_breed	
is_mixed	
hair_length	
group	
primary_color	
secondary_color	
'''
'''print('\n\n\nierjhfqpiurfnpiquernfqpiuernfpiquernfpqirunfpqirufjn')
print(outcome_table.head())
print(intake_data.columns)'''