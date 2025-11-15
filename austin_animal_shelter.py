'''
to me:

ok i have done a LOT of work cleaning this up and it is not in vain.  BUT you have now i think identified the way you would like your data to look. a lot of this will be cut and paste, but it will be worth starting over and doing it right
'''

'''import area'''
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

#these are the main data files
intake_data_raw = pd.read_csv('/Users/nothing/Documents/data analyst portfolio/Austin_animal_center/austin_animal_shelter_github/Austin_Animal_Center_Intakes.csv')
outcome_data_raw = pd.read_csv('/Users/nothing/Documents/data analyst portfolio/Austin_animal_center/austin_animal_shelter_github/Austin_Animal_Center_Outcomes.csv')

#these are tables made in excel to help catagorize breeds
shelter_locations = pd.read_csv('/Users/nothing/Documents/data analyst portfolio/Austin_animal_center/austin_animal_shelter_github/austin_shelter_locations.csv')
akc_groups = pd.read_csv('/Users/nothing/Documents/data analyst portfolio/Austin_animal_center/austin_animal_shelter_github/dog_info.csv')
bird_groups = pd.read_csv('/Users/nothing/Documents/data analyst portfolio/Austin_animal_center/austin_animal_shelter_github/bird_info.csv')
rabbit_info = pd.read_csv('/Users/nothing/Documents/data analyst portfolio/Austin_animal_center/austin_animal_shelter_github/rabbit info.csv')

#make a copy to ensure raw data is not changed
intake_data = intake_data_raw.copy()
outcome_data = outcome_data_raw.copy()

#make everything snake case
intake_data.columns = intake_data.columns.str.strip().str.lower().str.replace(' ', '_', regex = False)
outcome_data.columns = outcome_data.columns.str.strip().str.lower().str.replace(' ', '_', regex = False)

def col_names(df):
    print('\n\n\n')
    for column in df.columns:
        print(column)

col_names(intake_data)
col_names(outcome_data)

'''

'''

