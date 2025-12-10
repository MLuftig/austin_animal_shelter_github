
'''import area'''
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


'''

define functions

'''
#this function makes a header used in later functions
def header():
    print('\n\n\n')
    print('*' * 30)
    print('\n')

def header2():
    print('\n\n')
    print('-' * 20)
    print('\n')

def import_data(file_name, table_name):
    print(f'\n\nBeginning to load {table_name}:')

    df = None

    try:
        df = pd.read_csv(file_name, low_memory = False)
        print(f'Success, with {len(df)} rows')
        return df
    except FileNotFoundError:
        print(f'ERROR: File not found at path: {file_name}. Returning None.')
        return None
    except Exception as e:
        print(f'ERROR loading from {file_name}: {e}')
        return None
    
def load_raw_tables():
    intake = import_data("Austin_Animal_Center_Intakes.csv", "intake")
    outcome = import_data("Austin_Animal_Center_Outcomes.csv", "outcome")
    return intake, outcome
    
def create_intake_table(df) -> pd.DataFrame:
    header()
    print(f'Beginning to create intake table')
    
    if df is None:
        print(f'intake data could not be loaded. Exiting table creation.')
        return None
    return ( 
        df
        .pipe(imported_data_clean, 'intake_data_raw')
        .pipe(datetime_y_lineid, 'intake_data')
        .pipe(narrow_to_intake_columns)
        .pipe(datetime_extraction, 'intake_table')
        .pipe(intake_condition_clean, 'intake_table')
        .pipe(table_check, 'intake_table')
    )

def create_outtake_table(df) -> pd.DataFrame:
    header()
    print(f'Beginning to create outcome table')
    
    if df is None:
        print(f'outcome data could not be loaded. Exiting table creation.')
        return None
    return (
        df
        .pipe(imported_data_clean, 'outcome_data_raw')
        .pipe(datetime_y_lineid, 'outcome_data')
        .pipe(narrow_to_outcome_columns)
        .pipe(datetime_extraction, 'outcome_table')
        .pipe(clean_outcome_type, 'outcome_table')
        .pipe(clean_outcome_subtype, 'outcome_table')
        .pipe(table_check, 'outcome_table')
    )
   
def create_animal_table(animal_in, animal_out) -> pd.DataFrame:
    header()
    data_sources = {
        "animal_in": animal_in,
        "animal_out": animal_out
    }

    animals = {}
    for name, df in data_sources.items():

        df = (df
                .filter(regex = r'^(animal_id|name|animal_type|sex.*|age.*|breed|color)$').copy()
                .drop_duplicates(subset = 'animal_id')
                .pipe(clean_name)
                .pipe(clean_age)
                .pipe(lifecycle)
                .pipe(clean_sex)
                .pipe(clean_breed)
                .pipe(clean_spp)
                .pipe(akc_groups)
                .pipe(hair_length)
                .pipe(clean_color)
            )
       
        animals[name] = df
    animal_in_clean = animals.get('animal_in')
    animal_out_clean = animals.get('animal_out')

    ''' 
    print(f'animal_in: \n{animal_in_clean.head()}')
    print(f'animal_out: \n{animal_out_clean.head()}')'''
    
    merged_df = pd.merge(
    animal_in_clean, animal_out_clean,
    how = 'outer',
    on = 'animal_id',
    suffixes = ('_intake', '_outcome')
    )

    print('\n\n\nMerged animal table preview:')    
    print(merged_df.head())
    merged_df['animal_id'].value_counts()[merged_df['animal_id'].value_counts() > 1]
    print('\n\n\n')
    print(merged_df.columns.unique())

    count_dups = merged_df['animal_id'].duplicated().sum()
    print(f'\nThere are {count_dups} duplicated animal_id values in the merged animal table.')
    animal_table = merged_df[['animal_id', 'cln_name_outcome', 'cln_spp_outcome', 'primary_breed_intake', 'secondary_breed_intake', 'akc_group_outcome', 'hair_length_outcome', 
                              'cln_color_intake', 'altered_outcome', 'cln_sex_outcome', 'age_yr_outcome', 'lifecycle_stage_outcome']].copy()
    print('\n\n\nFinal animal table preview:')
    print(animal_table.head())
    print('*' * 30)
    
    return animal_table

def create_los_table(df_in, df_out) -> pd.DataFrame:
    # rename columns for clarity
    animal_in = df_in[['animal_id', 'datetime']].copy()
    animal_out = df_out[['animal_id', 'datetime']].copy()

    animal_in = animal_in.rename(columns={"datetime": "datetime_intake"})
    animal_out = animal_out.rename(columns={"datetime": "datetime_outcome"})

    # merge all records (exploded)
    merged = animal_in.merge(animal_out, on="animal_id", how="left")

    # keep only outcomes after intakes
    merged = merged[merged["datetime_outcome"] >= merged["datetime_intake"]]

    # take the earliest possible outcome
    merged = merged.sort_values(["animal_id", "datetime_intake", "datetime_outcome"])
    los = merged.groupby(["animal_id", "datetime_intake"]).first().reset_index()

    # calculate LOS
    los["length_of_stay_days"] = (los["datetime_outcome"] - los["datetime_intake"]).dt.days
    print('\n\n\nLength of Stay Table Preview:')
    print(los.head())

    return los

def imported_data_clean(df, df_name) -> pd.DataFrame: 
    header()
    try:
        assert df is not None, f'DataFrame {df_name} is None. Cannot clean.'
        print(f'Beginning to clean {df_name}')
    
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex = False) 
        df = df[~df.duplicated(subset=['animal_id', 'datetime'])].copy() 
        print('...duplicates dropped and column names snake case') 
        error_count = 0

        for column in df.columns:
            if df[column].dtype == 'object':
                try:
                    df[column] = (
                        df[column].astype(str) .str.strip().str.lower())
                except AttributeError:
                    print(f'Skipping non-string/mixed column: {column}')
                    error_count += 1
        if error_count == 0:
            print('...columns lowercase and stripped')
        else:
            print(f'{error_count} columns not made into lowercase and stripped')

        print('...complete')
        return df 
    except AssertionError as e:
        print(e)
        return None
    
def datetime_y_lineid(df, df_name) -> pd.DataFrame:
    header()
    try:
        assert df is not None, f'DataFrame {df_name} is None. Cannot process datetime and line_id.'
        print(f'Beginning to clean datetime for {df_name}')
        
        if df['datetime'].astype(str).str.contains(r'AM|PM', case = False, regex = True).any():
            df['datetime'] = pd.to_datetime(df['datetime'], format = ('%m/%d/%Y %I:%M:%S %p'), errors = 'coerce') 
            #intake_data['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            df['datetime'] = pd.to_datetime(df['datetime'], format = '%Y-%m-%d %H:%M:%S', errors = 'coerce')

        df['std_date_time'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['line_id'] = df['animal_id'].astype(str) + '_' + df['std_date_time'].astype(str)
        
        print('...complete')
        
        return df
    except AssertionError as e:
        print(e)
        return None
    
def narrow_to_intake_columns(df) -> pd.DataFrame:
    header()
    print('Beginning to narrow to intake columns')
    df = df[['line_id', 'animal_id', 'datetime', 'intake_condition']].copy()
    print('...complete')
    return df

def datetime_extraction(df, df_name) -> pd.DataFrame:
    header()
    print(f'Beginning to extract from datetime in {df_name}')
    #time.sleep(2)
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
    df['weekday'] = df['weekday'].str.lower()
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
    
    print('...complete')
    return df

def check_id_length(df, df_name) -> pd.DataFrame:
    header()
    print(f'Beginning to check the length of animal_id in {df_name}...')
    length_series = df['animal_id'].str.len()
    num_animal_id_dif = (length_series != 7).sum()
    print(f'\n\n\nthere are {num_animal_id_dif} lines with a length different than 7')
    return df

def intake_condition_clean(df, df_name) -> pd.DataFrame:
    header()
    print(f'\n\n\nunique conditions in {df_name}: {df['intake_condition'].unique()}')
    print(f'\n\n\nnull values in {df_name}: {df['intake_condition'].isna().sum()}')
    print(f'\n\n\nBeginning to clean up the condition list and condense to 3 catagorical values')
    #time.sleep(2)

    medical_conditions = ['sick', 'injured', 'medical', 'aged']
    behavior_conditions = ['feral', 'behavior']
    reproductive_conditions = ['pregnant', 'nursing']
    routine_conditions = ['normal']

    df['pregnant_o_nursing'] = np.where(df['intake_condition'].isin(reproductive_conditions), True, False)

    conditions = [
        df['intake_condition'].isin(medical_conditions) | df['intake_condition'].isin(reproductive_conditions),
        df['intake_condition'].isin(behavior_conditions),
        df['intake_condition'].isin(routine_conditions)
    ]

    choices = [
        'medical' ,
        'behavior',
        'routine'
    ]

    df['intake_reason'] = np.select(conditions, choices, default = 'Unknown')
    print('...complete')
    return df

def clean_outcome_type(df, df_name) -> pd.DataFrame:
    print(f'Beginning to clean {df_name}')
    
    unknown_outcome = ['nan', 'missing']
    alive_outcome = ['rto-adopt', 'adoption', 'return to owner']
    administrative_outcome = ['transfer', 'relocate']
    deceased_outcome = ['euthanasia', 'died', 'disposal']
    
    conditions = [
        df['outcome_type'].isin(unknown_outcome),
        df['outcome_type'].isin(alive_outcome),
        df['outcome_type'].isin(administrative_outcome),
        df['outcome_type'].isin(deceased_outcome)
    ]

    choices = [
        'unknown',
        'alive',
        'admin',
        'deceased'
    ]

    df['outcome_category'] = np.select(conditions, choices, default = 'unknown')
    print('...complete')
    return df

def narrow_to_outcome_columns(df) -> pd.DataFrame:
    header()
    print('Beginning to narrow to outcome columns')
    df = df[['line_id', 'animal_id', 'datetime', 'outcome_type', 'outcome_subtype']].copy()
    print('...complete')
    return df

def clean_outcome_subtype(df, df_name) -> pd.DataFrame:
    header()
    print(f'Beginning to clean {df_name}')

    location_subtype = ['in kennel', 'offsite', 'at vet', 'barn', 'enroute', 'in surgery'] #the animal's specific physical location or temporary status
    behavior_subtype = ['suffering', 'medical', 'aggressive', 'rabies risk', 'behavior'] #indicates the reason for a specific outcome (often euthanasia or specialized treatment)
    program_subtype = ['partner', 'underage', 'foster', 'in foster', 'snr', 'scrp', 'prc'] #subtypes related to specific shelter programs or transfer partners
    admin_subtype = ['field', 'possible theft', 'customer s', 'court/investigation', 'emer'] #Miscellaneous or administrative details
    unknown_subtype = ['nan']

    conditions = [
        df['outcome_subtype'].isin(location_subtype),
        df['outcome_subtype'].isin(behavior_subtype),
        df['outcome_subtype'].isin(program_subtype),
        df['outcome_subtype'].isin(admin_subtype),
        df['outcome_subtype'].isin(unknown_subtype)
    ]

    choices = [
        'location' ,
        'behavior',
        'program',
        'admin',
        'unknown'
    ]

    df['outcome_subcategory'] = np.select(conditions, choices, default = 'unknown')
    print('...complete')
    return df

def table_check(df, df_name) -> pd.DataFrame:
    """
    Checks and prints the count of null (NaN) values for every column in a Pandas DataFrame.
    """
    header()
    print(f'Beginning errorcheck for: {df_name} ***')
    print(f'\n--- Null Value Check ---')
    for column in df.columns: 
        null_count = df[column].isna().sum() 
        print(f'{column} nulls: {null_count}')
    print('\n')
    
    print(f'--- Integrity Check ---')
    if 'line_id' in df.columns:
        duplicate_count = df['line_id'].duplicated().sum()
        print(f'Total line_id duplicates: {duplicate_count}')
    else:
        print('WARNING: "line_id" not found for duplication check.')
    print('\n')
    print(f'\n--- Data Type Audit (DF.dtypes) ---')
    print(df.dtypes)
    
    print(f'\n--- Numerical Sanity Check (DF.describe) ---')
    
    numeric_cols = df.select_dtypes(include = ['int64', 'float64', 'Int64', 'int32']).columns
    if not numeric_cols.empty:
        print(df[numeric_cols].describe())
    else:
        print('No standard numerical columns found for description.')

    expected_seasons = ['summer', 'winter', 'spring', 'autumn']
    season_check = (~df['season'].isin(expected_seasons)).sum()
    print(f'\nthere are {season_check} rows with unexpected seasons')

    expected_weekdays = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday' , 'friday', 'saturday']
    day_check = (~df['weekday'].isin(expected_weekdays)).sum()
    print(f'there are {day_check} rows with unexpected days')

    print(f'\n--- Categorical Value Audit (Top 10 Counts) ---')
    object_cols = df.select_dtypes(include=['object']).columns

    for column in object_cols:
        print(f"\n{column.upper()}:")
        print(df[column].value_counts().nlargest(10))
    
    print('\n')
    print('*' * 30)
    return df

def clean_name(df) -> pd.DataFrame:
    header2()
    try:
        assert 'name' in df.columns, 'name column not found in DataFrame. Cannot clean names.'
        print('Beginning to clean name column')
        df['name_given_at_intake'] = (df['name'].astype(str).str.startswith('*'))
        df['cln_name'] = df['name'].str.lstrip('*') 
        df['cln_name'] = df['cln_name'].astype(str)
        df['cln_name'] = df['cln_name'].replace(['nan', ''], np.nan)
        df['cln_name'] = df['cln_name'].fillna(df['animal_id'].astype(str))
        print('removing original name column')
        df = df.drop(columns = 'name', axis = 1)
        print('...complete')
        return df
    except AssertionError as e:
        print(f'Error: {e}')
        return df

def clean_age(df) -> pd.DataFrame:
    header2()
    df.columns = df.columns.str.replace(r'^age.*$', 'age', regex = True)
    try:
        assert 'age' in df.columns, 'age column not found in DataFrame. Cannot clean age.'
        print('Beginning to clean age column')
        
    
        df['cln_age'] = df['age'].astype(str)
        df['age_num'] = df['cln_age'].str.split(' ', n = 1, expand = True)[0]
        df['age_num'] = df['age_num'].replace(['nan', ''], np.nan)
        #print(df['age'].isna().sum())
        df['cln_age'] = df['cln_age'].replace(['nan', ''], np.nan)
        #print(df[df['age'] == 'nan'].head())
        df['age_num'] = pd.to_numeric(df['age_num'])
        df['age_num'] = df['age_num'].where(df['age_num'] > 0, np.nan)
        
        conditions = [
            (df['cln_age'].str.contains(r'months?', case = False, na = False, regex = True)), 
            (df['cln_age'].str.contains(r'years?', case = False, na = False, regex = True)),
            (df['cln_age'].str.contains(r'weeks?', case = False, na = False, regex = True)),
            (df['cln_age'].str.contains(r'days?', case = False, na = False, regex = True))
        ]

        choices = [
            (df['age_num'] / 12), #converts the age in months to years
            (df['age_num']), #keeps the same age as it is in years
            (df['age_num'] / 52.1786), #converts from weeks to years
            (df['age_num'] / 365.25) #converts frem days to years
        ]

        df['age_yr'] = np.select(conditions, choices, default = np.nan)
        print('...complete')
        print('removing original age column')
        df = df.drop(columns = ['age', 'age_num'], axis = 1)
        return df
    except AssertionError as e:
        print(f'Error: {e}')
        return df

def lifecycle(df) -> pd.DataFrame:
    header2()
    try:
        assert 'age_yr' in df.columns, 'age_yr column not found in DataFrame. Cannot classify lifecycle.'
        print('Beginning to classify lifecycle stages')
        
        conditions = [
            (df['age_yr'] < 1), # less than 1 year
            (df['age_yr'] >= 1) & (df['age_yr'] < 3), # 1 to less than 3 years
            (df['age_yr'] >= 3) & (df['age_yr'] < 7), # 3 to less than 7 years
            (df['age_yr'] >= 7) # 7 years and older
        ]

        choices = [
            'juvenile',
            'young adult',
            'adult',
            'senior'
        ]

        df['lifecycle_stage'] = np.select(conditions, choices, default = 'unknown')
        print('...complete')
        return df
    except AssertionError as e:
        print(f'Error: {e}')
        return df
    
def clean_sex(df) -> pd.DataFrame:
    
    header2()
    df.columns = df.columns.str.replace(r'^sex.*$', 'sex', regex = True)
    try:
        assert 'sex' in df.columns, '\'sex\' column not found in DataFrame. Cannot clean sex.'
        print('Beginning to clean sex column')
        

       


        df['altered'] = (df['sex'].astype(str).str.startswith('n') | (df['sex'].astype(str).str.startswith('s')))
        df['cln_sex'] = df['sex'].astype(str)
        
        df['altered'] = (df['sex'].astype(str).str.startswith('n') | (df['sex'].astype(str).str.startswith('s')))

        df = df.fillna('unknown')
        condition = [
            df['sex'].str.contains('fe'),
            df['sex'].str.contains('male')
        ]

        choice = [
            'female',
            'male'
        ]

        df['cln_sex'] = np.select(condition, choice, default = 'unknown')
        print('...complete')
        print('removing original sex column')
        #print(df['cln_sex'].head())
        df = df.drop(columns = 'sex', axis = 1) 
        return df
    except AssertionError as e:
        print(f'Error: {e}')
        return df

def clean_breed(df) -> pd.DataFrame:
    header2()
    try:
        assert 'breed' in df.columns, 'breed column not found in DataFrame. Cannot clean breed.' 

        print('Beginning to clean breed column')
        df['primary_breed'] = df['breed'].astype(str).str.split(' mix', n = 1).str[0].str.strip().str.lower()
        df['secondary_breed'] = None  # Default to None

        with_slash = df['breed'].astype(str).str.contains('/')
        # Split at first '/' for mixed breeds
        df.loc[with_slash, 'primary_breed'] = df.loc[with_slash, 'breed'].astype(str).str.split('/', n=1).str[0].str.strip().str.lower()
        df.loc[with_slash, 'secondary_breed'] = df.loc[with_slash, 'breed'].astype(str).str.split('/', n=1).str[1].str.strip().str.lower()
        df = df.drop(columns = 'breed', axis = 1)

        print('...complete')
        print('removing original breed column') 
        if 'breed' in df.columns:
            df = df.drop(columns = 'breed', axis = 1)
        return df
    except AssertionError as e:
        print(f'Error: {e}')
        return df

def clean_spp(df) -> pd.DataFrame:
    header2()
    try:
        assert 'animal_type' in df.columns, 'animal_type column not found in DataFrame. Cannot clean species.'
        print('Beginning to clean species column')
        df = df.rename(columns = {'animal_type' : 'spp'})
        df['cln_spp'] = df['spp'].astype(str).str.strip().str.lower()
        #print(df['spp'].value_counts())
        other = df['cln_spp'] == 'other'
        #print(df.loc[other]['primary_breed'].unique())

        rabbit_list = [
            'polish', 'rabbit sh', 'ringtail', 'californian', 'lionhead', 'dutch', 
            'angora-french', 'lop-holland', 'angora-satin', 'rex', 'rhinelander', 
            'havana', 'new zealand wht', 'netherlnd dwarf', 'lop-english', 
            'english spot', 'rabbit lh', 'cinnamon', 'american', 'hotot', 
            'lop-amer fuzzy', 'lop-mini', 'checkered giant', 'american sable', 
            'flemish giant', 'harlequin', 'chinchilla-stnd', 'rex-mini', 
            'jersey wooly', 'silver', 'cottontail', 'britannia petit', 'beveren', 
            'dwarf hotot', 'himalayan', 'angora-english', 'belgian hare'
        ]
        rodent_small_pet_list = [
            'guinea pig', 'ferret', 'chinchilla', 'hamster', 'rat', 'mouse', 
            'hedgehog', 'gerbil', 'sugar glider', 'prairie dog', 'chinchilla-amer'
        ]
        reptile_amphibian_list = [
            'snake', 'lizard', 'tortoise', 'turtle', 'frog'
        ]
        arthropod_aquatic_list = [
            'tarantula', 'hermit crab', 'cold water', 'tropical'
        ]
        wildlife_list  = [
            'raccoon', 'opossum', 'bat', 'fox', 'squirrel', 'skunk', 'armadillo', 
            'coyote', 'otter', 'deer', 'bobcat'
        ]

        conditions = [
            df.loc[other, 'primary_breed'].isin(rabbit_list),
            df.loc[other, 'primary_breed'].isin(rodent_small_pet_list),
            df.loc[other, 'primary_breed'].isin(reptile_amphibian_list),
            df.loc[other, 'primary_breed'].isin(arthropod_aquatic_list),
            df.loc[other, 'primary_breed'].isin(wildlife_list)
        ]
        choices = [
            'rabbit',
            'rodent_small_pet',
            'reptile_amphibian',
            'arthropod_aquatic',
            'wildlife'
        ]
        df.loc[other, 'cln_spp'] = np.select(conditions, choices, default = 'unknown')

        #print(df.head(50))
        #still_there = df['spp'] == 'unknown'
        #print(f'listed as unknown: {df.loc[still_there]}')
        #df = rabbit(df)


        print('\n\n\nBreeds after cleaning:')
        print(df['cln_spp'].unique())
        print('...complete')
        print('removing original species column')
        df = df.drop(columns = 'spp', axis = 1) 
        return df
    except AssertionError as e:
        print(f'Error: {e}')
        return df

def akc_groups(df) -> pd.DataFrame:
    header()
    print('Beginning to identify dog breeds by AKC group')
    df['akc_group'] = 'unknown'
    dogs = df['cln_spp'].str.contains('dog')
    #print(f'unique dog breeds: \n {df.loc[dogs, "primary_breed"].unique()}')

    toy = ('affenpinscher', 'cavalier span', 'chihuahua longhair', 'chihuahua shorthair', 'chinese crested', 'entlebucher', 'havanese', 'italian greyhound', 'jack russell terrier', 'japanese chin', 'maltese', 
           'manchester terrier', 'miniature pinscher', 'miniature poodle', 'papillon', 'pekingese', 'pomeranian', 'pug', 'shih tzu', 'silky terrier', 'toy fox terrier', 'toy poodle', 'yorkshire terrier')
    hound = ('afghan hound' , 'american eskimo', 'american foxhound', 'basenji', 'basset hound', 'beagle', 'black mouth cur', 'bloodhound', 'blue lacy', 'bluetick hound', 'dachshund', 'dachshund longhair', 
             'dachshund stan', 'dachshund wirehair', 'english foxhound', 'english pointer', 'greyhound', 'harrier', 'ibizan hound', 'irish wolfhound', 'norwegian elkhound', 'otterhound', 'pharaoh hound', 
             'picardy sheepdog', 'pit bull', 'plott hound', 'podengo pequeno', 'redbone hound', 'rhod ridgeback', 'saluki', 'treeing walker coonhound', 'whippet')
    terrier = ('airedale terrier', 'akbash', 'american staffordshire terrier', 'australian terrier', 'border terrier', 'bull terrier', 'bull terrier miniature', 'cairn terrier', 'chesa bay retr', 
               'irish terrier', 'lakeland terrier', 'miniature schnauzer', 'norfolk terrier', 'norwich terrier', 'parson russell terrier', 'patterdale terr', 'pbgv', 'rat terrier', 'scottish terrier', 
               'sealyham terr', 'skye terrier', 'smooth fox terrier', 'soft coated wheaten terrier', 'standard poodle', 'welsh terrier', 'west highland', 'wire hair fox terrier')
    working = ('akita', 'alaskan husky', 'alaskan klee kai', 'alaskan malamute', 'bernese mountain dog', 'boerboel', 'boxer', 'boykin span', 'bullmastiff', 'cane corso', 'dogo argentino', 'dogue de bordeaux', 
               'german pinscher', 'german shepherd', 'glen of imaal', 'great dane', 'great pyrenees', 'greater swiss mountain dog', 'kuvasz', 'leonberger', 'mastiff', 'mexican hairless', 'neapolitan mastiff', 
               'newfoundland', 'presa canario', 'rottweiler', 'samoyed', 'siberian husky', 'standard schnauzer', 'sussex span', 'tibetan mastiff')
    foundation = ('american bulldog', 'american pit bull terrier', 'australian kelpie', 'bruss griffon', 'carolina dog', 'catahoula', 'doberman pinsch', 'dutch sheepdog', 'feist', 'hovawart', 'jindo', 'kangal', 
                  'port water dog', 'spanish mastiff', 'staffordshire', 'treeing cur', 'treeing tennesse brindle')
    sporting = ('anatol shepherd', 'brittany', 'clumber spaniel', 'cocker spaniel', 'english cocker spaniel', 'english coonhound', 'english setter', 'english shepherd', 'english springer spaniel', 
                'field spaniel', 'german wirehaired pointer', 'golden retriever', 'gordon setter', 'grand basset griffon vendeen', 'irish setter', 'labrador retriever', 'nova scotia duck tolling retriever', 
                'old english bulldog', 'pointer', 'spinone italiano', 'st. bernard rough coat', 'st. bernard smooth coat', 'vizsla', 'weimaraner', 'welsh springer spaniel', 'wirehaired pointing griffon', 
                'wolf hybrid')
    herding = ('australian cattle dog', 'australian shepherd', 'bearded collie', 'beauceron', 'bedlington terr', 'belgian malinois', 'belgian sheepdog', 'belgian tervuren', 'border collie', 'briard', 
               'canaan dog', 'cardigan welsh corgi', 'collie rough', 'collie smooth', 'german shorthair pointer', 'old english sheepdog', 'pembroke welsh corgi', 'queensland heeler', 'shetland sheepdog', 
               'spanish water dog', 'swedish vallhund', 'swiss hound')
    non_sporting = ('bichon frise', 'boston terrier', 'bouv flandres', 'bulldog', 'chinese sharpei', 'chow chow', 'coton de tulear', 'dalmatian', 'dandie dinmont', 'finnish spitz', 'flat coat retriever', 
                    'french bulldog', 'keeshond', 'lhasa apso', 'lowchen', 'schipperke', 'schnauzer giant', 'shiba inu', 'tibetan spaniel', 'tibetan terrier')
    conditions = [
        df.loc[dogs, 'primary_breed'].isin(toy),
        df.loc[dogs, 'primary_breed'].isin(hound),
        df.loc[dogs, 'primary_breed'].isin(terrier),
        df.loc[dogs, 'primary_breed'].isin(working),
        df.loc[dogs, 'primary_breed'].isin(foundation),
        df.loc[dogs, 'primary_breed'].isin(sporting),
        df.loc[dogs, 'primary_breed'].isin(herding),
        df.loc[dogs, 'primary_breed'].isin(non_sporting)
    ]           

    choices = [
        'toy',
        'hound',
        'terrier',
        'working',
        'foundation',
        'sporting',
        'herding',
        'non_sporting'
    ]

    df.loc[dogs, 'akc_group'] = np.select(conditions, choices, default = 'unknown')
    print('...complete')
    return df
  
def hair_length(df) -> pd.DataFrame:
    header()

    df['hair_length'] = 'unknown'
    short_hair = df['primary_breed'].str.contains(
        r'\b(short|shorthair|short-hair|sh hair|sd\b|s hair)\b'
        , case = False, regex = True)
    medium_hair = df['primary_breed'].str.contains(
    r'\b(medium|med hair|medium hair|md hair|m hair|mh)\b',
    case=False, regex=True
)
    long_hair = df['primary_breed'].str.contains(
    r'\b(long|longhair|long-hair|lg hair|l hair|lh)\b',
    case=False, regex=True
)
    print(f'\n\n\nshort hair:\n{df.loc[short_hair]}')
    print(f'\n\n\nmedium hair:\n{df.loc[medium_hair]}')
    print(f'\n\n\nlong hair:\n{df.loc[long_hair]}')

    order = [short_hair, medium_hair, long_hair]

    conditions = [
        short_hair,
        medium_hair,
        long_hair
    ]
    choices = [
        'short',
        'medium',
        'long'
    ]
    df['hair_length'] = np.select(conditions, choices, default = 'unknown')

    '''for length in order:
        #df.loc[length, 'primary_breed'] = df.loc[length, 'primary_breed'].str.replace(r' short| sh$| medium| md$|mdm$| long| lg$| lh$| lh$', '', case = False, regex = True).str.strip()
        if length.equals(short_hair):
            df.loc[length, 'hair_length'] = 'short'
        elif length.equals(medium_hair):   
            df.loc[length, 'hair_length'] = 'medium'
        elif length.equals(long_hair):
            df.loc[length, 'hair_length'] = 'long'  '''
    return df


    #print(df['primary_breed'].unique())
    
def clean_color(df) -> pd.DataFrame:
    header2()
    try:
        assert 'color' in df.columns, 'color column not found in DataFrame. Cannot clean color.' 
        #df['cln_color'] = df['color'].copy()
        print('Beginning to clean color column')
        df['cln_color'] = df['color'].astype(str).str.split('/', n = 1).str[0].str.strip().str.lower()
        df['secondary_color'] = df['color'].astype(str).str.split('/', n = 1).str[1].str.strip().str.lower()
        df.loc[df['cln_color'] == 'pink', 'cln_color'] = 'unknown'
        print('...condensing patterned colors')
        df = patterned(df)
        #print(f'current colors: {df["cln_color"].unique()}')
        print('...complete')
        print('removing original color column')
        df = df.drop(columns = 'color', axis = 1)
        return df
    except AssertionError as e:
        print(f'Error: {e}')
        return df 

def patterned(df) -> pd.DataFrame:
    pattern_list = [
     'tabby', 'tiger', 'calico', 'tortie', 'torbie', 'brindle', 'tricolor', 'tri-color', 'tri color', 'tick', 'merle', 'point', 'lynx'
    ]

    pattern_regex = '|'.join(pattern_list)

    is_patterned = df['cln_color'].str.contains(pattern_regex, case=False, regex=True)
    df.loc[is_patterned, 'cln_color'] = 'patterned'
    return df
       
def export_tables(intake_df, outcome_df, animal_df, los_df) -> None:
    header()
    print('Beginning to export tables to CSV files')
    intake_df.to_csv('intake_table.csv', index = False)
    outcome_df.to_csv('outcome_table.csv', index = False)
    animal_df.to_csv('animal_table.csv', index = False)
    los_df.to_csv('length_of_stay_table.csv', index = False)
    print('...export complete') 
'''
Body

'''

intake_raw, outcome_raw = load_raw_tables()
intake = create_intake_table(intake_raw)
outcome = create_outtake_table(outcome_raw)
animal = create_animal_table(intake_raw, outcome_raw)
los_table = create_los_table(intake, outcome)

header()
header2()
header()


print('\n\n\n CURRENT INTAKE TABLE: ')
print(intake.head(25))
print('\n\n\n CURRENT OUTCOME TABLE: ')
print(outcome.head(25))
print('\n\n\n CURRENT ANIMAL TABLE: ')
print(animal.head(25))
print('\n\n\n CURRENT LENGTH OF STAY TABLE: ')
print(los_table.head(25))

export_tables(intake, outcome, animal, los_table)