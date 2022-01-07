import pandas as pd
import numpy as np
import math


input_file = '../PD 2022 Wk 1 Input - Input.csv'
df = pd.read_csv(input_file)

# reformat pupil name
df['Pupils Name'] = df['pupil last name'] + ', ' + df['pupil first name']


# get contact email and parent
def get_primary_contact(contact, name1, name2, last):
    if contact == 1:
        first = name1
    else:
        first = name2

    return {'last': last, 'first': first}


def get_primary_contact_name(contact, name1, name2, last):
    primary = get_primary_contact(contact, name1, name2, last)

    return f"{primary['last']}, {primary['first']}"


def get_primary_contact_email(contact, name1, name2, last, employer):
    primary = get_primary_contact(contact, name1, name2, last)

    return f"{primary['first']}.{primary['last']}@{employer}.com"


df['Parental Contact Full Name'] = df.apply(
    lambda x: get_primary_contact_name(
        x['Parental Contact'],
        x['Parental Contact Name_1'],
        x['Parental Contact Name_2'],
        x['pupil last name']
    ), axis=1
)

df['Parental Contact Email Address'] = df.apply(
    lambda x: get_primary_contact_email(
        x['Parental Contact'],
        x['Parental Contact Name_1'],
        x['Parental Contact Name_2'],
        x['pupil last name'],
        x['Preferred Contact Employer']
    ), axis=1
)


# parse year for date of birth
def dob_to_year(dob):
    dob_dt = pd.to_datetime(dob)
    dob_diff = pd.to_datetime('2014-09-01') - dob_dt

    return math.ceil(dob_diff/np.timedelta64(1, 'Y')) + 1


df['Academic Year'] = df['Date of Birth'].apply(dob_to_year)

# reorder columns and drop unnecessary
cols = ['Academic Year', 'Pupils Name', 'Parental Contact Full Name', 'Parental Contact Email Address']
df = df[cols]

# output to csv file
df.to_csv('output.csv', index=False)
