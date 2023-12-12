import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
from collections import OrderedDict
import numpy as np

aws_access_key_id = "AKIAUMKD4AUURZD7A66G"
aws_secret_access_key = "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"
region_name = "us-east-1"

# S3 bucket and file paths
input_bucket = "dcscuploadfile"
input_object_key = "output.csv"
output_bucket = "dcsctransformdata"

# creating the global mapping for outcome types
outcomes_map = {'Rto-Adopt':1,
                'Adoption':2,
                'Euthanasia':3,
                'Transfer':4,
                'Return to Owner':5,
                'Died':6,
                'Disposal':7,
                'Missing': 8,
                'Relocate':9,
                'N/A':10,
                'Stolen':11}

def download_csv_from_s3(bucket_name, object_key):
    # Create an S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

    try:
        # Download the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(content))
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return None

def upload_df_to_s3(dataframe, bucket_name, object_key):
    # Create an S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

    try:
        # Convert DataFrame to CSV string
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False)

        # Upload the CSV string directly to S3
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=object_key)

        print(f"DataFrame uploaded successfully to {bucket_name}/{object_key}")
    except Exception as e:
        print(f"Error uploading DataFrame to S3: {e}")

def transform_data(data):
    new_data = data.copy()

    new_data = prep_data(new_data)

    dim_animal = prep_animal_dim(new_data)
    dim_dates = prep_date_dim(new_data)
    dim_outcome_types = prep_outcome_types_dim(new_data)

    fct_outcomes = prep_outcomes_fct(new_data)

    # We'll use a dictionary so that we could get simultaneously table name and contents when using to_sql
    # note that fact table can only be updated after dimensions have been updated
    return OrderedDict({'dim_animals': dim_animal,
                        'dim_dates': dim_dates,
                        'dim_outcome_types': dim_outcome_types,
                        'fct_outcomes': fct_outcomes
                        })

def prep_data(data):
    # remove stars from animal names. Need regex=False so that * isn't read as regex
    data['name'] = data['Name'].str.replace("*", "", regex=False)

    # separate the "sex upon outcome" column into property of an animal (male or female)
    # and property of an outcome (was the animal spayed/neutered at the shelter or not)
    data['sex'] = data['Sex upon Outcome'].replace({"Neutered Male": "M",
                                                    "Intact Male": "M",
                                                    "Intact Female": "F",
                                                    "Spayed Female": "F",
                                                    "Unknown": np.nan})

    data['is_fixed'] = data['Sex upon Outcome'].replace({"Neutered Male": True,
                                                         "Intact Male": False,
                                                         "Intact Female": False,
                                                         "Spayed Female": True,
                                                         "Unknown": np.nan})

    # prepare the data table for introducing the date dimension
    # we'll use condensed date as the key, e.g. '20231021'
    # time can be a separate dimension, but here we'll keep it as a field
    data['ts'] = pd.to_datetime(data.DateTime)
    data['date_id'] = data.ts.dt.strftime('%Y%m%d')
    data['time'] = data.ts.dt.time

    # prepare the data table for introducing the outcome type dimension:
    # introduce keys for the outcomes
    data['outcome_type_id'] = data['Outcome Type'].replace(outcomes_map)

    return data

def prep_animal_dim(data):
    # extract columns only relevant to animal dim
    animal_dim = data[['Animal ID', 'name', 'Date of Birth', 'sex', 'Animal Type', 'Breed', 'Color']]

    # rename the columns to agree with the DB tables
    animal_dim.columns = ['animal_id', 'name', 'dob', 'sex', 'animal_type', 'breed', 'color']

    mode_sex = animal_dim['sex'].mode().iloc[0]
    animal_dim = animal_dim.copy()
    animal_dim['sex'] = animal_dim['sex'].fillna(mode_sex)
    animal_dim['animal_id'] = animal_dim['animal_id'].drop_duplicates()

    animal_dim = animal_dim.dropna(subset=['animal_id'])
    # drop duplicate animal records
    return animal_dim.drop_duplicates()

def prep_date_dim(data):
    # use string representation as a key
    # separate out year, month, and day
    dates_dim = pd.DataFrame({
        'date_id': data.ts.dt.strftime('%Y%m%d'),
        'date': data.ts.dt.date,
        'year': data.ts.dt.year,
        'month': data.ts.dt.month,
        'day': data.ts.dt.day,
    })
    return dates_dim.drop_duplicates()

def prep_outcome_types_dim(data):
    # map outcome string values to keys
    outcome_types_dim = pd.DataFrame.from_dict(outcomes_map, orient='index').reset_index()

    # keep only the necessary fields
    outcome_types_dim.columns = ['outcome_type', 'outcome_type_id']
    return outcome_types_dim


def prep_outcomes_fct(data):
    # pick the necessary columns and rename
    outcomes_fct = data[["Animal ID", 'date_id', 'time', 'outcome_type_id', 'Outcome Subtype', 'is_fixed']]
    outcomes_fct = outcomes_fct.drop_duplicates(subset=['Animal ID'])
    outcomes_fct = outcomes_fct.rename(columns={"Animal ID": "animal_id", "Outcome Subtype": "outcome_subtype"})

    return outcomes_fct

def transform_main():
    # Download data from S3
    input_data = download_csv_from_s3(input_bucket, input_object_key)

    if input_data is not None:
        # Transform the data
        transformed_data = transform_data(input_data)

        # Upload the transformed data to S3
        for table_name, table_data in transformed_data.items():
            output_key = f"{table_name}.csv"
            upload_df_to_s3(table_data, output_bucket, output_key)
