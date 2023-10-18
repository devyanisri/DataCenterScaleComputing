import pandas as pd
import sys
import argparse
from sqlalchemy import create_engine,text

def extract_data(input_data):
    df = pd.read_csv(input_data)
    return df


def transform_data(data):
    transform_df = data.copy()
    transform_df[['Month', 'Year']] = transform_df['MonthYear'].str.split(' ', expand=True)
    transform_df[['Name']] = transform_df[['Name']].fillna('Unknown')
    transform_df[['Outcome Subtype']] = transform_df[['Outcome Subtype']].fillna('Not_Available')
    transform_df.drop(['MonthYear','Age upon Outcome'], axis=1, inplace = True)
    
    mapping = {
        'Animal ID': 'animal_id',
        'Name': 'animal_name',
        'DateTime': 'timestmp',
        'Date of Birth': 'dob',
        'Outcome Type': 'outcome_type',
        'Outcome Subtype': 'outcome_subtype',
        'Animal Type': 'animal_type',
        'Breed': 'breed',
        'Color': 'color',
        'Month': 'month_h',
        'Year': 'year_r',
        'Sex upon Outcome': 'sex'
    }

    
    new_columns = [mapping.get(col, col) for col in transform_df.columns]
    transform_df.columns = new_columns

    transform_df[['reprod', 'gender']] = transform_df.sex.str.split(' ', expand=True)
    transform_df.drop(columns = ['sex'], inplace=True)
    return transform_df


def load_data(transform_df):
    db_addr = "postgresql+psycopg2://dev:dev@db:5432/shelter"
    connec = create_engine(db_addr)
    time_df_data = transform_df.copy()
    
    outcome_df_data = transform_df.copy()
    transform_df.to_sql("dummy_table",connec,if_exists="append",index=False)
    
    time_df = time_df_data[['month_h','year_r']].drop_duplicates()
    time_df[['month_h','year_r']].to_sql("timingdim", connec, if_exists="append", index = False)
    transform_df[['animal_id','animal_type','animal_name','dob','breed','color','reprod','gender','timestmp']].to_sql("animaldimension", connec, if_exists="append", index = False)
    
    df = outcome_df_data[['outcome_type','outcome_subtype']].drop_duplicates()
    df[['outcome_type','outcome_subtype']].to_sql("outcomedim", connec, if_exists="append", index = False)
    
    sql_statement = text("""
    INSERT INTO outcomesfact (outcome_dim_key, animal_dim_key, time_dim_key)
    SELECT od.outcome_dim_key, a.animal_dim_key, td.time_dim_key
    FROM dummy_table o
    JOIN outcomedim od ON o.outcome_type = od.outcome_type AND o.outcome_subtype = od.outcome_subtype
    JOIN timingdim td ON o.month_h = td.month_h AND o.year_r = td.year_r
    JOIN animaldim a ON a.animal_id = o.animal_id AND a.animal_type = o.animal_type AND a.timestmp = o.timestmp;
    """)
    with connec.begin() as connection:
        connection.execute(sql_statement)

if __name__ == "__main__":
    input_data = sys.argv[1]
    print("Start processing....")
    data = extract_data()
    transform_df = transform_data(data)
    load_data(transform_df)
    print("Finished processing.....")
