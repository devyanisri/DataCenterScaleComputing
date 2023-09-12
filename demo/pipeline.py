import pandas as pd
import numpy as np
import argparse

def extract_data(source):
    return pd.read_csv(source)

def transform_data(data):
    new_data=data.copy()
    new_data[['month','year']]=new_data.MonthYear.str.split(' ',expand=True)
    new_data.drop(columns=['MonthYear','Sex upon Outcome'],inplace=True)
    return new_data

def load_data(data,target):
    data.to_csv(target)

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('source',help='source csv')
    parser.add_argument('target',help='target csv')
    args=parser.parse_args()

    print('Starting...')
    df=extract_data(args.source)
    new_df=transform_data(df)
    load_data(new_df,args.target)
    print('Complete')
