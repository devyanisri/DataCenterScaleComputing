# -*- coding: utf-8 -*-
import boto3
import pytz
import pandas as pd
from datetime import datetime
from io import StringIO

MT_Zone = pytz.timezone('US/Mountain')

def upload_df_to_s3(dataframe, bucket_name, object_key, aws_access_key_id, aws_secret_access_key, region='us-east-1'):
    # Create an S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region)

    try:
        # Convert DataFrame to CSV string
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False)

        current_date = datetime.now(MT_Zone).strftime('%Y-%m-%d')
        formatted_object_key = object_key.format(current_date, current_date)
        print("formatted_object_key: ", formatted_object_key)

        # Upload the CSV string directly to S3
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=formatted_object_key)

        print(f"DataFrame uploaded successfully to {bucket_name}/{formatted_object_key}")
    except Exception as e:
        print(f"Error uploading DataFrame: {e}")

def main():
    source_url = "https://data.austintexas.gov/api/views/9t4d-g238/rows.csv"
    bucket_name = "dcscuploadfile"
    object_key = "data/{}/output_{}.csv"
    aws_access_key_id = "AKIAUMKD4AUURZD7A66G"
    aws_secret_access_key = "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"
    region = 'us-east-1'  # Set the appropriate AWS region

    # Use pandas to read CSV data from the source URL into a DataFrame
    df = pd.read_csv(source_url)

    # Upload the DataFrame directly to S3
    upload_df_to_s3(df, bucket_name, object_key, aws_access_key_id, aws_secret_access_key, region)
    