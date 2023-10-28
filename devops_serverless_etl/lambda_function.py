import boto3
import requests
import pandas as pd
import io
import json
import os
from io import BytesIO, StringIO 

# Instantiate our clients
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
sns_client = boto3.client('sns')


def lambda_handler(event, context):
    try:
        print("Downloading file - 1")
        download_file('https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv', 'data.csv')
        print("Downloading file - 2")
        download_file('https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv', 'john_hopkins.csv')
        df = load_data('data/data.csv')
        jh = load_data('data/john_hopkins.csv')
        print("loading complete")
        df = rename_cols(df, {'date':'Date'}) #unify naming method
        print("Cleaning data")
        jh = clean_dataframe(jh, 'US', ['Deaths', 'Country/Region' ,'Province/State',  'Confirmed']) # remove needless columns and non US data
        print("data------------------------ 1")
        df = change_to_datetime(df)
        print("data------------------------ 2")
        jh = change_to_datetime(jh)
        print("data------------------------ 3")
        final = join_dfs(df, jh)
        print("data------------------------ 4")
        s3_bucket_name = 'devops-serverless-etl-poc'
        s3path = "output_data/" 
        
        # Convert DataFrame to CSV data in memory
        csv_buffer = StringIO()
        final.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # Encode the data to bytes
        csv_data_bytes = csv_data.encode('utf-8')

        # Upload the DataFrame to S3
        s3_object_key = s3path + 'output_data.csv'  # Specify the object key (file name) in S3
        s3_client.upload_fileobj(BytesIO(csv_data_bytes), s3_bucket_name, s3_object_key)
        
        print(f'Data uploaded to S3://{s3_bucket_name}/{s3_object_key}')

        # s3_resource.Bucket(bucket_name).put_object(Key=s3path, Body=final)

        #final = jh
        #instantiate_db()  # Create the table if it doesn't exist
        #print("data------------------------ 5")
        #data_sent = send_db(final)
        print("data------------------------ 6")
        #res = post_to_sns('Job ran successfully, Updated {} records in the database'.format(data_sent))
        #print('Job ran successfully, Updated {} records in the database'.format(data_sent))
    except Exception as e:
        #res = post_to_sns('Job failed with error: {}'.format(e))
        print('Job failed with error: {}'.format(e))

def download_file(url, file_name):
    ''' Downloads data from a file and saves it to S3 as an object'''
    body = requests.get(url).content.strip(b'\n')
    bucket_name = 'devops-serverless-etl-poc'
    s3_path = "data/" + file_name
    s3_resource.Bucket(bucket_name).put_object(Key=s3_path, Body=body)

def load_data(key):
    ''' Loads data from S3 into a pandas DataFrame'''
    obj = s3_client.get_object(Bucket='devops-serverless-etl-poc', Key=key)
    csv_file = obj['Body'].read().strip(b'\n')
    df = pd.read_csv(io.BytesIO(csv_file))
    return df

def rename_cols(df, column_mapping):
    """
    Rename columns in a pandas DataFrame based on a provided mapping.

    Args:
    df (pd.DataFrame): The DataFrame to rename columns in.
    column_mapping (dict): A dictionary where keys are the current column names and values are the new column names.

    Returns:
    pd.DataFrame: The DataFrame with renamed columns.
    """
    df.rename(columns=column_mapping, inplace=True)
    return df
  
def clean_dataframe(df, target_country, columns_to_drop):
    """
    Clean a pandas DataFrame by filtering rows for a specific country and dropping specified columns.

    Args:
    df (pd.DataFrame): The DataFrame to clean.
    target_country (str): The name of the country to filter rows.
    columns_to_drop (list): A list of column names to drop from the DataFrame.

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """
    # Filter rows for the target country
    df = df[df['Country/Region'] == target_country]
    
    # Drop specified columns
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
    
    return df

def change_to_datetime(df, columns=None):
    """
    Convert specified columns in a pandas DataFrame to datetime objects.

    Args:
    df (pd.DataFrame): The DataFrame to convert columns to datetime.
    columns (list or str, optional): A list of column names to convert to datetime. If not specified, it will attempt to convert all columns with datetime-like values.

    Returns:
    pd.DataFrame: The DataFrame with specified columns converted to datetime.
    """
    
    if columns is None:
        # If columns are not specified, attempt to convert all columns with datetime-like values
        df = df.apply(pd.to_datetime, errors='ignore', format='%Y-%m-%d')
    else:
        # Convert specified columns to datetime
        if isinstance(columns, str):
            columns = [columns]
        for col in columns:
            df[col] = pd.to_datetime(df[col], errors='ignore', format='%Y-%m-%d')

    return df
   
def join_dfs(df1, df2, on_column='Date', how='inner'):
    """
    Join two pandas DataFrames based on a common column.

    Args:
    df1 (pd.DataFrame): The first DataFrame to join.
    df2 (pd.DataFrame): The second DataFrame to join.
    on_column (str, optional): The column on which to join the DataFrames. Default is 'Date'.
    how (str, optional): The type of join to perform (e.g., 'inner', 'outer', 'left', 'right'). Default is 'inner'.

    Returns:
    pd.DataFrame: The result of joining the two DataFrames.
    """
    final = pd.merge(df1, df2, on=on_column, how=how)
    return final


def post_to_sns(message):
    ''' Sends the output of the job to an SNS topic which fans out
    the result to any number of interested message consumers'''
    response = sns_client.publish(
        TargetArn=os.environ['SNS_TOPIC'],
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
    )
    return response
