import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

# AWS profile to use
profile_name = 'jkumsi'

# S3 bucket and folder details
bucket_name = 'tini-d-gluebucket-001'
folder_path = 's3://tini-d-gluebucket-001/source-csv/emp_folder/'

# Columns for the CSV file
columns = ['emp_id', 'emp_name', 'salary', 'dept_id', 'join_date', 'timestamp']

# Function to add rows to CSV file in S3
def add_rows_to_csv(s3_client, bucket, key, data):
    # Fetch existing CSV data from S3
    existing_data = s3_client.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
    
    # Convert existing CSV data to DataFrame
    existing_df = pd.read_csv(StringIO(existing_data)) if existing_data else pd.DataFrame(columns=columns)
    
    # Convert new data to DataFrame
    new_df = pd.DataFrame(data, columns=columns)
    
    # Concatenate existing and new DataFrames
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    # Write combined DataFrame back to CSV
    new_csv_data = combined_df.to_csv(index=False)
    
    # Upload the updated CSV file to S3
    s3_client.put_object(Body=new_csv_data, Bucket=bucket, Key=key)

# Connect to S3 using the specified profile
session = boto3.Session(profile_name=profile_name)
s3_client = session.client('s3')

# Example data to add to the CSV file
new_rows = [
    {'emp_id': 101, 'emp_name': 'John Doe', 'salary': 60000, 'dept_id': 1, 'join_date': '2022-01-19', 'timestamp': str(datetime.now())},
    {'emp_id': 102, 'emp_name': 'Jane Smith', 'salary': 70000, 'dept_id': 2, 'join_date': '2022-01-20', 'timestamp': str(datetime.now())}
]

# Specify the CSV file key within the S3 folder
csv_file_key = f'{folder_path}/emp.csv'

# Add rows to the CSV file in S3
add_rows_to_csv(s3_client, bucket_name, csv_file_key, new_rows)

print(f"Rows added to the CSV file in S3: s3://{bucket_name}/{csv_file_key}")