<<<<<<< HEAD
import boto3
import urllib.parse
import os
import copy
import sys

# Configure database / table name and emp_id, file_id from workflow params?
DATABASE_NAME = 'glue_db'
TABLE_NAME = 'emp_hr_data'
dept_id = '1'

# # Initialise the Glue client using Boto 3
glue_client = boto3.client('glue')

#get current table schema for the given database name & table name
def get_current_schema(database_name, table_name):
    try:
        response = glue_client.get_table(
            DatabaseName=DATABASE_NAME,
            Name=TABLE_NAME
        )
    except Exception as error:
        print("Exception while fetching table info")
        sys.exit(-1)
    
    # Parsing table info required to create partitions from table
    table_data = {}
    table_data['input_format'] = response['Table']['StorageDescriptor']['InputFormat']
    table_data['output_format'] = response['Table']['StorageDescriptor']['OutputFormat']
    table_data['table_location'] = response['Table']['StorageDescriptor']['Location']
    table_data['serde_info'] = response['Table']['StorageDescriptor']['SerdeInfo']
    table_data['partition_keys'] = response['Table']['PartitionKeys']
    
    return table_data

#prepare partition input list using table_data
def generate_partition_input_list(table_data):
    input_list = []  # Initializing empty list
    partition_keys =  [{'Name': 'dept_id', 'Type': 'bigint'}]
    partition_values = [str(dept_id)]   
    
    # Ensure the number of partition keys matches the number of partition values
    if len(partition_keys) != len(partition_values):
        print("Error: Number of partition keys does not match the number of partition values")
        return None
    
    part_location = table_data['s3://tini-d-gluebucket-001/incoming_data/hr-data/']  # Base location
    
    for i, key in enumerate(partition_keys):
        part_location += f"{key['Name']}={partition_values[i]}/"
    
    input_dict = {
        'Values': partition_values,
        'StorageDescriptor': {
            'Location': part_location,
            'InputFormat': table_data['input_format'],
            'OutputFormat': table_data['output_format'],
            'SerdeInfo': table_data['serde_info']
        }
    }
    input_list.append(input_dict.copy())
    return input_list

#create partition dynamically using the partition input list
table_data = get_current_schema(DATABASE_NAME, TABLE_NAME)
input_list = generate_partition_input_list(dept_id)
try:
    create_partition_response = glue_client.batch_create_partition(
            DatabaseName=DATABASE_NAME,
            TableName=TABLE_NAME,
            PartitionInputList=input_list
        )
    print('Glue partition created successfully.') 
    print(create_partition_response)
except Exception as e:
            # Handle exception as per your business requirements
            print(e)
=======
import os
# Set the AWS profile using an environment variable
os.environ['AWS_PROFILE'] = 'jkumsi'
import boto3
import botocore.exceptions
import logging
import time


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Specify the AWS CLI profile to use
aws_profile = 'jkumsi'

# Create an AWSLogs client
#cloudwatch_logs = boto3.client('aws-glue/crawlers', region_name='us-east-1', profile_name=aws_profile)
cloudwatch_logs = boto3.client('logs', region_name='us-east-1')

# Specify your existing log group and log stream
log_group_name = '/aws-glue/crawlers'
log_stream_name = 'emp_data_crawler'

# Use the specified AWS CLI profile
session = boto3.Session(profile_name=aws_profile)

client = session.client('glue', region_name='us-east-1')
#client = session.client('awsglue.context', region_name='us-east-1')

crawler_name = 'emp_data_crawler'
role_name = 'tini-d-glue-crawler-role-001'
database_name = 'glue_db'
s3_input_path = 's3://tini-d-gluebucket-001/incoming_data/hr-data/'
s3_output_path = 's3://tini-d-gluebucket-001/output_data/'
log_messages = 'Calling Targets'

# Additional parameters for custom partitioning
partition_columns = ['year', 'month', 'day']
table_name = 'hr_data'

# Define Glue Table with StorageDescriptor
table_input = {
    'Name': table_name,
    'DatabaseName': database_name,
    'StorageDescriptor': {
        'Columns': [],
        'Location': s3_output_path,
        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
            'Parameters': {
                'serialization.format': '1'
            }
        },
        'BucketColumns': [],
        'SortColumns': [],
        'Parameters': {
            'classification': 'parquet'
        },
        'SkewedInfo': {
            'SkewedColumnNames': [],
            'SkewedColumnValues': [],
            'SkewedColumnValueLocationMaps': {}
        }
    }
}

crawler_targets = {
    'S3Targets': [
        {
            'Path': s3_input_path
        }
    ],
    'DynamoDBTargets': [],
    'Catalogs': [],
    'JdbcTargets': [],
    'MongoDBTargets': [],
    'KafkaTargets': [],
    'S3Target': [],
    'Table': {
        'DatabaseName': database_name,
        'Name': table_name
    }
}

log_messages ='Hello, I am here'

try:
    # Create Glue Table
    response = client.create_table(table_input)

    # Attempt to create the Glue Crawler
    response = client.create_crawler(
        Name=crawler_name,
        Role=role_name,
        DatabaseName=database_name,
        Targets=crawler_targets,
        TablePrefix=crawler_name
    )

    response = cloudwatch_logs.put_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        logEvents=[
            {
                'timestamp': int(time.time() * 1000),  # Timestamp in milliseconds
                'message': log_messages
            }
        ]
    )
    logger.info(f"Crawler {crawler_name} created successfully.")
except botocore.exceptions.ClientError as e:
    # Handle existing crawler, etc.
    if e.response['Error']['Code'] == 'AlreadyExistsException':
         logger.info(f"Crawler {crawler_name} in Exception.")
    else:
        logger.error(f"Error: {e}")
        raise e
except Exception as e:
    logger.error(f"Error: {e}")
    raise e
>>>>>>> 38ff75ddde0a9bd3c09c78d3b88f592404b6c95c
