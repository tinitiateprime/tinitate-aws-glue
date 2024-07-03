<<<<<<< HEAD
import boto3
import time

# Specify your AWS CLI profile
aws_profile = 'jkumsi'

# Create an AWSLogs client
cloudwatch_logs = boto3.client('logs', region_name='us-east-1', profile_name=aws_profile)

# Specify the log group and log stream
log_group_name = '/aws-glue/crawlers'
log_stream_name = 'emp_data_crawler'

# Get the latest timestamp of the log stream
response = cloudwatch_logs.describe_log_streams(
    logGroupName=log_group_name,
    logStreamNamePrefix=log_stream_name,
    orderBy='LastEventTime',
    descending=True,
    limit=1
)

latest_timestamp = response['logStreams'][0]['lastIngestionTime']

# Convert the timestamp to milliseconds
latest_timestamp_ms = int(latest_timestamp * 1000)

# Get the log events since the latest timestamp
response = cloudwatch_logs.get_log_events(
    logGroupName=log_group_name,
    logStreamName=log_stream_name,
    startTime=latest_timestamp_ms,
    limit=10  # Adjust the limit as needed
)

# Extract and print log messages
for event in response['events']:
    print(event['message'])
=======
import boto3
import time

# Specify your AWS CLI profile
aws_profile = 'jkumsi'

# Create an AWSLogs client
cloudwatch_logs = boto3.client('logs', region_name='us-east-1', profile_name=aws_profile)

# Specify the log group and log stream
log_group_name = '/aws-glue/crawlers'
log_stream_name = 'emp_data_crawler'

# Get the latest timestamp of the log stream
response = cloudwatch_logs.describe_log_streams(
    logGroupName=log_group_name,
    logStreamNamePrefix=log_stream_name,
    orderBy='LastEventTime',
    descending=True,
    limit=1
)

latest_timestamp = response['logStreams'][0]['lastIngestionTime']

# Convert the timestamp to milliseconds
latest_timestamp_ms = int(latest_timestamp * 1000)

# Get the log events since the latest timestamp
response = cloudwatch_logs.get_log_events(
    logGroupName=log_group_name,
    logStreamName=log_stream_name,
    startTime=latest_timestamp_ms,
    limit=10  # Adjust the limit as needed
)

# Extract and print log messages
for event in response['events']:
    print(event['message'])
>>>>>>> 38ff75ddde0a9bd3c09c78d3b88f592404b6c95c
