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

crawler_name = 'retail_category_crawler_part'
role_name = 'tini-d-glue-crawler-role-001'
database_name = 'glue_db'
s3_input_path = 's3://tini-d-gluebucket-001/incoming_data/retail/'
s3_output_path = 's3://tini-d-gluebucket-001/output_data/'
log_messages = 'Calling Targets'
crawler_targets = {
    'S3Targets': [
        {
            'Path': s3_input_path
        }
    ]
}

crawler_options = {
    'S3Targets': [
        {
            'Path': s3_output_path  # Set the output location
        }
    ]
}

log_messages ='Hello, I am here'

try:
    # Attempt to create the Glue Crawler
    response = client.create_crawler(
        Name=crawler_name,
        Role=role_name,
        DatabaseName=database_name,
        Targets=crawler_targets,
        TablePrefix='',  # Optional: Set a table prefix
        # CrawlerSecurityConfiguration='your_security_configuration',  # Optional: Set a security configuration
        # Add other optional parameters as needed
       # Configuration=f"{{\"Version\": \"1.0\", \"CrawlerOutput\": {\"OutputFormat\": \"COMPLETED\", \"OverwriteFilesWithDifferentSchema\": false, \"OverwriteEmptyFiles\": false, \"OutputOptions\": {{\"S3Targets\": {crawler_options['S3Targets']}}}}}}"
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
except botocore.exceptions.ClientError as e:
    # Check if the error is due to the crawler already existing
    log_messages ='Hello, I in except'
    if e.response['Error']['Code'] == 'AlreadyExistsException':
        # Check if the crawler is running
        response = client.get_crawler(Name=crawler_name)
        crawler_state = response['Crawler']['State']

        if crawler_state == 'RUNNING':
            # If it's running, wait for a while or stop it explicitly
            logger.info("Crawler is running. Waiting for it to complete or stopping it...")
            time.sleep(300)  # Wait for 5 minutes (adjust the time as needed)
            client.stop_crawler(Name=crawler_name)

            # You may want to add additional logic to ensure it stops before proceeding
            # For example, you can use a loop to check the status periodically and break out of the loop when it stops
        else:
            # If it's not running, update the Glue Crawler
            client.update_crawler(
                Name=crawler_name,
                Role=role_name,
                DatabaseName=database_name,
                Targets=crawler_targets,
                TablePrefix='',
                #Configuration=f"{{\"Version\": \"1.0\", \"CrawlerOutput\": {\"OutputFormat\": \"COMPLETED\", \"OverwriteFilesWithDifferentSchema\": false, \"OverwriteEmptyFiles\": false, \"OutputOptions\": {{\"S3Targets\": {crawler_options['S3Targets']}}}}}}"
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
            logger.info(f"Crawler {crawler_name} already exists. Updated successfully.")
except Exception as e:
    logger.info(type(e).__name__+ ' '+str(e))