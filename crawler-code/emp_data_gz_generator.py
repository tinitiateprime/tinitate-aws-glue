<<<<<<< HEAD
import csv
import gzip
import os
import boto3
import random
from datetime import datetime, timedelta
from faker import Faker


# Set up Faker for generating fake data
fake = Faker()

# Set up AWS profile and S3 bucket information
aws_profile = 'jkumsi'
s3_bucket = 'tini-d-gluebucket-001'
s3_folder = 'incoming_data/hr-data/'

# Generate CSV data
data = []
for _ in range(100):
    emp_id = fake.unique.random_number(1000, 9999)
    ename = fake.name()
    join_date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
    salary = round(fake.random.uniform(30000, 100000), 2)
    dept_id = random.randint(1,10)
    data.append([emp_id, ename, join_date, salary,dept_id])

# Save CSV data to a file
csv_filename = 'employee_data.csv'
with open(csv_filename, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['emp_id', 'ename', 'join_date', 'salary','dept_id'])
    csv_writer.writerows(data)

# Zip the CSV file
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
gz_filename = f'employee_data_{timestamp}.csv.gz'
with open(csv_filename, 'rb') as f_in, gzip.open(gz_filename, 'wb') as f_out:
    f_out.writelines(f_in)

# Upload the gzipped file to S3 using the specified AWS profile
s3 = boto3.session.Session(profile_name=aws_profile).client('s3')
s3_key = os.path.join(s3_folder, gz_filename)
s3.upload_file(gz_filename, s3_bucket, s3_key)

# Cleanup: Remove the local CSV and gz files
os.remove(csv_filename)
os.remove(gz_filename)

print(f"CSV data generated, zipped, and uploaded to S3 bucket '{s3_bucket}' in folder '{s3_folder}' with key '{s3_key}'.")
=======
import csv
import gzip
import os
import boto3
import random
from datetime import datetime, timedelta
from faker import Faker


# Set up Faker for generating fake data
fake = Faker()

# Set up AWS profile and S3 bucket information
aws_profile = 'jkumsi'
s3_bucket = 'tini-d-gluebucket-001'
s3_folder = 'incoming_data/hr-data/'

# Generate CSV data
data = []
for _ in range(100):
    emp_id = fake.unique.random_number(1000, 9999)
    ename = fake.name()
    join_date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
    salary = round(fake.random.uniform(30000, 100000), 2)
    dept_id = random.randint(1,10)
    data.append([emp_id, ename, join_date, salary,dept_id])

# Save CSV data to a file
csv_filename = 'employee_data.csv'
with open(csv_filename, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['emp_id', 'ename', 'join_date', 'salary'])
    csv_writer.writerows(data)

# Zip the CSV file
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
gz_filename = f'employee_data_{timestamp}.csv.gz'
with open(csv_filename, 'rb') as f_in, gzip.open(gz_filename, 'wb') as f_out:
    f_out.writelines(f_in)

# Upload the gzipped file to S3 using the specified AWS profile
s3 = boto3.session.Session(profile_name=aws_profile).client('s3')
s3_key = os.path.join(s3_folder, gz_filename)
s3.upload_file(gz_filename, s3_bucket, s3_key)

# Cleanup: Remove the local CSV and gz files
os.remove(csv_filename)
os.remove(gz_filename)

print(f"CSV data generated, zipped, and uploaded to S3 bucket '{s3_bucket}' in folder '{s3_folder}' with key '{s3_key}'.")
>>>>>>> 38ff75ddde0a9bd3c09c78d3b88f592404b6c95c
