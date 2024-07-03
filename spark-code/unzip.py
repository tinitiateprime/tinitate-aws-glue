import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

import boto3
import io
from zipfile import ZipFile

s3 = boto3.client("s3")

bucket = "tini-d-gluebucket-001" # your s3 bucket name
prefix = "s3://tini-d-gluebucket-001/incoming_data/01_valid_invoices.zip" # the prefix for the objects that you want to unzip
unzip_prefix = "s3://tini-d-gluebucket-001/incoming_unzip_data/" # the location where you want to store your unzipped files 

# Get a list of all the resources in the specified prefix
objects = s3.list_objects(
    Bucket=bucket,
    Prefix=prefix
)["Contents"]

# The following will get the unzipped files so the job doesn't try to unzip a file that is already unzipped on every run
unzipped_objects = s3.list_objects(
    Bucket=bucket,
    Prefix=unzip_prefix
)["Contents"]

# Get a list containing the keys of the objects to unzip
object_keys = [ o["Key"] for o in objects if o["Key"].endswith(".zip") ] 
# Get the keys for the unzipped objects
unzipped_object_keys = [ o["Key"] for o in unzipped_objects ] 

for key in object_keys:
    obj = s3.get_object(
        Bucket="wayfair-datasource",
        Key=key
    )
    
    objbuffer = io.BytesIO(obj["Body"].read())
    
    # using context manager so you don't have to worry about manually closing the file
    with ZipFile(objbuffer) as zip:
        filenames = zip.namelist()

        # iterate over every file inside the zip
        for filename in filenames:
            with zip.open(filename) as file:
                filepath = unzip_prefix + filename
                if filepath not in unzipped_object_keys:
                    s3.upload_fileobj(file, bucket, filepath)

job.commit()