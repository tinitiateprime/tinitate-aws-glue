# Reading JSON Data into AWS Glue
This document outlines the process for efficiently reading NDJSON (Newline Delimited JSON) data into AWS Glue, focusing specifically on handling both uncompressed and gzip-compressed NDJSON files directly from Amazon S3. This method leverages the powerful data processing capabilities of AWS Glue and Apache Spark to facilitate data ingestion and transformation.

Objective:

## Objective:
- Format Handling: Manage both uncompressed and gzip-compressed NDJSON files.
- Data Transformation: Utilize AWS Glue's capabilities alongside Apache Spark to effectively transform and process NDJSON data.

## Prerequisites:
- Input Sources: NDJSON data located in S3 buckets at the following paths:
  - Uncompressed NDJSON: s3://ti-author-data/customer-billing/ndjson/
  - Gzip-compressed NDJSON: s3://ti-author-data/customer-billing/ndjson-gz/
- Output: Results of the data processing are logged in AWS CloudWatch.

## PySpark Scripts:
* [pyspark-read-from-json-S3](../glue-code/ti-pyspark-read-from-ndjson-s3.py)

## Main Operations
### 1. Initializing Spark and Glue Contexts:
* objective:Set up the necessary Spark and AWS Glue contexts to facilitate data manipulation.
* Implemnetation:
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  from pyspark.sql import SparkSession
  from awsglue.job import Job
  
  sc = SparkContext.getOrCreate()
  glueContext = GlueContext(sc)
  spark = SparkSession.builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
  job = Job(glueContext)

  ```
### 2. Reading NDJSON Data from S3:

* Objective:  Load NDJSON and gzip-compressed NDJSON files directly from S3 using Spark DataFrame capabilities.

  *Note* : JSON and NDJSON compressed cannot be read into dynamic dataframe, hence spark dataframe is used. 
* Implemnetation:
  ```python
  # Read the regular NDJSON file
  df_ndjson = spark.read.option("lineSep", "\n").json(ndjson_path)
  
  # Read the gzip-compressed NDJSON file
  df_ndjson_gz = spark.read.option("lineSep", "\n").option("compression", "gzip").json(ndjson_gz_path)

  ```
### 3. Displaying Dataframe Contents:

* Objective: Show the contents of the DataFrames to verify correct data loading and provide a preview of the data.
* Implemnetation:
  ```python
  print("Regular NDJSON file preview:")
  df_ndjson.show(5)
  print("Gzip-compressed NDJSON file preview:")
  df_ndjson_gz.show(5)

  ```
### 4. Job Initialization and Commitment:

* Objective: Properly initialize and commit the AWS Glue job to ensure all processes are executed as configured.
* Implemnetation:
  ```python
  job.init("manual-job-name", {})
  job.commit()
