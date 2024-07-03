# Reading JSON Data into AWS Glue
This document describes the process of reading JSON data into AWS Glue, utilizing both direct S3 access and AWS Glue Data Catalog. The aim is to provide a detailed overview of handling JSON data in various formats, ensuring efficient data ingestion and processing in AWS Glue.

## Objective:
- Format Handling: Manage both uncompressed and gzip-compressed JSON files reads into dynamic dataframes.
- Data Transformation: Leverage AWS Glue's capabilities to transform and process JSON data effectively.

## Prerequisites:
* Input Sources:
  * Method 1: Read JSON data from S3 bucket directly to dynamic dataframe.
    S3 buckets: s3://ti-author-data/customer-billing/json/, s3://ti-author-data/customer-billing/ndjson-gz/
  * Method 2: Utilize AWS Glue Data Catalog to manage metadata and read JSON data stored in S3.
* Output: Dynamic Dataframe results logged in CloudWatch.
* Crawlers: For Method 2, utilize "customer_billing_json", "customer_billing_gz_json" for automated metadata cataloging.

## PySpark Scripts:
* [pyspark-read-from-json-S3](../glue-code/ti-pyspark-read-from-json-s3.py)
* [pyspark-read-from-json-Cralwer](../glue-code/ti-pyspark-read-from-json-crawler.py)

## Method 1 
### Main Operations
### 1. Initializing Spark and Glue Contexts:
* objective:Set up the necessary Spark and AWS Glue contexts to facilitate data manipulation.
* Implemnetation:
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  sc = SparkContext()
  glueContext = GlueContext(sc)
  ```
### 2. Reading CSV Data from S3:

* Objective:  Load JSON and gzip-compressed JSON files directly from S3 into dynamic dataframes/Spark dataframe.

  *Note* : JSON compressed cannot be read into dynamic dataframe, hence spark dataframe is used. 
* Implemnetation:
  ```python
  # Read the regular JSON file
  dynamic_frame_json = glueContext.create_dynamic_frame.from_options(
      connection_type="s3",
      connection_options={"paths": [json_path]},
      format="json"
  )
  # Read the gzip-compressed JSON file using Spark DataFrame
  df_gzip_json = spark.read.option("compression", "gzip").json(gzip_json_path)
  ```
### 3. Data Transformation into DataFrames:

* Objective: Convert DynamicFrames into DataFrames to utilize advanced data processing capabilities of PySpark.
* Implemnetation:
  ```python
  df_json = dynamic_frame_json.toDF()
  ```
### 4. Displaying Dataframe Contents:

* Objective: Show the contents of the DataFrames to verify correct data loading and provide a preview of the data.
* Implemnetation:
  ```python
  print("Regular JSON file preview:")
  df_json.show(5)
  print("Compressed JSON file preview:")
  df_gzip_json.show(5)
  ```
### 5. Job Initialization and Commitment:

* Objective: Properly initialize and commit the AWS Glue job to ensure all processes are executed as configured.
* Implemnetation:
  ```python
  job.init("manual-job-name", {})
  job.commit()
  ```
## Method 2
### Main Operations
### 1. Initializing Spark and Glue Contexts:
* objective:Set up the necessary Spark and AWS Glue contexts to facilitate data manipulation.
* Implemnetation:
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  sc = SparkContext()
  glueContext = GlueContext(sc)
  ```
### 2. Data Loading from Glue Data Catalog:

* Objective: Leverage the AWS Glue Data Catalog for loading data, simplifying data management and accessibility.
* Implemnetation:
  ```python
  dynamic_frame_json = glueContext.create_dynamic_frame.from_catalog(
      database="glue_db",
      table_name="customer_billing_json"
  )
  dynamic_frame_gzip_json = glueContext.create_dynamic_frame.from_catalog(
      database="glue_db",
      table_name="customer_billing_gzip_json"
  )
  ```
### 3. Conversion to DataFrames and Processing:

* Objective: Convert DynamicFrames into Spark DataFrames to utilize PySpark's full data processing capabilities and perform simple data operations like counts or previews.
* Implemnetation:
  ```python
  df_json = dynamic_frame_json.toDF()
  df_gzip_json = dynamic_frame_gzip_json.toDF()
  ```
### 4. Displaying Dataframe Contents:

* Objective: Objective: Display contents of the DataFrames to verify data integrity and provide immediate data insights.
* Implemnetation:
  ```python
   print("Reading JSON Contents:", df_json.show(5))
   print("Reading JSON ZIP Contents:", df_gzip_json.show(5))
  ```
### 5. Job Initialization and Commitment:

* Objective: Properly initialize and commit the AWS Glue job to ensure all processes are executed as configured.
* Implemnetation:
  ```python
  job.init("manual-job-name", {})
  job.commit()
  ```
