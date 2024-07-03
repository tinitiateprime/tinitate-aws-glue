# Reading data from CSV files into Dataframe
This document describes the process of reading CSV data into AWS Glue, utilizing both direct S3 access and AWS Glue Data Catalog, which includes data stored as uncompressed CSV files and gzip-compressed CSV files. The aim is to provide a detailed overview of handling CSV data in various formats, ensuring efficient data ingestion and processing in AWS Glue.

* Method 1- Read from S3 - CSV data from S3 bucket to dynamic dataframe.
* Method 2: Utilize AWS Glue Data Catalog to manage metadata and read CSV data stored in S3.

## Objective:
- Format Handling: Manage both uncompressed and gzip-compressed CSV files reads into dyamic dataframe.
- Data Transformation: Leverage AWS Glue's capabilities to transform and process CSV data effectively.

## Prerequisites
- Input Sources:
  * Method 1 : S3 buckets s3://ti-author-data/customer-billing/csv/, s3://ti-author-data/customer-billing/csv-gz/
  * Method 2: Data Catalog tables - customer_billing_csv, customer_billing_csv_gz
- Output: Dynamic Dataframe results in cloudwatch
- Cralwers: Method 2 : "customer_billing_csv", "customer_billing_csv_zip"

## PySpark Scripts:
### Method 1: [Read from S3](../glue-code/ti-pyspark-read-from-csv-S3.py)
### Method 2: [Read from Data Catalog](../glue-code/ti-pyspark-read-from-csv-crawler.py)

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

* Objective: Load CSV and gzip-compressed CSV files directly from S3 into DynamicFrames.
* Implemnetation:
  ```python
  # Read the regular CSV file
  dynamic_frame_csv = glueContext.create_dynamic_frame.from_options(
      connection_type="s3",
      connection_options={"paths": [csv_path]},
      format="csv",
      format_options={"withHeader": True}
  )
  # Read the gzip-compressed CSV file
  dynamic_frame_gzip_csv = glueContext.create_dynamic_frame.from_options(
      connection_type="s3",
      connection_options={"paths": [gzip_csv_path]},
      format="csv",
      format_options={"withHeader": True, "compression": "gzip"}
  )
  ```
### 3. Data Transformation into DataFrames:

* Objective: Convert DynamicFrames into DataFrames to utilize advanced data processing capabilities of PySpark.
* Implemnetation:
  ```python
  df_csv = dynamic_frame_csv.toDF()
  df_gzip_csv = dynamic_frame_gzip_csv.toDF()
  ```
### 4. Displaying Dataframe Contents:

* Objective: Show the contents of the DataFrames to verify correct data loading and provide a preview of the data.
* Implemnetation:
  ```python
  print("Reading CSV Contents:", df_csv.show(5))
  print("Reading ZIP CSV Contents:", df_gzip_csv.show(5))
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
  dynamic_frame_csv = glueContext.create_dynamic_frame.from_catalog(
    database="glue_db", 
    table_name="customer_billing_csv"
  )
  dynamic_frame_gzip_csv = glueContext.create_dynamic_frame.from_catalog(
      database="glue_db", 
      table_name="customer_billing_csv_gz"
  )

  ```
### 3. Conversion to DataFrames and Processing:

* Objective: Convert DynamicFrames into Spark DataFrames to utilize PySpark's full data processing capabilities and perform simple data operations like counts or previews.
* Implemnetation:
  ```python
   df_csv = dynamic_frame_csv.toDF()
   df_gzip_csv = dynamic_frame_gzip_csv.toDF()

  ```
### 4. Displaying Dataframe Contents:

* Objective: Objective: Display contents of the DataFrames to verify data integrity and provide immediate data insights.
* Implemnetation:
  ```python
  print("Reading CSV Contents:", df_csv.show(5))
  print("Reading ZIP CSV Contents:", df_gzip_csv.show(5))
  ```
### 5. Job Initialization and Commitment:

* Objective: Properly initialize and commit the AWS Glue job to ensure all processes are executed as configured.
* Implemnetation:
  ```python
  job.init("manual-job-name", {})
  job.commit()
  ```
