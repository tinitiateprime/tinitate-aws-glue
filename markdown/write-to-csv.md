# Writing Data into CSV 
This guide demonstrates how to create sample data within AWS Glue, convert it to a DataFrame and a DynamicFrame, and then write it to S3 as both uncompressed and gzip-compressed CSV files. This workflow is crucial for testing and verification in data processing pipelines, ensuring that data is correctly formatted and stored.

Objective:

## Objective:
- Data Creation and Manipulation: Create and manipulate sample data using PySpark and AWS Glue.
- Data Output: Efficiently write data to S3 in CSV format, both uncompressed and gzip-compressed.

## Prerequisites:
- Input Sources: Sample data in dynamic dataframe/Spark dataframe.
- Output: S3 bucktes:
  * s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-csv-outputs/csv/
  * s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-csv-outputs/csv-gz/

## PySpark Scripts - [Write to csv](../glue-code/ti-pyspark-write-to-csv.py)

## Main Operations
### 1. Initializing Spark and Glue Contexts:
* objective:Set up the necessary Spark and AWS Glue contexts to facilitate data manipulation.
* Implemnetation:
  ```python
  from awsglue.context import GlueContext
  from pyspark.context import SparkContext
  sc = SparkContext()
  glueContext = GlueContext(sc)


  ```
### 2. Sample Data Creation:

* Objective: Sample data is created using Row objects, which are then parallelized into a DataFrame to mimic real data.
* Implemnetation:
  ```python
  from pyspark.sql import Row
  data = [
      Row(name="John Doe", age=30, city="New York"),
      Row(name="Jane Smith", age=25, city="Los Angeles"),
      Row(name="Mike Johnson", age=35, city="Chicago")
  ]
  df = sc.parallelize(data).toDF()


  ```
### 3. Dataframe Coalescing:

* Objective: Coalescing the DataFrame to a single partition to streamline the output process, making the resulting file singular and easier to handle.
* Implemnetation:
  ```python
  df_single = df.coalesce(1)
  ```
### 4. Conversion to DynamicFrame:

* Objective: Convert the DataFrame to a DynamicFrame, which is the preferred format for data manipulation and output in AWS Glue.
* Implemnetation:
  ```python
  from awsglue.dynamicframe import DynamicFrame
  dynamic_df_single = DynamicFrame.fromDF(df_single, glueContext, "dynamic_df_single")
  ```
### 5. Writing Data to S3:

* Objective: Data is written to S3 in two formats: uncompressed CSV and gzip-compressed CSV. This demonstrates the flexibility of AWS Glue in handling different file compressions.
* Implemnetation:
  ```python
  # Write the data as uncompressed CSV
  glueContext.write_dynamic_frame.from_options(
      frame=dynamic_df_single,
      connection_type="s3",
      connection_options={"path": output_dir_csv},
      format="csv",
      format_options={"writeHeader": True}
  )
  
  # Write the data as gzip-compressed CSV
  glueContext.write_dynamic_frame.from_options(
      frame=dynamic_df_single,
      connection_type="s3",
      connection_options={"path": output_dir_csv_gz},
      format="csv",
      format_options={"writeHeader": True, "compression": "gzip"}
  )
  ```

### 6. Job Completion and Logging:

* Objective: Ensuring the job is successfully completed and logging the outcome. This is crucial for tracking and verifying that data is written as expected.
* Implemnetation:
  ```python
  print("Successfully written the data in S3")
  job.commit()
  ```
