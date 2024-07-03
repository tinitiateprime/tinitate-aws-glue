# Data Bucketing with PySpark in AWS Glue

This documentation outlines a PySpark script deployed within AWS Glue that efficiently buckets data based on a specific column (categoryid) and writes the organized data to Amazon S3. This process enhances data management and access, particularly for operations that can benefit from data being pre-sorted or pre-aggregated.

## Overview:
1. Data Bucketing: Distributes data across a defined number of buckets based on hash partitioning, improving query performance and reducing shuffling.
2. Data Format: Utilizes Parquet format for storage due to its efficient compression and encoding schemes.
   
## Prerequisites
Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-bucketing.py](../glue-code/ti-pyspark-bucketing.py)
* Input Table: products_csv
* Output Formats: CSV, JSON, and Parquet files in S3 buckets.
* Crawlers Used : product_crwaler

## Main Operations
### 1. Initializing Spark and Glue Contexts:
  * Objective: Configures the Spark and Glue contexts to ensure proper execution of operations with informative logging.
  * Implementation:
    ```python
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    sc = SparkContext()
    sc.setLogLevel("INFO")
    glueContext = GlueContext(sc)
    ```
### 2. Data Loading:
  * Objective: Load the purchase table from Athena into a DataFrame, preparing it for transformation.
  * Implementation:
    ```python
    df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="products_csv").toDF()
    ```
### 3. Data Bucketing:
  * Objective: Configures the number of buckets and the column used for partitioning.
  * Note: The choice of the number of buckets should consider the cardinality of the column (i.e., the number of distinct values). If the cardinality is lower than the number of buckets, you will naturally end up with empty buckets.
  * Implementation:
    ```python
    num_buckets = 5
    bucket_column = "categoryid"

    ```

### 4. Write Bucketed Data to S3:
  * aves the data to S3 bucketed by the categoryid column in Parquet format.
  * Implementation:
    ```python
    df.write.format("parquet") \
    .bucketBy(num_buckets, bucket_column) \
    .mode("overwrite") \
    .saveAsTable("bucketed_products", path=base_path)
    ```
  * Sample output for bucketing By Column : "catergoryid"
    <img width="959" alt="bucket_1" src="https://github.com/sarutlaa/tinitiate-aws-glue/assets/141533429/f26247fb-624b-4352-a38a-546740adda6c">
    
### 5. Verify Bucketing:
  * Objective: Reads the bucketed data back and prints its schema to verify the bucketing process and display some data.
  * Implementation:
    ```python
    bucketed_df = spark.read.parquet(base_path)
    bucketed_df.printSchema()
    bucketed_df.show()
    ```

### Note: 
* Parquet: Directly supports bucketing, so bucketBy is used.
* JSON and CSV: Do not support direct bucketing with bucketBy. Instead, we use repartition to simulate bucketing by distributing the data across multiple files based on the category column. This will not be true bucketing but will help in managing large datasets by reducing file sizes and potentially improving read times for filtered queries.
* Distinct Count Calculation: Before setting the number of buckets, the script calculates the distinct count of the values in the bucket_column. This count determines how many unique paths or directories will be needed.
* Dynamic Number of Buckets: The number of buckets is set to the lesser of the distinct count or a maximum value. This approach ensures that there are no more buckets than necessary, reducing the chance of creating empty buckets. It’s important to set a reasonable maximum based on the expected workload and data distribution.
* Flexibility and Optimization: This dynamic adjustment makes the bucketing process more flexible and optimized for the actual data characteristics. If the column has fewer distinct values than the maximum number of buckets you are willing to create, it uses just as many buckets as there are distinct values.
