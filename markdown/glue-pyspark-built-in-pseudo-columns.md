# Pseudo Columns with PySpark in AWS Glue

This document provides a detailed guide on utilizing PySpark in AWS Glue to enhance data with built-in and custom pseudo columns. The process involves transforming data from the from a sample dataframe created adding unique identifiers and other artificial attributes to facilitate data processing and analysis.

## Pseudo Columns in PySpark:
Pseudo columns are not directly stored within the data source but are generated on-the-fly during data processing. They are invaluable for creating unique row identifiers or adding sortable attributes that do not originally exist in the source data.

- monotonically_increasing_id(): Generates a unique identifier for each row, which increases monotonically across partitions but is not guaranteed to be consecutive.
- lit(): Creates a column with a fixed value.
- current_date() and current_timestamp(): Functions for adding the current date and timestamp, respectively, often used for time-stamping data transformations.
- expr("from_utc_timestamp(...)"): Converts UTC time to a specified timezone, enhancing data with locale-specific timestamps.

## Prerequisites
Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:

* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-psuedo-columns](../glue-code/ti-pyspark-psuedo.py)
- Input tables          : purchase
- Output files          : Displays results in cloudwatch logs
- Crawlers used         : purchase_crawler


## Main Operations
### 1. Initializing Spark and Glue Contexts:
  * Objective: Configures the Spark and Glue contexts to ensure proper execution of operations with informative logging and import the needed spark libraries.
  * Implementation:
    ```python
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from pyspark.sql import SparkSession, functions as F # Import for built in pseudo functions in spark 
    from pyspark.sql.types import IntegerType, StringType
    from pyspark.sql.functions import lit, current_date, current_timestamp, expr # Import for custom pseudo columns in spark
    
    # Initialize Spark and Glue contexts
    sc = SparkContext()
    sc.setLogLevel("INFO")
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    ```
### 2. Data Loading and Generation:
  * Objective: Load the purchase table from Athena into a DataFrame, preparing it for transformation.
  * Implementation:
    ```python
    df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="purchase").toDF()
    # Sample data with two columns
    data = [("Alice", 28), ("Bob", 34)]
    schema = ["Name", "Age"]
    
    # Create DataFrame
    test_df = spark.createDataFrame(data, schema=schema)
    print("Dataframe Created:")
    test_df.show()
    ```
### 3. Adding built in Pseudo Column:
  * Objective: Enrich the DataFrame by adding a monotonically increasing ID within test_df as a primary key.
  * Implementation:
    ```python
    # Add a primary key column using inbuilt pseudo function 'monotonically_increasing_id()'.
    test_df = test_df.withColumn("Primary_Key", F.monotonically_increasing_id())
    print("Dataframe after add PK:")
    test_df.show()
    ```
### 3. Adding Custom Pseudo Columns:
  * Objective: Enrich the DataFrame by adding a custom columns of String, Integer, Float, Datatime, timezone types within test_df with a fixed value.
  * Implementation:
    ```python
    # Add custom columns with fixed values and current timestamps
    test_df = test_df.withColumn("Custom_String", lit("Employee").cast(StringType())) \
                     .withColumn("Custom_Int", lit(100).cast(IntegerType())) \
                     .withColumn("Custom_Float", lit(123.456).cast("float")) \
                     .withColumn("Custom_Date", current_date()) \
                     .withColumn("Custom_DateTime", current_timestamp()) \
                     .withColumn("Custom_TimeZone", expr("from_utc_timestamp(current_timestamp(), 'America/New_York')"))
    ```

### 4. Displaying Final Outputs:
  * Objective: Diaplays the final output in cloudwatch logs.
  * Implementation:
    ```python
    print("Final Dataframe:")
    test_df.show(truncate=False)
    ```
