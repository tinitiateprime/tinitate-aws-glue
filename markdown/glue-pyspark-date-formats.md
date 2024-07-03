# Understanding Timezone Conversion with PySpark in AWS Glue
This guide explains how to leverage PySpark within AWS Glue for advanced datetime manipulations, focusing on historical data from a "dispatch" table stored in Athena. The operations include converting datetime strings to a standard UTC format, applying timezone adjustments, and performing various datetime calculations. These manipulations prepare the data for analyses that require precise timing, such as scheduling and duration assessments.

DateTime manipulations are crucial for preparing data for analyses that depend on accurate timing and scheduling insights. This includes timezone adjustments, formatting dates for readability, and calculating time differences to understand durations or delays.

## Prerequisites

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  

##  PySpark Script - [pyspark-date-formats](../glue-code/ti-pyspark-datetime.py)
- Input tables          : dispatch
- Output                : Cloudwatch logs
- Crawlers used         : dispatch_crawler


## Main Operations

### 1. Initializing Spark and Glue Contexts:
* Objective: Establishes the necessary Spark and Glue contexts for data manipulation with logging set to INFO to control verbosity.
* Code Example:
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  sc = SparkContext()
  sc.setLogLevel("INFO")
  glueContext = GlueContext(sc)
  ```

### 2. Data Loading:
* Objective: Loads the "dispatch" table from the Athena database into a DataFrame, preparing it for datetime conversion.
* Implementation:
  ```python
  df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="dispatch").toDF()
  ```
### 3. Parsing Dates:
* Objective: Convert the 'dispatch_date' string column to a TimestampType to standardize and facilitate further datetime operations.
* Implementation:
  ```python
  from pyspark.sql.functions import to_timestamp
  df_prepared = df.withColumn("parsed_date", to_timestamp(df["dispatch_date"], "dd-MMM-yyyy HH:mm:ss"))
  ```  
    
### 4.  Additional DateTime Operations:
* Objective: Perform additional datetime manipulations including date addition/subtraction, extraction of year/month/day, and calculation of time differences in years.
* Note: Replace 'desired_timezone' with the appropriate timezone string, like 'America/New_York'.
* Implementation:
  ```python
  from pyspark.sql.functions import date_add, date_sub, year, month, dayofmonth, expr
  df_add_days = df_prepared.withColumn("date_plus_10_days", date_add("parsed_date", 10))
  df_sub_days = df_add_days.withColumn("date_minus_10_days", date_sub("parsed_date", 10))
  df_extracted = df_sub_days.withColumn("year", year("parsed_date"))
  df_extracted = df_extracted.withColumn("month", month("parsed_date"))
  df_extracted = df_extracted.withColumn("day", dayofmonth("parsed_date"))
  df_date_diff = df_extracted.withColumn("years_from_now", expr("floor(datediff(current_date(), parsed_date)/365)"))
  ```

### 5. Displaying Results and Logging:
* Objective: Show intermediate and final results directly in the execution environment and log critical information for verification and troubleshooting.
* Implementation:
  ```python
  df_extracted.show()
  df_date_diff.show()
  glueContext.get_logger().info("Completed data manipulation and logged results.")
  ```
