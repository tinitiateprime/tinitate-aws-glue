
# Understanding Joins with PySpark in AWS Glue:

This document outlines the use of PySpark in AWS Glue for performing different types of joins between data stored in Glue Data Catalog. The script prepares the Spark and Glue contexts, performs data transformations, executes various joins, and displays the results.

## Types of Joins

![Joins_Diagram](https://github.com/sarutlaa/tinitiate-aws-glue/assets/141533429/3287a0ec-4739-4af3-9dc4-1cf39cfe6546)


1. Inner Join: Matches rows where there's a common value in the join columns of both tables.

2. Left Join: Includes all rows from the left table and matching rows from the right table based on the join condition.

3. Right Join: Includes all rows from the right table and matching rows from the left table based on the join condition.

4. Full Outer Join: Includes all rows from both tables, even if there's no matching value in the join columns.

5. Left Semi Join: Includes rows from the left table where there's a matching value in the right table, but only the left table's columns are included.

6. Left Anti Join: Includes rows from the left table where there's NO matching value in the right table.

Below is a detailed breakdown of the script's components and operations.

## Prerequisites for the pyspark script execution

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)

## PySpark Script - [pyspark-joins](../glue-code/ti-pyspark-joins.py)
- Input              : products_csv, categories_csv
- Output             : Cloudwatch Logs
- Crawlers used      : product_crawler, category_crawler


## Main Operations
### 1. Context Initialization:
  - Objective: Establish necessary contexts for Spark and Glue operations and set appropriate log levels.
  - Implementation:
    ```python
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("INFO")
    glueContext = GlueContext(sc)
    ```
### 2. Data Loading and Preparation:
  - Objective: Load tables from Athena into Spark DataFrames and prepare them for joining.
  - Implementation:
    ```python
    product_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="products_csv").toDF()
    category_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="categories_csv").toDF()
    product_selected_df = product_df.select("productid", "productname", "categoryid", "unit_price").withColumnRenamed("categoryid", "product_categoryid")
    category_selected_df = category_df.select("categoryid", "categoryname")
    ```
### 3. Display Initial DataFrames:
  - Objective: Show the structure and some data of the initial loaded DataFrames for verification.
  - Implementation:
    ```python
    print("DATAFRAME: product_selected_df")
    product_selected_df.show()
   
    print("DATAFRAME: category_selected_df")
    category_selected_df.show()
    ```
### 3. Executing Joins and Saving Results:
  - Inner Join: Display results where there is a match in both DataFrames.
    ```python
      print("DATAFRAME: Inner Join")
      inner_join_df = product_selected_df.join(
          category_selected_df,
          product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
          "inner"
      )
      inner_join_df.show()
    ```
  - Left Join: Display all records from the left DataFrame, and matched records from the right DataFrame.
    ```python
      print("DATAFRAME: Left Outer Join")
      left_outer_join_df = product_selected_df.join(
          category_selected_df,
          product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
          "left_outer"
      )
      left_outer_join_df.show()
    ```
  - Right Join: Display all records from the right DataFrame, and matched records from the left DataFrame.
    ```python
      print("DATAFRAME: Right Outer Join")
      right_outer_join_df = product_selected_df.join(
          category_selected_df,
          product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
          "right_outer"
      )
      right_outer_join_df.show()

    ```
- Full Outer Join: Display all records when there is a match in either the left or the right DataFrame.
    ```python
      print("DATAFRAME: Full Outer Join")
      full_outer_join_df = product_selected_df.join(
          category_selected_df,
          product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
          "outer"
      )
      full_outer_join_df.show()
    ```
- Left Semi Join: Display records from the left DataFrame for which there is a matching record in the right DataFrame, without including any columns from the right DataFrame.
   ```python
      print("DATAFRAME: Left Semi Join")
      left_semi_join_df = product_selected_df.join(
          category_selected_df,
          product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
          "left_semi"
      )
      left_semi_join_df.show()
   ```
- Left Anti Join: Display records from the left DataFrame where there are no corresponding records in the right DataFrame.
   ```python
      print("DATAFRAME: Left Anti Join")
      left_anti_join_df = product_selected_df.join(
          category_selected_df,
          product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
          "left_anti"
      )
      left_anti_join_df.show()
    ```

### 5. Outputs:
Outputs are displayed directly in the console and can be viewed in AWS CloudWatch logs.


## Executing multiple joins: [pyspark-multiple-joins](glue-pyspark-multiple-joins.md)
