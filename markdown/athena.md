# What is Athena ?

Amazon Athena is an interactive query service that makes it easy to analyze data directly in Amazon Simple Storage Service (Amazon S3) using standard SQL. It's serverless, so there's no infrastructure to manage, and you only pay for the queries that you run. Athena is highly appealing for users ranging from seasoned data analysts to beginners due to its simplicity and integration with other AWS services.

## Key Features of Amazon Athena
- Serverless: Athena is serverless, meaning you don't need to set up, manage, or scale any infrastructure. You focus solely on writing queries and analyzing your data.

- SQL-Based: Athena allows you to use standard SQL expressions and functions to query your data. This makes it accessible to anyone with SQL knowledge and minimizes the learning curve for new users.

- Easy Integration: Athena is tightly integrated with AWS Glue, a managed ETL (Extract, Transform, Load) and data catalog service. AWS Glue can create and manage a data catalog that is used by Athena to run queries against the available data.

- Built-in with Amazon S3: Directly designed to query data stored in Amazon S3, Athena is ideal for analyzing large-scale datasets without the need for data movement or transformation.

- Wide Range of Data Formats: Supports various data formats such as CSV, JSON, ORC, Avro, and Parquet. Athena can handle both structured and semi-structured data, making it versatile for different data analytics needs.

- Pay-Per-Query: Costs are based only on the queries you execute, calculated by the amount of data scanned by each query. This can be cost-effective, especially with proper data management and query optimization techniques.

## When Should Athena be used?

- Ad-Hoc Querying of Log Data:
  Athena is ideal for querying log data stored in S3. Organizations often store web server logs, application logs, or event logs in S3, and Athena allows analysts to run 
  queries directly on this data to monitor application performance, user activities, or system anomalies.

-  Data Lake Exploration:
  For organizations utilizing a data lake architecture, Athena helps in querying and analyzing data stored in S3 as part of a data lake. This includes structured, semi- 
  structured, and unstructured data. Athena enables quick SQL queries across diverse datasets without the need to transform or load the data into a separate analytics system.
  <p align="center">
    <img src="images/athena_1.png" alt="Athena 1" width="600"/>
  </p>

- Quick BI Reporting:
  Athena integrates with popular BI tools via ODBC/JDBC drivers, enabling fast and effective business intelligence reporting. Businesses can use Athena for real-time 
  reporting on data stored in S3, creating dashboards that pull live data for up-to-date insights.

## How will Athena work?
  <p align="center">
    <img src="images/athena_2.png" alt="Athena 2" width="600"/>
  </p>


* AWS Glue Crawler: Walks through your data stores, looks at all the data, and takes notes about what it finds (like the types of data and their organization).

* AWS Glue Data Catalog: Stores these notes in an organized manner, making it easy for you and other AWS services to understand and use the data efficiently.

## Athena Console Walkthrough 
### Method 1 Access: Using Glue Data Crawler and Catalog
1. Connecting to AWS Data Catalog and choosing the database from Glue.
2. Executing ANSI SQL Queries in Athena.

### Method 2 Access: Reading from S3 without crawler
1. Creating table to directly read the data from S3 (CSV File) from specified location
   sample SQL query:
   ```sql
    CREATE EXTERNAL TABLE IF NOT EXISTS AwsDataCatalog.tinitiate_athena.pq_sample_athena (
     productid INT,
     categoryid INT,
     productname STRING,
     unit_price DOUBLE
     )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
     WITH SERDEPROPERTIES (
         "separatorChar" = ",",
         "quoteChar" = "\"",
         "escapeChar" = "\\"
     )
     STORED AS TEXTFILE
     LOCATION 's3://ti-student-apr-2024/athena/products_input'
     TBLPROPERTIES (
      'classification'='csv', 
      'skip.header.line.count'='1');
    ```
For this make sure your S3 bucket has give the glue access to create database in Glue Data Catalog. The above query can be explained as :
* CREATE EXTERNAL TABLE IF NOT EXISTS: Creates a new external table in Athena only if a table with the same name does not already exist.
* Table Name and Schema: Specifies the table's name and its columns (productid, categoryid, productname, unit_price) with their data types.
* ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde': Uses the OpenCSVSerde to parse CSV format data. SERDE (Serializer/Deserializer) is responsible for defining how strings in the file are interpreted as rows in the table. This choice is particularly suitable for CSV files.
* WITH SERDEPROPERTIES:
    - separatorChar = ",": Sets the comma (,) as the column delimiter.
    - quoteChar = "\"": Sets the double quote (") as the character for enclosing fields, particularly useful for handling fields that contain the delimiter.
    - escapeChar = "\\": Sets the backslash (\) as the escape character to allow for inclusion of special characters like newlines, tabs, or even delimiters as literal characters.
* STORED AS TEXTFILE: Indicates the data is stored as plain text files.
* LOCATION 's3://ti-author-data/retail/product/': Points to the specific Amazon S3 directory that contains the data files Athena will query.
* TBLPROPERTIES: Sets additional table properties such as CSV classification and instructs Athena to skip the header row in the data files.

UI Creation Walk through - Hands On

## Common Use Cases
- Ad-hoc Analysis: Quickly run ad-hoc queries against large-scale datasets. Analysts use Athena for data exploration and quick checks without needing to set up complex data processing infrastructure.

- Log Analysis: Commonly used for querying logs stored in S3, such as application logs, system logs, and web server logs. This helps in monitoring, troubleshooting, and the optimization of applications.

- Data Lake Queries: Execute queries on data lakes stored in S3. Athena helps in extracting insights from large pools of raw data stored in a distributed manner.

- Business Intelligence and Reporting: Integrated with various BI tools via JDBC/ODBC, Athena facilitates reporting and visual analytics, allowing businesses to make informed decisions based on the latest data.

## Athena Cost Model:

* Pay-Per-Query Pricing: Athena charges based on the amount of data scanned by your queries. You pay for the amount of data scanned in each query execution.
* No Data Loading or ETL Costs: Since Athena queries data directly in S3, there are no additional costs associated with data loading, transformation, or ETL processes
* Free Query Cancellation: You are not charged for queries if they are cancelled before any data is processed.
* Query Result Caching: Athena provides query result caching that can help reduce costs and improve performance for repeated queries over the same data.
* Data Transfer Costs: There are no additional data transfer fees for data scanned by Athena within the same AWS region. However, if query results are exported out of the 
  AWS region, standard AWS data transfer rates apply.

## Task 1 - Bucket Policies
1. Create two S3 Buckets.
2. Upload the files given to you during the class to your respective S3 buckets.
3. Create a bucket policy that allows all IAM users in your AWS account list-only access to Bucket1.
4. Create a bucket policy that allows all IAM users in your AWS account read-only access to Bucket2.

## Task 2 - Athena Tables (DDL) Creation from S3
1. From the folder uploaded into the "ti-student-apr-2024" , Creating tables in Athena_tinitate Data Catalog, as discussed in the class.
