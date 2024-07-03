# AWS Glue
> Jay Kumsi & Sravya Arutla

## Glue Topics
* [AWS Glue Overview](Intro.md)
* [AWS Glue Crawler](aws-glue-crawler.md)

## Glue with PySpark
* [IAM Prerequisites](IAM-prerequisites.md)
* [Crawler Setup Instructions](set-up-instructions.md)

## Additional Topics
* [Apache Spark Overview](spark.md)
  
## SQL Like Implementations with PySpark and AWS Glue ETL Scripts

### Data Retrieval and Manipulation

* [Select & Alias](glue-pyspark-select-alias.md)
* [Order By](glue-pyspark-orderby.md)
* [Group By](glue-pyspark-groupby.md)
* [Distinct](glue-pyspark-distinct.md)
* [Filtering](glue-pyspark-condition.md)
* [Having](glue-pyspark-having.md)
* [Joins](glue-pyspark-joins.md)
* [Set Operations](glue-pyspark-set-operations.md)

### Advanced Data Manipulation
* [Analytical Functions](glue-pyspark-analytical.md)
* [Pivot, UnPivot](glue-pyspark-pivot-unpivot.md)
* [Common Table Expressions](glue-pyspark-cte.md)
* [Create Table As Select](glue-pyspark-ctas.md)
  
### Date and Null Handling
* [Date Formats](glue-pyspark-date-formats.md)
* [Null Checks](glue-pyspark-null-checks.md)

### Advanced Query Techniques
* [Psuedo Columns](glue-pyspark-built-in-pseudo-columns.md)
  
### Partitioning Concepts:
* [Data Repartition](glue-repartition.md)
* [Data Partitioning](glue-s3-data-partitioning.md)
* [Bucketing](glue-bucketing.md)

## Adding External Libraies
* [External Libraries](adding-external-libraries.md)
  
## Building a DataFrame from Various Source types:
* [Reading from CSV](read-from-csv.md)
* [Reading from JSON](read-from-json.md)
* [Reading from NDJSON](read-from-ndjson.md)
* [Reading from Parquet](read-from-parquet.md)
* [Reading from AWS ION](read-from-awsion.md)
  
## Exporting a DataFrame into different targets:
#### Source : CSV files stored in S3, Target : Dynamic/Spark DataFrame
* [Writing CSV to S3](write-to-csv.md)
* [Writing JSON to S3](write-to-json.md)
* [Writing Parquet to S3](write-to-parquet.md)
* [Writing AWS ION to S3](write-to-awsion.md)



