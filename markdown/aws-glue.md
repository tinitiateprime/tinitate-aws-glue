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
#### Read from Athena Tables
* [Select & Alias](glue-pyspark-select-alias.md)
* [Order By](glue-pyspark-orderby.md)
* [Group By](glue-pyspark-groupby.md)
* [Distinct](glue-pyspark-distinct.md)
* [Filtering](glue-pyspark-condition.md)
* [Having](glue-pyspark-having.md)
* [Joins](glue-pyspark-joins.md)
* [Set Operations](glue-pyspark-set-operations.md)
#### Read from S3 Folder Files
* [Select & Alias](glue-pyspark-select-alias.md)
* [Order By](glue-pyspark-orderby.md)
* [Group By](glue-pyspark-groupby.md)
* [Distinct](glue-pyspark-distinct.md)
* [Filtering](glue-pyspark-condition.md)
* [Having](glue-pyspark-having.md)
* [Joins](glue-pyspark-joins.md)
* [Set Operations](glue-pyspark-set-operations.md)

### Advanced Data Manipulation
#### Read from Athena Tables
* [Analytical Functions](glue-pyspark-analytical.md)
* [Pivot, UnPivot](glue-pyspark-pivot-unpivot.md)
* [Common Table Expressions](glue-pyspark-cte.md)
* [Create Table As Select](glue-pyspark-ctas.md)
#### Read from S3 Folder Files
* [Analytical Functions](glue-pyspark-analytical.md)
* [Pivot, UnPivot](glue-pyspark-pivot-unpivot.md)
* [Common Table Expressions](glue-pyspark-cte.md)
* [Create Table As Select](glue-pyspark-ctas.md)
  
### Date and Null Handling
#### Read from Athena Tables
* [Date Formats](glue-pyspark-date-formats.md)
* [Null Checks](glue-pyspark-null-checks.md)
#### Read from S3 Folder Files
* [Date Formats](glue-pyspark-date-formats.md)
* [Null Checks](glue-pyspark-null-checks.md)

### Advanced Query Techniques
#### Read from Athena Tables
* [Psuedo Columns](glue-pyspark-built-in-pseudo-columns.md)
#### Read from S3 Folder Files
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
* reading from AWS Athena Table

## Exporting a DataFrame into different targets:
#### Source : CSV files stored in S3, Target : Dynamic/Spark DataFrame
* [Writing CSV to S3](write-to-csv.md)
* [Writing JSON to S3](write-to-json.md)
* [Writing Parquet to S3](write-to-parquet.md)
* [Writing AWS ION to S3](write-to-awsion.md)

## Updating Data
🔁 1. Understanding Glue and Athena Data Flow
* Glue Data Catalog vs Glue Job vs Crawler vs Athena
* How Athena reads data from S3 using Glue Catalog
* Glue-supported file formats: CSV, JSON, Parquet, ORC, Avro
* Partitioning in Glue & Athena for efficient filtering and updates

✏️ 2. Performing UPDATEs in Athena via Glue (Indirect)
* Athena is read-only, so updates must be done through:
* CTAS (Create Table As Select)
* INSERT OVERWRITE (with partition projection)
* Glue PySpark job to filter/update and write back
* Topics:
* Using CTAS to simulate updates
* Rewriting S3 data with Glue PySpark jobs
* Update logic with SCD2 (Slowly Changing Dimensions)

❌ 3. Deleting Records from S3 Datasets
* Deletion = Rewrite excluding unwanted records
* Topics:
* Glue PySpark jobs to:
* Read data (from Parquet/CSV/JSON/NDJSON)
* Filter rows
* Overwrite S3 target
* DELETE simulation using partition overwrite
* Handling S3 versioning and clean-up

🧱 4. Format-Specific Considerations
✅ Parquet
* Columnar and supports schema evolution
* Efficient for updates via PySpark overwrite

✅ CSV / JSON
* Text-based, harder for large-scale efficient updates
* Use Glue job to rewrite entire files or partitions

✅ NDJSON (Newline-delimited JSON)
* Efficient for streaming-style ingestion
* Use spark.read.json(..., multiLine=False)
* Topics:
* Pros/cons of each format for upsert/delete
* Schema inference challenges (especially with JSON)

📄 5. Writing Glue PySpark Jobs
* Topics:
* Read data from S3 (spark.read.format(...))
* Apply updates/deletes using DataFrame transformations
* Write updated data to a new location
* Replace old data or update Glue Catalog table location
* Glue DynamicFrame ↔ DataFrame conversion

🧩 6. Partitioned Data Handling
* Topics:
* Overwriting specific partitions in S3
* Partition projection in Athena
* Glue job to update only affected partitions

🔄 7. Implementing UPSERT (Update + Insert) Logic
* Topics:
* Type 2 Slowly Changing Dimension (SCD2) via Glue
* Use of window functions and joins to detect changes
* Flagging active, setting eff_start / eff_end

🧪 8. End-to-End Scenarios for Practice
* Update customer names in a CSV dataset
* Delete all logs before a certain date in a JSON/NDJSON file
* Upsert product catalog stored in Parquet
* Simulate UPDATE on Athena via CTAS and S3 overwrite
* Incrementally process & update NDJSON from daily logs
🛠 9. Performance & Optimization Topics
* Glue job bookmarks (incremental update)
* File size and partition tuning
* Compression formats (Snappy, GZIP for Parquet/JSON)
* Catalog consistency issues


