PySpark Cheat Sheet
===================
This cheat sheet will help you learn PySpark and write PySpark apps faster. Everything in here is fully functional PySpark code you can run or adapt to your programs.

These snippets are licensed under the CC0 1.0 Universal License. That means you can freely copy and adapt these code snippets and you don't need to give attribution or include any notices.

These snippets use DataFrames loaded from various data sources:
- "Auto MPG Data Set" available from the [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/auto+mpg).
- customer_spend.csv, a generated time series dataset.
- date_examples.csv, a generated dataset with various date and time formats.

These snippets were tested against the Spark 3.0.1 API. This page was last updated 2020-12-25 08:08:01.

Make note of these helpful links:
- [PySpark DataFrame Operations](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)
- [Built-in Spark SQL Functions](https://spark.apache.org/docs/latest/api/sql/index.html)
- [PySpark MLlib Reference](https://spark.apache.org/docs/latest/api/python/pyspark.mllib.html)
- [PySpark SQL Functions Source](https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html)

If you find this guide helpful and want an easy way to run Spark, check out [Oracle Cloud Infrastructure Data Flow](https://www.oracle.com/big-data/data-flow/), a fully-managed Spark service that lets you run Spark jobs at any scale with no administrative overhead. You can try Data Flow free.


Table of contents
=================

<!--ts-->
   * [Loading and Saving Data](#loading-and-saving-data)
      * [Load a DataFrame from CSV](#load-a-dataframe-from-csv)
      * [Load a DataFrame from a Tab Separated Value (TSV) file](#load-a-dataframe-from-a-tab-separated-value-tsv-file)
      * [Load a CSV file with a money column into a DataFrame](#load-a-csv-file-with-a-money-column-into-a-dataframe)
      * [Provide the schema when loading a DataFrame from CSV](#provide-the-schema-when-loading-a-dataframe-from-csv)
      * [Load a DataFrame from JSON Lines (jsonl) Formatted Data](#load-a-dataframe-from-json-lines-jsonl-formatted-data)
      * [Configure security to read a CSV file from Oracle Cloud Infrastructure Object Storage](#configure-security-to-read-a-csv-file-from-oracle-cloud-infrastructure-object-storage)
      * [Save a DataFrame in Parquet format](#save-a-dataframe-in-parquet-format)
      * [Save a DataFrame in CSV format](#save-a-dataframe-in-csv-format)
      * [Save a DataFrame to CSV, overwriting existing data](#save-a-dataframe-to-csv-overwriting-existing-data)
      * [Save a DataFrame to CSV with a header](#save-a-dataframe-to-csv-with-a-header)
      * [Save a DataFrame in a single CSV file](#save-a-dataframe-in-a-single-csv-file)
      * [Save DataFrame as a dynamic partitioned table](#save-dataframe-as-a-dynamic-partitioned-table)
      * [Overwrite specific partitions](#overwrite-specific-partitions)
      * [Read an Oracle DB table into a DataFrame using a Wallet](#read-an-oracle-db-table-into-a-dataframe-using-a-wallet)
      * [Write a DataFrame to an Oracle DB table using a Wallet](#write-a-dataframe-to-an-oracle-db-table-using-a-wallet)
   * [DataFrame Operations](#dataframe-operations)
      * [Add a new column with to a DataFrame](#add-a-new-column-with-to-a-dataframe)
      * [Modify a DataFrame column](#modify-a-dataframe-column)
      * [Add a column with multiple conditions](#add-a-column-with-multiple-conditions)
      * [Add a constant column](#add-a-constant-column)
      * [Concatenate columns](#concatenate-columns)
      * [Drop a column](#drop-a-column)
      * [Change a column name](#change-a-column-name)
      * [Change multiple column names](#change-multiple-column-names)
      * [Convert a DataFrame column to a Python list](#convert-a-dataframe-column-to-a-python-list)
      * [Select particular columns from a DataFrame](#select-particular-columns-from-a-dataframe)
      * [Create an empty dataframe with a specified schema](#create-an-empty-dataframe-with-a-specified-schema)
      * [Create a constant dataframe](#create-a-constant-dataframe)
      * [Convert String to Double](#convert-string-to-double)
      * [Convert String to Integer](#convert-string-to-integer)
      * [Get the size of a DataFrame](#get-the-size-of-a-dataframe)
      * [Get a DataFrame's number of partitions](#get-a-dataframe-s-number-of-partitions)
      * [Get data types of a DataFrame's columns](#get-data-types-of-a-dataframe-s-columns)
      * [Convert an RDD to Data Frame](#convert-an-rdd-to-data-frame)
      * [Print the contents of an RDD](#print-the-contents-of-an-rdd)
      * [Print the contents of a DataFrame](#print-the-contents-of-a-dataframe)
      * [Process each row of a DataFrame](#process-each-row-of-a-dataframe)
      * [DataFrame Map example](#dataframe-map-example)
      * [DataFrame Flatmap example](#dataframe-flatmap-example)
      * [Create a custom UDF](#create-a-custom-udf)
   * [Transforming Data](#transforming-data)
      * [Extract data from a string using a regular expression](#extract-data-from-a-string-using-a-regular-expression)
      * [Fill NULL values in specific columns](#fill-null-values-in-specific-columns)
      * [Fill NULL values with column average](#fill-null-values-with-column-average)
      * [Fill NULL values with group average](#fill-null-values-with-group-average)
      * [Unpack a DataFrame's JSON column to a new DataFrame](#unpack-a-dataframe-s-json-column-to-a-new-dataframe)
      * [Query a JSON column](#query-a-json-column)
   * [Sorting and Searching](#sorting-and-searching)
      * [Filter a column using a condition](#filter-a-column-using-a-condition)
      * [Filter based on a specific column value](#filter-based-on-a-specific-column-value)
      * [Filter based on an IN list](#filter-based-on-an-in-list)
      * [Filter based on a NOT IN list](#filter-based-on-a-not-in-list)
      * [Filter values based on keys in another DataFrame](#filter-values-based-on-keys-in-another-dataframe)
      * [Get Dataframe rows that match a substring](#get-dataframe-rows-that-match-a-substring)
      * [Filter a Dataframe based on a custom substring search](#filter-a-dataframe-based-on-a-custom-substring-search)
      * [Filter based on a column's length](#filter-based-on-a-column-s-length)
      * [Multiple filter conditions](#multiple-filter-conditions)
      * [Sort DataFrame by a column](#sort-dataframe-by-a-column)
      * [Take the first N rows of a DataFrame](#take-the-first-n-rows-of-a-dataframe)
      * [Get distinct values of a column](#get-distinct-values-of-a-column)
      * [Remove duplicates](#remove-duplicates)
   * [Grouping](#grouping)
      * [count(*) on a particular column](#count-on-a-particular-column)
      * [Group and sort](#group-and-sort)
      * [Filter groups based on an aggregate value, equivalent to SQL HAVING clause](#filter-groups-based-on-an-aggregate-value-equivalent-to-sql-having-clause)
      * [Group by multiple columns](#group-by-multiple-columns)
      * [Aggregate multiple columns](#aggregate-multiple-columns)
      * [Aggregate multiple columns with custom orderings](#aggregate-multiple-columns-with-custom-orderings)
      * [Get the maximum of a column](#get-the-maximum-of-a-column)
      * [Sum a list of columns](#sum-a-list-of-columns)
      * [Sum a column](#sum-a-column)
      * [Aggregate all numeric columns](#aggregate-all-numeric-columns)
      * [Count unique after grouping](#count-unique-after-grouping)
      * [Count distinct values on all columns](#count-distinct-values-on-all-columns)
      * [Group by then filter on the count](#group-by-then-filter-on-the-count)
      * [Find the top N per row group (use N=1 for maximum)](#find-the-top-n-per-row-group-use-n-1-for-maximum)
      * [XXX NTILE on Count](#xxx-ntile-on-count)
      * [Group key/values into a list](#group-key-values-into-a-list)
      * [Compute a histogram](#compute-a-histogram)
   * [Joining DataFrames](#joining-dataframes)
      * [Join two DataFrames by column name](#join-two-dataframes-by-column-name)
      * [Join two DataFrames with an expression](#join-two-dataframes-with-an-expression)
      * [Multiple join conditions](#multiple-join-conditions)
      * [Various Spark join types](#various-spark-join-types)
      * [Concatenate two DataFrames](#concatenate-two-dataframes)
      * [Load multiple files into a single DataFrame](#load-multiple-files-into-a-single-dataframe)
      * [Subtract DataFrames](#subtract-dataframes)
   * [File Processing](#file-processing)
      * [Load Local File Details into a DataFrame](#load-local-file-details-into-a-dataframe)
      * [Load Files from Oracle Cloud Infrastructure into a DataFrame](#load-files-from-oracle-cloud-infrastructure-into-a-dataframe)
      * [Transform Many Images using Pillow](#transform-many-images-using-pillow)
   * [Handling Missing Data](#handling-missing-data)
      * [Filter rows with None or Null values](#filter-rows-with-none-or-null-values)
      * [Drop rows with Null values](#drop-rows-with-null-values)
      * [Count all Null or NaN values in a DataFrame](#count-all-null-or-nan-values-in-a-dataframe)
   * [Dealing with Dates](#dealing-with-dates)
      * [Convert an ISO 8601 formatted date string to date type](#convert-an-iso-8601-formatted-date-string-to-date-type)
      * [Convert a custom formatted date string to date type](#convert-a-custom-formatted-date-string-to-date-type)
      * [Get the last day of the current month](#get-the-last-day-of-the-current-month)
      * [Convert UNIX (seconds since epoch) timestamp to date](#convert-unix-seconds-since-epoch-timestamp-to-date)
      * [Load a CSV file with complex dates into a DataFrame](#load-a-csv-file-with-complex-dates-into-a-dataframe)
   * [Unstructured Analytics](#unstructured-analytics)
      * [Flatten top level text fields in a JSONl document](#flatten-top-level-text-fields-in-a-jsonl-document)
      * [Flatten top level text fields from a JSON column](#flatten-top-level-text-fields-from-a-json-column)
      * [Unnest an array of complex structures](#unnest-an-array-of-complex-structures)
   * [Pandas](#pandas)
      * [Convert Spark DataFrame to Pandas DataFrame](#convert-spark-dataframe-to-pandas-dataframe)
      * [Convert Pandas DataFrame to Spark DataFrame](#convert-pandas-dataframe-to-spark-dataframe)
      * [Convert N rows from a DataFrame to a Pandas DataFrame](#convert-n-rows-from-a-dataframe-to-a-pandas-dataframe)
      * [Define a UDAF with Pandas](#define-a-udaf-with-pandas)
      * [Define a Pandas Grouped Map Function](#define-a-pandas-grouped-map-function)
   * [Data Profiling](#data-profiling)
      * [Compute the number of NULLs across all columns](#compute-the-number-of-nulls-across-all-columns)
      * [Compute average values of all numeric columns](#compute-average-values-of-all-numeric-columns)
      * [Compute minimum values of all numeric columns](#compute-minimum-values-of-all-numeric-columns)
      * [Compute maximum values of all numeric columns](#compute-maximum-values-of-all-numeric-columns)
      * [Compute median values of all numeric columns](#compute-median-values-of-all-numeric-columns)
      * [Identify Outliers in a DataFrame](#identify-outliers-in-a-dataframe)
   * [Spark Streaming](#spark-streaming)
      * [Connect to Kafka using SASL PLAIN authentication](#connect-to-kafka-using-sasl-plain-authentication)
      * [Add the current timestamp to a DataFrame](#add-the-current-timestamp-to-a-dataframe)
   * [Time Series](#time-series)
      * [Zero fill missing values in a timeseries](#zero-fill-missing-values-in-a-timeseries)
      * [First Time an ID is Seen](#first-time-an-id-is-seen)
      * [Cumulative Sum](#cumulative-sum)
      * [Cumulative Sum in a Period](#cumulative-sum-in-a-period)
      * [Cumulative Average](#cumulative-average)
      * [Cumulative Average in a Period](#cumulative-average-in-a-period)
   * [Machine Learning](#machine-learning)
      * [A basic Linear Regression model](#a-basic-linear-regression-model)
      * [A basic Random Forest Regression model](#a-basic-random-forest-regression-model)
      * [A basic Random Forest Classification model](#a-basic-random-forest-classification-model)
      * [Encode string variables before using a VectorAssembler](#encode-string-variables-before-using-a-vectorassembler)
      * [Get feature importances of a trained model](#get-feature-importances-of-a-trained-model)
      * [Automatically encode categorical variables](#automatically-encode-categorical-variables)
      * [Hyperparameter tuning](#hyperparameter-tuning)
      * [Plot Hyperparameter tuning metrics](#plot-hyperparameter-tuning-metrics)
      * [A Random Forest Classification model with Hyperparameter Tuning](#a-random-forest-classification-model-with-hyperparameter-tuning)
      * [Compute correlation matrix](#compute-correlation-matrix)
      * [Load a model and use it for predictions](#load-a-model-and-use-it-for-predictions)
      * [Save a model](#save-a-model)
   * [Performance](#performance)
      * [Get the Spark version](#get-the-spark-version)
      * [Cache a DataFrame](#cache-a-dataframe)
      * [Partition by a Column Value](#partition-by-a-column-value)
      * [Range Partition a DataFrame](#range-partition-a-dataframe)
      * [Change Number of DataFrame Partitions](#change-number-of-dataframe-partitions)
      * [Coalesce DataFrame partitions](#coalesce-dataframe-partitions)
      * [Set the number of shuffle partitions](#set-the-number-of-shuffle-partitions)
      * [Sample a subset of a DataFrame](#sample-a-subset-of-a-dataframe)
      * [Print Spark configuration properties](#print-spark-configuration-properties)
      * [Set Spark configuration properties](#set-spark-configuration-properties)
      * [Increase Spark driver/executor heap space](#increase-spark-driver-executor-heap-space)
<!--te-->
    

Loading and Saving Data
=======================
Loading data into DataFrames from various formats, and saving it out again.

Load a DataFrame from CSV
-------------------------

```python
# See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html
# for a list of supported options.
df = spark.read.format("csv").option("header", True).load("data/auto-mpg.csv")
```
```
# Code snippet result:
+----+---------+------------+----------+------+------------+---------+------+----------+
| mpg|cylinders|displacement|horsepower|weight|acceleration|modelyear|origin|   carname|
+----+---------+------------+----------+------+------------+---------+------+----------+
|18.0|        8|       307.0|     130.0| 3504.|        12.0|       70|     1|chevrol...|
|15.0|        8|       350.0|     165.0| 3693.|        11.5|       70|     1|buick s...|
|18.0|        8|       318.0|     150.0| 3436.|        11.0|       70|     1|plymout...|
|16.0|        8|       304.0|     150.0| 3433.|        12.0|       70|     1|amc reb...|
|17.0|        8|       302.0|     140.0| 3449.|        10.5|       70|     1|ford to...|
|15.0|        8|       429.0|     198.0| 4341.|        10.0|       70|     1|ford ga...|
|14.0|        8|       454.0|     220.0| 4354.|         9.0|       70|     1|chevrol...|
|14.0|        8|       440.0|     215.0| 4312.|         8.5|       70|     1|plymout...|
|14.0|        8|       455.0|     225.0| 4425.|        10.0|       70|     1|pontiac...|
|15.0|        8|       390.0|     190.0| 3850.|         8.5|       70|     1|amc amb...|
+----+---------+------------+----------+------+------------+---------+------+----------+
only showing top 10 rows
```

Load a DataFrame from a Tab Separated Value (TSV) file
------------------------------------------------------

```python
# See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html
# for a list of supported options.
df = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", "\t")
    .load("data/auto-mpg.tsv")
)
```
```
# Code snippet result:
+----+---------+------------+----------+------+------------+---------+------+----------+
| mpg|cylinders|displacement|horsepower|weight|acceleration|modelyear|origin|   carname|
+----+---------+------------+----------+------+------------+---------+------+----------+
|18.0|        8|       307.0|     130.0| 3504.|        12.0|       70|     1|chevrol...|
|15.0|        8|       350.0|     165.0| 3693.|        11.5|       70|     1|buick s...|
|18.0|        8|       318.0|     150.0| 3436.|        11.0|       70|     1|plymout...|
|16.0|        8|       304.0|     150.0| 3433.|        12.0|       70|     1|amc reb...|
|17.0|        8|       302.0|     140.0| 3449.|        10.5|       70|     1|ford to...|
|15.0|        8|       429.0|     198.0| 4341.|        10.0|       70|     1|ford ga...|
|14.0|        8|       454.0|     220.0| 4354.|         9.0|       70|     1|chevrol...|
|14.0|        8|       440.0|     215.0| 4312.|         8.5|       70|     1|plymout...|
|14.0|        8|       455.0|     225.0| 4425.|        10.0|       70|     1|pontiac...|
|15.0|        8|       390.0|     190.0| 3850.|         8.5|       70|     1|amc amb...|
+----+---------+------------+----------+------+------------+---------+------+----------+
only showing top 10 rows
```

Load a CSV file with a money column into a DataFrame
----------------------------------------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import DecimalType
from decimal import Decimal

# Load the text file.
df = (
    spark.read.format("csv")
    .option("header", True)
    .load("data/customer_spend.csv")
)

# Convert with a hardcoded custom UDF.
money_udf = udf(lambda x: Decimal(x[1:].replace(",", "")), DecimalType(8, 4))
money1 = df.withColumn("spend_dollars", money_udf(df.spend_dollars))

# Convert with the money_parser library (much safer).
from money_parser import price_str

money_convert = udf(
    lambda x: Decimal(price_str(x)) if x is not None else None,
    DecimalType(8, 4),
)
money2 = df.withColumn("spend_dollars", money_convert(df.spend_dollars))
```
```
# Code snippet result:
+----------+-----------+-------------+
|      date|customer_id|spend_dollars|
+----------+-----------+-------------+
|2020-01-31|          0|       0.0700|
|2020-01-31|          1|       0.9800|
|2020-01-31|          2|       0.0600|
|2020-01-31|          3|       0.6500|
|2020-01-31|          4|       0.5700|
|2020-02-29|          0|       0.1000|
|2020-02-29|          2|       4.4000|
|2020-02-29|          3|       0.3900|
|2020-02-29|          4|       2.1300|
|2020-02-29|          5|       0.8200|
+----------+-----------+-------------+
only showing top 10 rows
```

Provide the schema when loading a DataFrame from CSV
----------------------------------------------------

```python
# See https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/types.html
# for a list of types.
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType(
    [
        StructField("mpg", DoubleType(), True),
        StructField("cylinders", IntegerType(), True),
        StructField("displacement", DoubleType(), True),
        StructField("horsepower", DoubleType(), True),
        StructField("weight", DoubleType(), True),
        StructField("acceleration", DoubleType(), True),
        StructField("modelyear", IntegerType(), True),
        StructField("origin", IntegerType(), True),
        StructField("carname", StringType(), True),
    ]
)
df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load("data/auto-mpg.csv")
)
```
```
# Code snippet result:
+----+---------+------------+----------+------+------------+---------+------+----------+
| mpg|cylinders|displacement|horsepower|weight|acceleration|modelyear|origin|   carname|
+----+---------+------------+----------+------+------------+---------+------+----------+
|18.0|        8|       307.0|     130.0|3504.0|        12.0|       70|     1|chevrol...|
|15.0|        8|       350.0|     165.0|3693.0|        11.5|       70|     1|buick s...|
|18.0|        8|       318.0|     150.0|3436.0|        11.0|       70|     1|plymout...|
|16.0|        8|       304.0|     150.0|3433.0|        12.0|       70|     1|amc reb...|
|17.0|        8|       302.0|     140.0|3449.0|        10.5|       70|     1|ford to...|
|15.0|        8|       429.0|     198.0|4341.0|        10.0|       70|     1|ford ga...|
|14.0|        8|       454.0|     220.0|4354.0|         9.0|       70|     1|chevrol...|
|14.0|        8|       440.0|     215.0|4312.0|         8.5|       70|     1|plymout...|
|14.0|        8|       455.0|     225.0|4425.0|        10.0|       70|     1|pontiac...|
|15.0|        8|       390.0|     190.0|3850.0|         8.5|       70|     1|amc amb...|
+----+---------+------------+----------+------+------------+---------+------+----------+
only showing top 10 rows
```

Load a DataFrame from JSON Lines (jsonl) Formatted Data
-------------------------------------------------------

```python
# JSON Lines / jsonl format uses one JSON document per line.
# If you have data with mostly regular structure this is better than nesting it in an array.
# See https://jsonlines.org/
df = spark.read.json("data/weblog.jsonl")
```
```
# Code snippet result:
+----------+----------+--------+----------+----------+------+
|    client|   country| session| timestamp|       uri|  user|
+----------+----------+--------+----------+----------+------+
|[false,...|Bangladesh|55fa8213| 869196249|http://...|dde312|
|[true, ...|      Niue|2fcd4a83|1031238717|http://...|9d00b9|
|[true, ...|    Rwanda|013b996e| 628683372|http://...|1339d4|
|[false,...|   Austria|07e8a71a|1043628668|https:/...|966312|
|[false,...|    Belize|b23d05d8| 192738669|http://...|2af1e1|
|[false,...|Lao Peo...|d83dfbae|1066490444|http://...|844395|
|[false,...|French ...|e77dfaa2|1350920869|https:/...|  null|
|[false,...|Turks a...|56664269| 280986223|http://...|  null|
|[false,...|  Ethiopia|628d6059| 881914195|https:/...|8ab45a|
|[false,...|Saint K...|85f9120c|1065114708|https:/...|  null|
+----------+----------+--------+----------+----------+------+
only showing top 10 rows
```

Configure security to read a CSV file from Oracle Cloud Infrastructure Object Storage
-------------------------------------------------------------------------------------

```python
import oci

oci_config = oci.config.from_file()
conf = spark.sparkContext.getConf()
conf.set("fs.oci.client.auth.tenantId", oci_config["tenancy"])
conf.set("fs.oci.client.auth.userId", oci_config["user"])
conf.set("fs.oci.client.auth.fingerprint", oci_config["fingerprint"])
conf.set("fs.oci.client.auth.pemfilepath", oci_config["key_file"])
conf.set(
    "fs.oci.client.hostname",
    "https://objectstorage.{0}.oraclecloud.com".format(oci_config["region"]),
)
PATH = "oci://<your_bucket>@<your_namespace/<your_path>"
df = spark.read.format("csv").option("header", True).load(PATH)
```


Save a DataFrame in Parquet format
----------------------------------

```python
df.write.parquet("output.parquet")
```


Save a DataFrame in CSV format
------------------------------

```python
# See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html
# for a list of supported options.
df.write.csv("output.csv")
```


Save a DataFrame to CSV, overwriting existing data
--------------------------------------------------

```python
df.write.mode("overwrite").csv("output.csv")
```


Save a DataFrame to CSV with a header
-------------------------------------

```python
# See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html
# for a list of supported options.
df.coalesce(1).write.csv("header.csv", header="true")
```


Save a DataFrame in a single CSV file
-------------------------------------

```python
df.coalesce(1).write.csv("single.csv")
```


Save DataFrame as a dynamic partitioned table
---------------------------------------------

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("append").partitionBy("modelyear").saveAsTable("autompg")
```


Overwrite specific partitions
-----------------------------

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
your_dataframe.write.mode("overwrite").insertInto("your_table")
```


Read an Oracle DB table into a DataFrame using a Wallet
-------------------------------------------------------

```python
# Key variables you need.
# Get the tnsname from tnsnames.ora.
# Wallet path should point to an extracted wallet file.
password = "my_password"
table = "source_table"
tnsname = "my_tns_name"
user = "ADMIN"
wallet_path = "/path/to/your/wallet"

properties = {
    "driver": "oracle.jdbc.driver.OracleDriver",
    "oracle.net.tns_admin": tnsname,
    "password": password,
    "user": user,
}
url = f"jdbc:oracle:thin:@{tnsname}?TNS_ADMIN={wallet_path}"
df = spark.read.jdbc(url=url, table=table, properties=properties)
```


Write a DataFrame to an Oracle DB table using a Wallet
------------------------------------------------------

```python
# Key variables you need.
# Get the tnsname from tnsnames.ora.
# Wallet path should point to an extracted wallet file.
password = "my_password"
table = "target_table"
tnsname = "my_tns_name"
user = "ADMIN"
wallet_path = "/path/to/your/wallet"

properties = {
    "driver": "oracle.jdbc.driver.OracleDriver",
    "oracle.net.tns_admin": tnsname,
    "password": password,
    "user": user,
}
url = f"jdbc:oracle:thin:@{tnsname}?TNS_ADMIN={wallet_path}"

# Possible modes are "Append", "Overwrite", "Ignore", "Error"
df.write.jdbc(url=url, table=table, mode="Append", properties=properties)
```


DataFrame Operations
====================
Adding, removing and modifying DataFrame columns.

Add a new column with to a DataFrame
------------------------------------

```python
from pyspark.sql.functions import upper, lower

df = df.withColumn("upper", upper(df.carname)).withColumn(
    "lower", lower(df.carname)
)
```
```
# Code snippet result:
+----+---------+------------+----------+------+------------+---------+------+----------+----------+----------+
| mpg|cylinders|displacement|horsepower|weight|acceleration|modelyear|origin|   carname|     upper|     lower|
+----+---------+------------+----------+------+------------+---------+------+----------+----------+----------+
|18.0|        8|       307.0|     130.0| 3504.|        12.0|       70|     1|chevrol...|CHEVROL...|chevrol...|
|15.0|        8|       350.0|     165.0| 3693.|        11.5|       70|     1|buick s...|BUICK S...|buick s...|
|18.0|        8|       318.0|     150.0| 3436.|        11.0|       70|     1|plymout...|PLYMOUT...|plymout...|
|16.0|        8|       304.0|     150.0| 3433.|        12.0|       70|     1|amc reb...|AMC REB...|amc reb...|
|17.0|        8|       302.0|     140.0| 3449.|        10.5|       70|     1|ford to...|FORD TO...|ford to...|
|15.0|        8|       429.0|     198.0| 4341.|        10.0|       70|     1|ford ga...|FORD GA...|ford ga...|
|14.0|        8|       454.0|     220.0| 4354.|         9.0|       70|     1|chevrol...|CHEVROL...|chevrol...|
|14.0|        8|       440.0|     215.0| 4312.|         8.5|       70|     1|plymout...|PLYMOUT...|plymout...|
|14.0|        8|       455.0|     225.0| 4425.|        10.0|       70|     1|pontiac...|PONTIAC...|pontiac...|
|15.0|        8|       390.0|     190.0| 3850.|         8.5|       70|     1|amc amb...|AMC AMB...|amc amb...|
+----+---------+------------+----------+------+------------+---------+------+----------+----------+----------+
only showing top 10 rows
```

Modify a DataFrame column
-------------------------

```python
from pyspark.sql.functions import col, concat, lit

df = df.withColumn("modelyear", concat(lit("19"), col("modelyear")))
```
```
# Code snippet result:
+----+---------+------------+----------+------+------------+---------+------+----------+
| mpg|cylinders|displacement|horsepower|weight|acceleration|modelyear|origin|   carname|
+----+---------+------------+----------+------+------------+---------+------+----------+
|18.0|        8|       307.0|     130.0| 3504.|        12.0|     1970|     1|chevrol...|
|15.0|        8|       350.0|     165.0| 3693.|        11.5|     1970|     1|buick s...|
|18.0|        8|       318.0|     150.0| 3436.|        11.0|     1970|     1|plymout...|
|16.0|        8|       304.0|     150.0| 3433.|        12.0|     1970|     1|amc reb...|
|17.0|        8|       302.0|     140.0| 3449.|        10.5|     1970|     1|ford to...|
|15.0|        8|       429.0|     198.0| 4341.|        10.0|     1970|     1|ford ga...|
|14.0|        8|       454.0|     220.0| 4354.|         9.0|     1970|     1|chevrol...|
|14.0|        8|       440.0|     215.0| 4312.|         8.5|     1970|     1|plymout...|
|14.0|        8|       455.0|     225.0| 4425.|        10.0|     1970|     1|pontiac...|
|15.0|        8|       390.0|     190.0| 3850.|         8.5|     1970|     1|amc amb...|
+----+---------+------------+----------+------+------------+---------+------+----------+
only showing top 10 rows
```

Add a column with multiple conditions
-------------------------------------

```python
from pyspark.sql.functions import col, when

df = df.withColumn(
    "mpg_class",
    when(col("mpg") <= 20, "low")
    .when(col("mpg") <= 30, "mid")
    .when(col("mpg") <= 40, "high")
    .otherwise("very high"),
)
```
```
# Code snippet result:
+----+---------+------------+----------+------+------------+---------+------+----------+---------+
| mpg|cylinders|displacement|horsepower|weight|acceleration|modelyear|origin|   carname|mpg_class|
+----+---------+------------+----------+------+------------+---------+------+----------+---------+
|18.0|        8|       307.0|     130.0| 3504.|        12.0|       70|     1|chevrol...|      low|
|15.0|        8|       350.0|     165.0| 3693.|        11.5|       70|     1|buick s...|      low|
|18.0|        8|       318.0|     150.0| 3436.|        11.0|       70|     1|plymout...|      low|
|16.0|        8|       304.0|     150.0| 3433.|        12.0|       70|     1|amc reb...|      low|
|17.0|        8|       302.0|     140.0| 3449.|        10.5|       70|     1|ford to...|      low|
|15.0|        8|       429.0|     198.0| 4341.|        10.0|       70|     1|ford ga...|      low|
|14.0|        8|       454.0|     220.0| 4354.|         9.0|       70|     1|chevrol...|      low|
|14.0|        8|       440.0|     215.0| 4312.|         8.5|       70|     1|plymout...|      low|
|14.0|        8|       455.0|     225.0| 4425.|        10.0|       70|     1|pontiac...|      low|
|15.0|        8|       390.0|     190.0| 3850.|         8.5|       70|     1|amc amb...|      low|
+----+---------+------------+----------+------+------------+---------+------+----------+---------+
only showing top 10 rows
```

Add a constant column
---------------------

```python
from pyspark.sql.functions import lit

df = df.withColumn("one", lit(1))
```
```
# Code snippet result:
+----+---------+------------+----------+------+------------+---------+------+----------+---+
| mpg|cylinders|displacement|horsepower|weight|acceleration|modelyear|origin|   carname|one|
+----+---------+------------+----------+------+------------+---------+------+----------+---+
|18.0|        8|       307.0|     130.0| 3504.|        12.0|       70|     1|chevrol...|  1|
|15.0|        8|       350.0|     165.0| 3693.|        11.5|       70|     1|buick s...|  1|
|18.0|        8|       318.0|     150.0| 3436.|        11.0|       70|     1|plymout...|  1|
|16.0|        8|       304.0|     150.0| 3433.|        12.0|       70|     1|amc reb...|  1|
|17.0|        8|       302.0|     140.0| 3449.|        10.5|       70|     1|ford to...|  1|
|15.0|        8|       429.0|     198.0| 4341.|        10.0|       70|     1|ford ga...|  1|
|14.0|        8|       454.0|     220.0| 4354.|         9.0|       70|     1|chevrol...|  1|
|14.0|        8|       440.0|     215.0| 4312.|         8.5|       70|     1|plymout...|  1|
|14.0|        8|       455.0|     225.0| 4425.|        10.0|       70|     1|pontiac...|  1|
|15.0|        8|       390.0|     190.0| 3850.|         8.5|       70|     1|amc amb...|  1|
+----+---------+------------+----------+------+------------+---------+------+----------+---+
only showing top 10 rows
```

Concatenate columns
-------------------

```python
from pyspark.sql.functions import concat, col, lit

df = df.withColumn(
    "concatenated", concat(col("cylinders"), lit("_"), col("mpg"))
)
```
```
# Code snippet result:
+----+---------+------------+----------+------+------------+---------+------+----------+------------+
| mpg|cylinders|displacement|horsepower|weight|acceleration|modelyear|origin|   carname|concatenated|
+----+---------+------------+----------+------+------------+---------+------+----------+------------+
|18.0|        8|       307.0|     130.0| 3504.|        12.0|       70|     1|chevrol...|      8_18.0|
|15.0|        8|       350.0|     165.0| 3693.|        11.5|       70|     1|buick s...|      8_15.0|
|18.0|        8|       318.0|     150.0| 3436.|        11.0|       70|     1|plymout...|      8_18.0|
|16.0|        8|       304.0|     150.0| 3433.|        12.0|       70|     1|amc reb...|      8_16.0|
|17.0|        8|       302.0|     140.0| 3449.|        10.5|       70|     1|ford to...|      8_17.0|
|15.0|        8|       429.0|     198.0| 4341.|        10.0|       70|     1|ford ga...|      8_15.0|
|14.0|        8|       454.0|     220.0| 4354.|         9.0|       70|     1|chevrol...|      8_14.0|
|14.0|        8|       440.0|     215.0| 4312.|         8.5|       70|     1|plymout...|      8_14.0|
|14.0|        8|       455.0|     225.0| 4425.|        10.0|       70|     1|pontiac...|      8_14.0|
|15.0|        8|       390.0|     190.0| 3850.|         8.5|       70|     1|amc amb...|      8_15.0|
+----+---------+------------+----------+------+------------+---------+------+----------+------------+
only showing top 10 rows
```

Drop a column
-------------

```python
df = df.drop("horsepower")
```
```
# Code snippet result:
+----+---------+------------+------+------------+---------+------+----------+
| mpg|cylinders|displacement|weight|acceleration|modelyear|origin|   carname|
+----+---------+------------+------+------------+---------+------+----------+
|18.0|        8|       307.0| 3504.|        12.0|       70|     1|chevrol...|
|15.0|        8|       350.0| 3693.|        11.5|       70|     1|buick s...|
|18.0|        8|       318.0| 3436.|        11.0|       70|     1|plymout...|
|16.0|        8|       304.0| 3433.|        12.0|       70|     1|amc reb...|
|17.0|        8|       302.0| 3449.|        10.5|       70|     1|ford to...|
|15.0|        8|       429.0| 4341.|        10.0|       70|     1|ford ga...|
|14.0|        8|       454.0| 4354.|         9.0|       70|     1|chevrol...|
|14.0|        8|       440.0| 4312.|         8.5|       70|     1|plymout...|
|14.0|        8|       455.0| 4425.|        10.0|       70|     1|pontiac...|
|15.0|        8|       390.0| 3850.|         8.5|       70|     1|amc amb...|
+----+---------+------------+------+------------+---------+------+----------+
only showing top 10 rows
```

Change a column name
--------------------

```python
df = df.withColumnRenamed("horsepower", "horses")
```
```
# Code snippet result:
+----+---------+------------+------+------+------------+---------+------+----------+
| mpg|cylinders|displacement|horses|weight|acceleration|modelyear|origin|   carname|
+----+---------+------------+------+------+------------+---------+------+----------+
|18.0|        8|       307.0| 130.0| 3504.|        12.0|       70|     1|chevrol...|
|15.0|        8|       350.0| 165.0| 3693.|        11.5|       70|     1|buick s...|
|18.0|        8|       318.0| 150.0| 3436.|        11.0|       70|     1|plymout...|
|16.0|        8|       304.0| 150.0| 3433.|        12.0|       70|     1|amc reb...|
|17.0|        8|       302.0| 140.0| 3449.|        10.5|       70|     1|ford to...|
|15.0|        8|       429.0| 198.0| 4341.|        10.0|       70|     1|ford ga...|
|14.0|        8|       454.0| 220.0| 4354.|         9.0|       70|     1|chevrol...|
|14.0|        8|       440.0| 215.0| 4312.|         8.5|       70|     1|plymout...|
|14.0|        8|       455.0| 225.0| 4425.|        10.0|       70|     1|pontiac...|
|15.0|        8|       390.0| 190.0| 3850.|         8.5|       70|     1|amc amb...|
+----+---------+------------+------+------+------------+---------+------+----------+
only showing top 10 rows
```

Change multiple column names
----------------------------

```python
df = df.withColumnRenamed("horsepower", "horses").withColumnRenamed(
    "modelyear", "year"
)
```
