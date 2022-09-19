#!/usr/bin/env python3

import argparse
import datetime
import inspect
import logging
import os
import pandas
import pyspark
import shutil
import sys
import yaml
from pyspark.sql import SparkSession, SQLContext
from slugify import slugify

from delta import *

warehouse_path = "file://{}/spark_warehouse".format(os.getcwd())
builder = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.memory", "2G")
    .config("spark.executor.memory", "2G")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", warehouse_path)
    .appName("cheatsheet")
)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=["org.postgresql:postgresql:42.4.0"]
).getOrCreate()
sqlContext = SQLContext(spark)


def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)


def get_result_text(result, truncate=True):
    if type(result) == tuple:
        result_df, options = result
        return getShowString(result_df, **options)
    if type(result) == pyspark.sql.dataframe.DataFrame:
        return getShowString(result, truncate=truncate)
    elif type(result) == pandas.core.frame.DataFrame:
        return str(result)
    elif type(result) == list:
        return "\n".join(result)
    elif type(result) == dict and "image" in result:
        return "![{}]({})".format(result["alt"], result["image"])
    else:
        return result


class snippet:
    def __init__(self):
        self.dataset = None
        self.name = None
        self.preconvert = False
        self.skip_run = False
        self.manual_output = None
        self.truncate = True
        self.requires_environment = False
        self.docmd = None

    def load_data(self):
        assert self.dataset is not None, "Dataset not set"
        if self.dataset == "UNUSED":
            return None
        if self.dataset == "covtype.parquet":
            from pyspark.sql.functions import col

            df = spark.read.format("parquet").load(
                os.path.join("data", "covtype.parquet")
            )
            for column_name in df.columns:
                df = df.withColumn(column_name, col(column_name).cast("int"))
            return df
        df = (
            spark.read.format("csv")
            .option("header", True)
            .load(os.path.join("data", self.dataset))
        )
        if self.preconvert:
            if self.dataset in ("auto-mpg.csv", "auto-mpg-fixed.csv"):
                from pyspark.sql.functions import col

                for (
                    column_name
                ) in (
                    "mpg cylinders displacement horsepower weight acceleration".split()
                ):
                    df = df.withColumn(column_name, col(column_name).cast("double"))
                df = df.withColumn("modelyear", col("modelyear").cast("int"))
                df = df.withColumn("origin", col("origin").cast("int"))
            elif self.dataset == "customer_spend.csv":
                from pyspark.sql.functions import col, to_date, udf
                from pyspark.sql.types import DecimalType
                from decimal import Decimal
                from money_parser import price_str

                money_convert = udf(
                    lambda x: Decimal(price_str(x)) if x is not None else None,
                    DecimalType(8, 4),
                )
                df = (
                    df.withColumn("customer_id", col("customer_id").cast("integer"))
                    .withColumn("spend_dollars", money_convert(df.spend_dollars))
                    .withColumn("date", to_date(df.date))
                )
        return df

    def snippet(self, df):
        assert False, "Snippet not overridden"

    def run(self, show=True):
        assert self.dataset is not None, "Dataset not set"
        assert self.name is not None, "Name not set"
        logging.info("--- {} ---".format(self.name))
        if self.skip_run:
            if self.manual_output:
                result_text = self.manual_output
                if show:
                    logging.info(result_text)
                else:
                    return result_text
            return None
        self.df = self.load_data()
        retval = self.snippet(self.df)
        if show:
            if retval is not None:
                result_text = get_result_text(retval, self.truncate)
                logging.info(result_text)
        else:
            return retval


class loadsave_dataframe_from_csv(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from CSV"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 100
        self.docmd = """
See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html for a list of supported options.
"""

    def snippet(self, df):
        df = spark.read.format("csv").option("header", True).load("data/auto-mpg.csv")
        return df


class loadsave_dataframe_from_csv_delimiter(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from a Tab Separated Value (TSV) file"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 110
        self.docmd = """
See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html for a list of supported options.
"""

    def snippet(self, df):
        df = (
            spark.read.format("csv")
            .option("header", True)
            .option("sep", "\t")
            .load("data/auto-mpg.tsv")
        )
        return df


class loadsave_save_csv(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame in CSV format"
        self.category = "Accessing Data Sources"
        self.dataset = "auto-mpg.csv"
        self.priority = 120
        self.docmd = """
See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html for a list of supported options.
"""

    def snippet(self, auto_df):
        auto_df.write.csv("output.csv")


class loadsave_load_parquet(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from Parquet"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 200
        self.documd = """
Using the parquet format loads parquet files. If the path is a directory, all files in the directory will be combined into one DataFrame. Loading Parquet files does not require the schema to be given.
"""

    def snippet(self, df):
        df = spark.read.format("parquet").load("data/auto-mpg.parquet")
        return df


class loadsave_save_parquet(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame in Parquet format"
        self.category = "Accessing Data Sources"
        self.dataset = "auto-mpg.csv"
        self.priority = 210

    def snippet(self, auto_df):
        auto_df.write.parquet("output.parquet")


class loadsave_read_jsonl(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from JSON Lines (jsonl) Formatted Data"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 300
        self.docmd = """
JSON Lines / jsonl format uses one JSON document per line. If you have data with mostly regular structure this is better than nesting it in an array. See [jsonlines.org](https://jsonlines.org/)
"""

    def snippet(self, df):
        df = spark.read.json("data/weblog.jsonl")
        return df


class loadsave_save_catalog(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame into a Hive catalog table"
        self.category = "Accessing Data Sources"
        self.dataset = "auto-mpg.csv"
        self.priority = 500
        self.docmd = """
Save a DataFrame to a Hive-compatible catalog. Use `table` to save in the session's current database or `database.table` to save
in a specific database.
"""

    def snippet(self, auto_df):
        auto_df.write.mode("overwrite").saveAsTable("autompg")


class loadsave_load_catalog(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a Hive catalog table into a DataFrame"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 510
        self.docmd = """Load a DataFrame from a particular table. Use `table` to load from the session's current database or `database.table` to load from a specific database.
"""

    def snippet(self, df):
        df = spark.table("autompg")
        return df


class loadsave_load_sql(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from a SQL query"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 520
        self.docmd = """
This example shows loading a DataFrame from a query run over the a table in a Hive-compatible catalog.
"""

    def snippet(self, df):
        df = sqlContext.sql(
            "select carname, mpg, horsepower from autompg where horsepower > 100 and mpg > 25"
        )
        return df


class loadsave_read_from_s3(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a CSV file from Amazon S3"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 1000
        self.skip_run = True
        self.docmd = """
This example shows how to load a CSV file from AWS S3. This example uses a credential pair and the `SimpleAWSCredentialsProvider`. For other authentication options, refer to the [Hadoop-AWS module documentation](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html).
"""

    def snippet(self, df):
        import configparser
        import os

        config = configparser.ConfigParser()
        config.read(os.path.expanduser("~/.aws/credentials"))
        access_key = config.get("default", "aws_access_key_id").replace('"', "")
        secret_key = config.get("default", "aws_secret_access_key").replace('"', "")

        # Requires compatible hadoop-aws and aws-java-sdk-bundle JARs.
        spark.conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        spark.conf.set("fs.s3a.access.key", access_key)
        spark.conf.set("fs.s3a.secret.key", secret_key)

        df = (
            spark.read.format("csv")
            .option("header", True)
            .load("s3a://cheatsheet111/auto-mpg.csv")
        )
        return df


class loadsave_read_from_oci(snippet):
    def __init__(self):
        super().__init__()
        self.name = (
            "Load a CSV file from Oracle Cloud Infrastructure (OCI) Object Storage"
        )
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 1300
        self.skip_run = True
        self.docmd = """
This example shows loading data from Oracle Cloud Infrastructure Object Storage using an API key.
"""

    def snippet(self, df):
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
        return df


class loadsave_read_oracle(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Read an Oracle DB table into a DataFrame using a Wallet"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 10000
        self.skip_run = True
        self.docmd = """
Get the tnsname from tnsnames.ora. The wallet path should point to an extracted wallet file. The wallet files need to be available on all nodes.
"""

    def snippet(self, df):
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
        return df


class loadsave_write_oracle(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Write a DataFrame to an Oracle DB table using a Wallet"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 10100
        self.skip_run = True
        self.docmd = """
Get the tnsname from tnsnames.ora. The wallet path should point to an extracted wallet file. The wallet files need to be available on all nodes.
"""

    def snippet(self, df):
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


class loadsave_write_postgres(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Write a DataFrame to a Postgres table"
        self.category = "Accessing Data Sources"
        self.dataset = "auto-mpg.csv"
        self.priority = 11000
        self.requires_environment = True
        self.docmd = """
You need a Postgres JDBC driver to connect to a Postgres database.

Options include:
- Add `org.postgresql:postgresql:<version>` to `spark.jars.packages`
- Provide the JDBC driver using `spark-submit --jars`
- Add the JDBC driver to your Spark runtime (not recommended)

If you use Delta Lake there is a special procedure for specifying `spark.jars.packages`, see the source code that generates this file for details.
"""

    def snippet(self, auto_df):
        pg_database = os.environ.get("PGDATABASE") or "postgres"
        pg_host = os.environ.get("PGHOST") or "localhost"
        pg_password = os.environ.get("PGPASSWORD") or "password"
        pg_user = os.environ.get("PGUSER") or "postgres"
        table = "autompg"

        properties = {
            "driver": "org.postgresql.Driver",
            "user": pg_user,
            "password": pg_password,
        }
        url = f"jdbc:postgresql://{pg_host}:5432/{pg_database}"
        auto_df.write.jdbc(url=url, table=table, mode="Append", properties=properties)


class loadsave_read_postgres(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Read a Postgres table into a DataFrame"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 11010
        self.requires_environment = True
        self.docmd = """
You need a Postgres JDBC driver to connect to a Postgres database.

Options include:
- Add `org.postgresql:postgresql:<version>` to `spark.jars.packages`
- Provide the JDBC driver using `spark-submit --jars`
- Add the JDBC driver to your Spark runtime (not recommended)
"""

    def snippet(self, df):
        pg_database = os.environ.get("PGDATABASE") or "postgres"
        pg_host = os.environ.get("PGHOST") or "localhost"
        pg_password = os.environ.get("PGPASSWORD") or "password"
        pg_user = os.environ.get("PGUSER") or "postgres"
        table = "autompg"

        properties = {
            "driver": "org.postgresql.Driver",
            "user": pg_user,
            "password": pg_password,
        }
        url = f"jdbc:postgresql://{pg_host}:5432/{pg_database}"
        df = spark.read.jdbc(url=url, table=table, properties=properties)
        return df


class loadsave_dataframe_from_csv_provide_schema(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Provide the schema when loading a DataFrame from CSV"
        self.category = "Data Handling Options"
        self.dataset = "UNUSED"
        self.priority = 100
        self.docmd = """
See https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/types.html for a list of types.
"""

    def snippet(self, df):
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
        return df


class loadsave_overwrite_output_directory(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame to CSV, overwriting existing data"
        self.category = "Data Handling Options"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, auto_df):
        auto_df.write.mode("overwrite").csv("output.csv")


class loadsave_csv_with_header(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame to CSV with a header"
        self.category = "Data Handling Options"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.docmd = """
See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html for a list of supported options.
"""

    def snippet(self, auto_df):
        auto_df.coalesce(1).write.csv("header.csv", header="true")


class loadsave_single_output_file(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame in a single CSV file"
        self.category = "Data Handling Options"
        self.dataset = "auto-mpg.csv"
        self.priority = 400
        self.docmd = """
This example outputs CSV data to a single file. The file will be written in a directory called single.csv and have a random name. There is no way to change this behavior.

If you need to write to a single file with a name you choose, consider converting it to a Pandas dataframe and saving it using Pandas.

Either way all data will be collected on one node before being written so be careful not to run out of memory.
"""

    def snippet(self, auto_df):
        auto_df.coalesce(1).write.csv("single.csv")


class loadsave_dynamic_partitioning(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save DataFrame as a dynamic partitioned table"
        self.category = "Data Handling Options"
        self.dataset = "auto-mpg.csv"
        self.priority = 500
        self.docmd = """
When you write using dynamic partitioning, the output partitions are determined bby the values of a column rather than specified in code.

The values of the partitions will appear as subdirectories and are not contained in the output files, i.e. they become "virtual columns". When you read a partition table these virtual columns will be part of the DataFrame.

Dynamic partitioning has the potential to create many small files, this will impact performance negatively. Be sure the partition columns do not have too many distinct values and limit the use of multiple virtual columns.
"""

    def snippet(self, auto_df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        auto_df.write.mode("append").partitionBy("modelyear").saveAsTable(
            "autompg_partitioned"
        )


class loadsave_overwrite_specific_partitions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Overwrite specific partitions"
        self.category = "Data Handling Options"
        self.dataset = "auto-mpg.csv"
        self.priority = 501
        self.skip_run = True
        self.docmd = """
Enabling dynamic partitioning lets you add or overwrite partitions based on DataFrame contents. Without dynamic partitioning the overwrite will overwrite the entire table.

With dynamic partitioning, partitions with keys in the DataFrame are overwritten, but partitions not in the DataFrame are untouched.
"""

    def snippet(self, df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        your_dataframe.write.mode("overwrite").insertInto("your_table")


class loadsave_money(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a CSV file with a money column into a DataFrame"
        self.category = "Data Handling Options"
        self.dataset = "UNUSED"
        self.priority = 600
        self.docmd = """
Spark is not that smart when it comes to parsing numbers, not allowing things like commas. If you need to load monetary amounts the safest option is to use a parsing library like `money_parser`.
"""

    def snippet(self, df):
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
        df = df.withColumn("spend_dollars", money_convert(df.spend_dollars))
        return df


class dfo_modify_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Modify a DataFrame column"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 200
        self.docmd = """
Modify a column in-place using `withColumn`, specifying the output column name to be the same as the existing column name.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, concat, lit

        df = auto_df.withColumn("modelyear", concat(lit("19"), col("modelyear")))
        return df


class dfo_add_column_builtin(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Add a new column to a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 100
        self.docmd = """
`withColumn` returns a new DataFrame with a column added to the source DataFrame. `withColumn` can be chained together multiple times.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import upper, lower

        df = auto_df.withColumn("upper", upper(auto_df.carname)).withColumn(
            "lower", lower(auto_df.carname)
        )
        return df


class dfo_add_column_custom_udf(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Create a custom UDF"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 2100
        self.docmd = """
Create a UDF by providing a function to the udf function. This example shows a [lambda function](https://docs.python.org/3/reference/expressions.html#lambdas-1). You can also use ordinary functions for more complex UDFs.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, udf
        from pyspark.sql.types import StringType

        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = auto_df.withColumn("manufacturer", first_word_udf(col("carname")))
        return df


class dfo_concat_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Concatenate columns"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 450
        self.docmd = """TODO"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import concat, col, lit

        df = auto_df.withColumn(
            "concatenated", concat(col("cylinders"), lit("_"), col("mpg"))
        )
        return df


class dfo_string_to_double(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert String to Double"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1000

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.withColumn("horsepower", col("horsepower").cast("double"))
        return df


class dfo_string_to_integer(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert String to Integer"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1100

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.withColumn("horsepower", col("horsepower").cast("int"))
        return df


class dfo_change_column_name_single(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Change a column name"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 600

    def snippet(self, auto_df):
        df = auto_df.withColumnRenamed("horsepower", "horses")
        return df


class dfo_change_column_name_multi(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Change multiple column names"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 700
        self.docmd = """
If you need to change multiple column names you can chain `withColumnRenamed` calls together. If you want to change all column names see "Change all column names at once".
"""

    def snippet(self, auto_df):
        df = auto_df.withColumnRenamed("horsepower", "horses").withColumnRenamed(
            "modelyear", "year"
        )
        return df


class dfo_change_column_name_all(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Change all column names at once"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 710
        self.docmd = """
To rename all columns use toDF with the desired column names in the argument list. This example puts an X in front of all column names.
"""

    def snippet(self, auto_df):
        df = auto_df.toDF(*["X" + name for name in auto_df.columns])
        return df


class dfo_column_to_python_list(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert a DataFrame column to a Python list"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 710
        self.docmd = """
Steps below:

- `select` the target column, this example uses `carname`.
- Access the DataFrame's rdd using `.rdd`
- Use `flatMap` to convert the rdd's `Row` objects into simple values.
- Use `collect` to assemble everything into a list.
"""

    def snippet(self, auto_df):
        names = auto_df.select("carname").rdd.flatMap(lambda x: x).collect()
        print(str(names[:10]))
        return str(names[:10])


class dfo_consume_as_dict(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Consume a DataFrame row-wise as Python dictionaries"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 730
        self.docmd = """
Steps below:

- `collect` all DataFrame Rows in the driver.
- Iterate over the Rows.
- Call the Row's `asDict` method to convert the Row to a Python dictionary.
"""

    def snippet(self, auto_df):
        first_three = auto_df.limit(3)
        for row in first_three.collect():
            my_dict = row.asDict()
            print(my_dict)
            # EXCLUDE
            return """
{'mpg': '18.0', 'cylinders': '8', 'displacement': '307.0', 'horsepower': '130.0', 'weight': '3504.', 'acceleration': '12.0', 'modelyear': '70', 'origin': '1', 'carname': 'chevrolet chevelle malibu'}
{'mpg': '15.0', 'cylinders': '8', 'displacement': '350.0', 'horsepower': '165.0', 'weight': '3693.', 'acceleration': '11.5', 'modelyear': '70', 'origin': '1', 'carname': 'buick skylark 320'}
{'mpg': '18.0', 'cylinders': '8', 'displacement': '318.0', 'horsepower': '150.0', 'weight': '3436.', 'acceleration': '11.0', 'modelyear': '70', 'origin': '1', 'carname': 'plymouth satellite'}
"""
            # INCLUDE


class dfo_scalar_query_to_python_variable(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert a scalar query to a Python value"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 720
        self.docmd = """
If you have a `DataFrame` with one row and one column, how do you access its value?

Steps below:
- Create a DataFrame with one row and one column, this example uses an average but it could be anything.
- Call the DataFrame's `first` method, this returns the first `Row` of the DataFrame.
- `Row`s can be accessed like arrays, so we extract the zeroth value of the first `Row` using `first()[0]`.
"""

    def snippet(self, auto_df):
        average = auto_df.agg(dict(mpg="avg")).first()[0]
        print(str(average))
        return str(average)


class dfo_dataframe_from_rdd(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert an RDD to Data Frame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 1500
        self.docmd = """
If you have an `rdd` how do you convert it to a `DataFrame`? The `rdd` method `toDf` can be used, but the `rdd` must be a collection of `Row` objects.

Steps below:
- Create an `rdd` to be converted to a `DataFrame`.
- Use the `rdd`'s `map` method:
  - The example uses a lambda function to convert `rdd` elements to `Row`s.
  - The `Row` constructor request key/value pairs with the key serving as the "column name".
  - Each `rdd` entry is converted to a dictionary and the dictionary is unpacked to create the `Row`.
  - `map` creates a new `rdd` containing all the `Row` objects.
  - This new `rdd` is converted to a `DataFrame` using the `toDF` method.

The second example is a variation on the first, modifying source `rdd` entries while creating the target `rdd`.
"""

    def snippet(self, auto_df):
        from pyspark.sql import Row

        # First, get the RDD from the DataFrame.
        rdd = auto_df.rdd

        # This converts it back to an RDD with no changes.
        df = rdd.map(lambda x: Row(**x.asDict())).toDF()

        # This changes the rows before creating the DataFrame.
        df = rdd.map(
            lambda x: Row(**{k: v * 2 for (k, v) in x.asDict().items()})
        ).toDF()
        return df


class dfo_empty_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Create an empty dataframe with a specified schema"
        self.category = "DataFrame Operations"
        self.dataset = "NA"
        self.priority = 900
        self.docmd = """
You can create an empty `DataFrame` the same way you create other in-line `DataFrame`s, but using an empty list.
"""

    def load_data(self):
        pass

    def snippet(self, rdd):
        from pyspark.sql.types import StructField, StructType, LongType, StringType

        schema = StructType(
            [
                StructField("my_id", LongType(), True),
                StructField("my_string", StringType(), True),
            ]
        )
        df = spark.createDataFrame([], schema)
        return df


class dfo_drop_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Drop a column"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, auto_df):
        df = auto_df.drop("horsepower")
        return df


class dfo_print_contents_rdd(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Print the contents of an RDD"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1600
        self.docmd = """
To see an `RDD`'s contents, convert the output of the `take` method to a string.
"""

    def snippet(self, auto_df):
        rdd = auto_df.rdd
        print(rdd.take(10))
        return str(rdd.take(10))


class dfo_print_contents_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Print the contents of a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1700

    def snippet(self, auto_df):
        auto_df.show(10)
        return auto_df


class dfo_column_conditional(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Add a column with multiple conditions"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.docmd = """
To set a new column's values when using `withColumn`, use the `when` / `otherwise` idiom. Multiple `when` conditions can be chained together.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, when

        df = auto_df.withColumn(
            "mpg_class",
            when(col("mpg") <= 20, "low")
            .when(col("mpg") <= 30, "mid")
            .when(col("mpg") <= 40, "high")
            .otherwise("very high"),
        )
        return df


class dfo_constant_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Add a constant column"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, auto_df):
        from pyspark.sql.functions import lit

        df = auto_df.withColumn("one", lit(1))
        return df


class dfo_foreach(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Process each row of a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1800
        self.docmd = """
Use the `foreach` function to process each row of a `DataFrame` using a Python function. The function will get one argument, a `Row` object. The `Row` will have properties whose names map to the `DataFrame`'s columns.
"""

    def snippet(self, auto_df):
        import os

        def foreach_function(row):
            if row.horsepower is not None:
                os.system("echo " + row.horsepower)

        auto_df.foreach(foreach_function)


class dfo_map(snippet):
    def __init__(self):
        super().__init__()
        self.name = "DataFrame Map example"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1900
        self.docmd = """
You can run `map` on a `DataFrame` by accessing its underlying `RDD`. It is much more common to use `foreach` directly on the `DataFrame` itself. This can be useful if you have code written specifically for `RDD`s that you need to use against a `DataFrame`.
"""

    def snippet(self, auto_df):
        def map_function(row):
            if row.horsepower is not None:
                return [float(row.horsepower) * 10]
            else:
                return [None]

        df = auto_df.rdd.map(map_function).toDF()
        return df


class dfo_flatmap(snippet):
    def __init__(self):
        super().__init__()
        self.name = "DataFrame Flatmap example"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 2000
        self.docmd = """
Use `flatMap` when you have a UDF that produces a list of `Rows` per input `Row`. `flatMap` is an `RDD` operation so we need to access the `DataFrame`'s `RDD`, call `flatMap` and convert the resulting `RDD` back into a `DataFrame`. Spark will handle "flatting" arrays into the output `RDD`.

Note also that you can [`yield`](https://docs.python.org/3/reference/expressions.html#yield-expressions) results rather than returning full lists which can simplify code considerably.
"""

    def snippet(self, auto_df):
        from pyspark.sql.types import Row

        def flatmap_function(row):
            if row.cylinders is not None:
                return list(range(int(row.cylinders)))
            else:
                return [None]

        rdd = auto_df.rdd.flatMap(flatmap_function)
        row = Row("val")
        df = rdd.map(row).toDF()
        return df


class dfo_constant_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Create a constant dataframe"
        self.category = "DataFrame Operations"
        self.dataset = "UNUSED"
        self.priority = 950
        self.docmd = """
Constant `DataFrame`s are mostly useful for unit tests.
"""

    def snippet(self, df):
        import datetime
        from pyspark.sql.types import (
            StructField,
            StructType,
            LongType,
            StringType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("my_id", LongType(), True),
                StructField("my_string", StringType(), True),
                StructField("my_timestamp", TimestampType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                (1, "foo", datetime.datetime.strptime("2021-01-01", "%Y-%m-%d")),
                (2, "bar", datetime.datetime.strptime("2021-01-02", "%Y-%m-%d")),
            ],
            schema,
        )
        return df


class dfo_select_particular(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Select particular columns from a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 800

    def snippet(self, auto_df):
        df = auto_df.select(["mpg", "cylinders", "displacement"])
        return df


class dfo_size(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get the size of a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1200
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        print("{} rows".format(auto_df.count()))
        print("{} columns".format(len(auto_df.columns)))
        # EXCLUDE
        return [
            "{} rows".format(auto_df.count()),
            "{} columns".format(len(auto_df.columns)),
        ]
        # INCLUDE


class dfo_get_number_partitions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get a DataFrame's number of partitions"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1300

    def snippet(self, auto_df):
        print("{} partition(s)".format(auto_df.rdd.getNumPartitions()))
        return "{} partition(s)".format(auto_df.rdd.getNumPartitions())


class dfo_get_dtypes(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get data types of a DataFrame's columns"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1400

    def snippet(self, auto_df):
        print(auto_df.dtypes)
        return str(auto_df.dtypes)


class group_max_value(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get the maximum of a column"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, max

        df = auto_df.select(max(col("horsepower")).alias("max_horsepower"))
        return df


class group_filter_on_count(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Group by then filter on the count"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1100

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.groupBy("cylinders").count().where(col("count") > 100)
        return df


class group_topn_per_group(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Find the top N per row group (use N=1 for maximum)"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1200
        self.docmd = """
To find the top N per group we:

* Build a `Window`
* Partition by the target group
* Order by the value we want to rank
* Use `row_number` to add the numeric rank
* Use `where` to filter any row number less than or equal to N
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, row_number
        from pyspark.sql.window import Window

        # To get the maximum per group, set n=1.
        n = 5
        w = Window().partitionBy("cylinders").orderBy(col("horsepower").desc())
        df = (
            auto_df.withColumn("horsepower", col("horsepower").cast("double"))
            .withColumn("rn", row_number().over(w))
            .where(col("rn") <= n)
            .select("*")
        )
        return df


class group_basic_ntile(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute global percentiles"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 1500
        self.docmd = """
The `ntile` function computes percentiles. Specify how many with an integer argument, for example use 4 to compute quartiles.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, ntile
        from pyspark.sql.window import Window

        w = Window().orderBy(col("mpg").desc())
        df = auto_df.withColumn("ntile4", ntile(4).over(w))
        return df


class group_ntile_partition(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute percentiles within a partition"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 1510
        self.docmd = """
If you need to compute partition-wise percentiles, for example percentiles broken down by years, add `partitionBy` to your `Window`.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, ntile
        from pyspark.sql.window import Window

        w = Window().partitionBy("cylinders").orderBy(col("mpg").desc())
        df = auto_df.withColumn("ntile4", ntile(4).over(w))
        return df


class group_ntile_after_aggregate(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute percentiles after aggregating"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 1520
        self.docmd = """
If you need to compute percentiles of an aggregate, for example ranking averages, compute the aggregate in a second `DataFrame`, then compute percentiles.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, ntile
        from pyspark.sql.window import Window

        grouped = auto_df.groupBy("modelyear").count()
        w = Window().orderBy(col("count").desc())
        df = grouped.withColumn("ntile4", ntile(4).over(w))
        return df


class group_filter_less_than_percentile(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter rows with values below a target percentile"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 1530
        self.docmd = """
To filter out all rows with a value outside a target percentile range:
* Get the numeric percentile value using the `percentile` function and extracting it from the resulting `DataFrame`.
* In a second step filter anything larger than (or smaller than, depending on what you want) that value.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, lit
        import pyspark.sql.functions as F

        target_percentile = auto_df.agg(
            F.expr("percentile(mpg, 0.9)").alias("target_percentile")
        ).first()[0]
        df = auto_df.filter(col("mpg") > lit(target_percentile))
        return df


class group_rollup(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Aggregate and rollup"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 1600
        self.docmd = """
`rollup` functions like `groupBy` but produces additional summary rows. Specify the grouping sets just like you do with `groupBy`.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import avg, col, count, desc

        subset = auto_df.filter(col("modelyear") > 79)
        df = (
            subset.rollup("modelyear", "cylinders")
            .agg(
                avg("horsepower").alias("avg_horsepower"),
                count("modelyear").alias("count"),
            )
            .orderBy(desc("modelyear"), desc("cylinders"))
        )
        # EXCLUDE
        options = dict(n=25)
        # INCLUDE
        return (df, options)


class group_cube(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Aggregate and cube"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 1610
        self.docmd = """
`cube` functions like `groupBy` but produces additional summary rows. Specify the grouping sets just like you do with `groupBy`.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import avg, col, count, desc

        subset = auto_df.filter(col("modelyear") > 79)
        df = (
            subset.cube("modelyear", "cylinders")
            .agg(
                avg("horsepower").alias("avg_horsepower"),
                count("modelyear").alias("count"),
            )
            .orderBy(desc("modelyear"), desc("cylinders"))
        )
        # EXCLUDE
        options = dict(n=25)
        # INCLUDE
        return (df, options)


class group_count_unique_after_group(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Count unique after grouping"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 900

    def snippet(self, auto_df):
        from pyspark.sql.functions import countDistinct

        df = auto_df.groupBy("cylinders").agg(countDistinct("mpg"))
        return df


class group_sum_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Sum a column"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 700

    def snippet(self, auto_df):
        from pyspark.sql.functions import sum

        df = auto_df.groupBy("cylinders").agg(sum("weight").alias("total_weight"))
        return df


class group_sum_columns_no_group(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Sum a list of columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 600
        self.docmd = """
The `agg` method allows you to easily run multiple aggregations by accepting a dictionary with keys being the column name and values being the aggregation type. This example uses this to sum 3 columns in one expression.
"""

    def snippet(self, auto_df):
        exprs = {x: "sum" for x in ("weight", "cylinders", "mpg")}
        df = auto_df.agg(exprs)
        return df


class group_histogram(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute a histogram"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1400
        self.docmd = """
Spark's `RDD` object supports computing histograms. This example computes the DataFrame column called horsepower to an RDD before calling `histogram`.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        # Target column must be numeric.
        df = auto_df.withColumn("horsepower", col("horsepower").cast("double"))

        # N is the number of bins.
        N = 11
        histogram = df.select("horsepower").rdd.flatMap(lambda x: x).histogram(N)
        print(histogram)
        return str(histogram)


class group_key_value_to_key_list(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Group key/values into a list"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1300
        self.docmd = """
The [`collect_list`](https://spark.apache.org/docs/latest/api/sql/index.html#collect_list) function returns an `ArrayType` column containing all values seen per grouping key. The array entries are not unique, you can use `collect_set` if you need unique values.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, collect_list

        df = auto_df.groupBy("cylinders").agg(
            collect_list(col("carname")).alias("models")
        )
        return df


class group_group_and_count(snippet):
    def __init__(self):
        super().__init__()
        self.name = "count(*) on a particular column"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 100
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        from pyspark.sql.functions import desc

        # No sorting.
        df = auto_df.groupBy("cylinders").count()

        # With sorting.
        df = auto_df.groupBy("cylinders").count().orderBy(desc("count"))
        return df


class group_group_and_sort(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Group and sort"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 110
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        from pyspark.sql.functions import avg, desc

        df = (
            auto_df.groupBy("cylinders")
            .agg(avg("horsepower").alias("avg_horsepower"))
            .orderBy(desc("avg_horsepower"))
        )
        return df


class group_group_having(snippet):
    def __init__(self):
        super().__init__()
        self.name = (
            "Filter groups based on an aggregate value, equivalent to SQL HAVING clause"
        )
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 120
        self.docmd = """
To filter values after an aggregation simply use `.filter` on the `DataFrame` after the aggregate, using the column name the aggregate generates.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, desc

        df = (
            auto_df.groupBy("cylinders")
            .count()
            .orderBy(desc("count"))
            .filter(col("count") > 100)
        )
        return df


class group_multiple_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Group by multiple columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 200
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        from pyspark.sql.functions import avg, desc

        df = (
            auto_df.groupBy(["modelyear", "cylinders"])
            .agg(avg("horsepower").alias("avg_horsepower"))
            .orderBy(desc("avg_horsepower"))
        )
        return df


class group_agg_multiple_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Aggregate multiple columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.docmd = """
The `agg` method allows you to easily run multiple aggregations by accepting a dictionary with keys being the column name and values being the aggregation type. This example uses this to aggregate 3 columns in one expression.
"""

    def snippet(self, auto_df):
        expressions = dict(horsepower="avg", weight="max", displacement="max")
        df = auto_df.groupBy("modelyear").agg(expressions)
        return df


class group_order_multiple_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Aggregate multiple columns with custom orderings"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 400
        self.docmd = """
If you want to specify sort columns you have two choices:
* You can chain `orderBy` operations.
* You can take advantage of `orderBy`'s support for multiple arguments.

`orderBy` doesn't accept a list. If you need to build orderings dynamically put them in a list and splat them into `orderBy`'s arguments like in the example below.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import asc, desc_nulls_last

        expressions = dict(horsepower="avg", weight="max", displacement="max")
        orderings = [
            desc_nulls_last("max(displacement)"),
            desc_nulls_last("avg(horsepower)"),
            asc("max(weight)"),
        ]
        df = auto_df.groupBy("modelyear").agg(expressions).orderBy(*orderings)
        return df


class group_distinct_all_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Count distinct values on all columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1000

    def snippet(self, auto_df):
        from pyspark.sql.functions import countDistinct

        df = auto_df.agg(*(countDistinct(c) for c in auto_df.columns))
        return df


class group_aggregate_all_numerics(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Aggregate all numeric columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 800
        self.docmd = """
`DataFrames` store the data types of each column in a property called `dtypes`. This example computes the sum of all numeric columns.
"""

    def snippet(self, auto_df_fixed):
        numerics = set(["decimal", "double", "float", "integer", "long", "short"])
        exprs = {x[0]: "sum" for x in auto_df_fixed.dtypes if x[1] in numerics}
        df = auto_df_fixed.agg(exprs)
        return df


class sortsearch_distinct_values(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get distinct values of a column"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 1000

    def snippet(self, auto_df):
        df = auto_df.select("cylinders").distinct()
        return df


class sortsearch_string_match(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get Dataframe rows that match a substring"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, auto_df):
        df = auto_df.where(auto_df.carname.contains("custom"))
        return df


class sortsearch_string_contents(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter a Dataframe based on a custom substring search"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 510

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.where(col("carname").like("%custom%"))
        return df


class sortsearch_in_list(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter based on an IN list"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.where(col("cylinders").isin(["4", "6"]))
        return df


class sortsearch_not_in_list(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter based on a NOT IN list"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.where(~col("cylinders").isin(["4", "6"]))
        return df


class sortsearch_in_list_from_df(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter values based on keys in another DataFrame"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 410
        self.preconvert = True
        self.docmd = """
If you have `DataFrame` 1 containing values you want to remove from `DataFrame` 2, join them using the `left_anti` join strategy.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        # Our DataFrame of keys to exclude.
        exclude_keys = auto_df.select(
            (col("modelyear") + 1).alias("adjusted_year")
        ).distinct()

        # The anti join returns only keys with no matches.
        filtered = auto_df.join(
            exclude_keys,
            how="left_anti",
            on=auto_df.modelyear == exclude_keys.adjusted_year,
        )

        # Alternatively we can register a temporary table and use a SQL expression.
        exclude_keys.registerTempTable("exclude_keys")
        df = auto_df.filter(
            "modelyear not in ( select adjusted_year from exclude_keys )"
        )
        return df


class sortsearch_column_length(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter based on a column's length"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 600

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, length

        df = auto_df.where(length(col("carname")) < 12)
        return df


class sortsearch_equality(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter based on a specific column value"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.where(col("cylinders") == "8")
        return df


class sortsearch_sort_descending(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Sort DataFrame by a column"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 800
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.orderBy("carname")
        df = auto_df.orderBy(col("carname").desc())
        return df


class sortsearch_first_1k_rows(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Take the first N rows of a DataFrame"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 900
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        n = 10
        df = auto_df.limit(n)
        return df


class sortsearch_multi_filter(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Multiple filter conditions"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 700
        self.docmd = """
The key thing to remember if you have multiple filter conditions is that `filter` accepts standard Python expressions. Use bitwise operators to handle and/or conditions.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        # OR
        df = auto_df.filter((col("mpg") > "30") | (col("acceleration") < "10"))
        # AND
        df = auto_df.filter((col("mpg") > "30") & (col("acceleration") < "13"))
        return df


class sortsearch_remove_duplicates(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Remove duplicates"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 1100
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        df = auto_df.dropDuplicates(["carname"])
        return df


class sortsearch_filtering_basic(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter a column using a condition"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 100
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.filter(col("mpg") > "30")
        return df


class transform_sql(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Run a SparkSQL Statement on a DataFrame"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 100
        self.docmd = """
You can run arbitrary SQL statements on a `DataFrame` provided you:

1. Register the `DataFrame` as a temporary table using `registerTempTable`.
2. Use `sqlContext.sql` and use the temp table name you specified as the table source.

You can also join `DataFrames` if you register them. If you're porting complex SQL from another application this can be a lot easier than converting it to use `DataFrame` SQL APIs.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, regexp_extract

        auto_df.registerTempTable("auto_df")
        df = sqlContext.sql(
            "select modelyear, avg(mpg) from auto_df group by modelyear"
        )
        return df


class transform_regexp_extract(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Extract data from a string using a regular expression"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 110
        self.truncate = False
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, regexp_extract

        group = 0
        df = (
            auto_df.withColumn(
                "identifier", regexp_extract(col("carname"), "(\S?\d+)", group)
            )
            .drop("acceleration")
            .drop("cylinders")
            .drop("displacement")
            .drop("modelyear")
            .drop("mpg")
            .drop("origin")
            .drop("horsepower")
            .drop("weight")
        )
        return df


class transform_fillna_specific_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Fill NULL values in specific columns"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 120

    def snippet(self, auto_df):
        df = auto_df.fillna({"horsepower": 0})
        return df


class transform_fillna_col_avg(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Fill NULL values with column average"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, auto_df):
        from pyspark.sql.functions import avg

        df = auto_df.fillna({"horsepower": auto_df.agg(avg("horsepower")).first()[0]})
        return df


class transform_fillna_group_avg(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Fill NULL values with group average"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.docmd = """
Sometimes NULL values in a column cause problems and it's better to guess at a value than leave it NULL. There are several strategies for doing with this. This example shows replacing NULL values with the average value within that column.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import coalesce

        unmodified_columns = auto_df.columns
        unmodified_columns.remove("horsepower")
        manufacturer_avg = auto_df.groupBy("cylinders").agg({"horsepower": "avg"})
        df = auto_df.join(manufacturer_avg, "cylinders").select(
            *unmodified_columns,
            coalesce("horsepower", "avg(horsepower)").alias("horsepower"),
        )
        return df


class transform_json_to_key_value(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Unpack a DataFrame's JSON column to a new DataFrame"
        self.category = "Transforming Data"
        self.dataset = "UNUSED"
        self.priority = 700
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, json_tuple

        source = spark.sparkContext.parallelize(
            [["1", '{ "a" : 10, "b" : 11 }'], ["2", '{ "a" : 20, "b" : 21 }']]
        ).toDF(["id", "json"])
        df = source.select("id", json_tuple(col("json"), "a", "b"))
        return df


class transform_query_json_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Query a JSON column"
        self.category = "Transforming Data"
        self.dataset = "UNUSED"
        self.priority = 800
        self.docmd = """
If you have JSON text data embedded in a String column, `json_tuple` will parse that text and extract fields within the JSON text.
"""

    def snippet(self, df):
        from pyspark.sql.functions import col, json_tuple

        source = spark.sparkContext.parallelize(
            [["1", '{ "a" : 10, "b" : 11 }'], ["2", '{ "a" : 20, "b" : 21 }']]
        ).toDF(["id", "json"])
        df = (
            source.select("id", json_tuple(col("json"), "a", "b"))
            .withColumnRenamed("c0", "a")
            .withColumnRenamed("c1", "b")
            .where(col("b") > 15)
        )
        return df


class join_concatenate(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Concatenate two DataFrames"
        self.category = "Joining DataFrames"
        self.dataset = "UNUSED"
        self.priority = 500
        self.docmd = """
Spark's union operator is similar to SQL UNION ALL.
"""

    def snippet(self, df):
        df1 = spark.read.format("csv").option("header", True).load("data/part1.csv")
        df2 = spark.read.format("csv").option("header", True).load("data/part2.csv")
        df = df1.union(df2)
        return df


class join_basic(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Join two DataFrames by column name"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 100
        self.docmd = """
The second argument to `join` can be a string if that column name exists in both DataFrames.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        # Load a list of manufacturer / country pairs.
        countries = (
            spark.read.format("csv")
            .option("header", True)
            .load("data/manufacturers.csv")
        )

        # Add a manufacturers column, to join with the manufacturers list.
        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = auto_df.withColumn("manufacturer", first_word_udf(auto_df.carname))

        # The actual join.
        df = df.join(countries, "manufacturer")
        return df


class join_basic2(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Join two DataFrames with an expression"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 200
        self.docmd = """
The boolean expression given to `join` determines the matching condition.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        # Load a list of manufacturer / country pairs.
        countries = (
            spark.read.format("csv")
            .option("header", True)
            .load("data/manufacturers.csv")
        )

        # Add a manufacturers column, to join with the manufacturers list.
        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = auto_df.withColumn("manufacturer", first_word_udf(auto_df.carname))

        # The actual join.
        df = df.join(countries, df.manufacturer == countries.manufacturer)
        return df


class join_multiple_files_single_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load multiple files into a single DataFrame"
        self.category = "Joining DataFrames"
        self.dataset = "UNUSED"
        self.priority = 600
        self.docmd = """
If you have a collection of files you need to load into one DataFrame, it's more efficient to load them all rather than union a collection of DataFrames together.

Ways to do this include:
- If you load a directory, Spark attempts to combine all files in that director into one DataFrame.
- You can pass a list of paths to the `load` function.
- You can pass wildcards to the `load` function.
"""

    def snippet(self, auto_df):
        # Approach 1: Use a list.
        df = (
            spark.read.format("csv")
            .option("header", True)
            .load(["data/part1.csv", "data/part2.csv"])
        )

        # Approach 2: Use a wildcard.
        df = spark.read.format("csv").option("header", True).load("data/part*.csv")
        return df


class join_multiple_conditions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Multiple join conditions"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.docmd = """
If you need to join on multiple conditions, combine them with bitwise operators in the join expression. It's worth noting that most Python boolean expressions can be used as the join expression.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        # Load a list of manufacturer / country pairs.
        countries = (
            spark.read.format("csv")
            .option("header", True)
            .load("data/manufacturers.csv")
        )

        # Add a manufacturers column, to join with the manufacturers list.
        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = auto_df.withColumn("manufacturer", first_word_udf(auto_df.carname))

        # The actual join.
        df = df.join(
            countries,
            (df.manufacturer == countries.manufacturer)
            | (df.mpg == countries.manufacturer),
        )
        return df


class join_subtract_dataframes(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Subtract DataFrames"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 700
        self.docmd = """
Spark's `subtract` operator is similar to SQL's `MINUS` operator.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.subtract(auto_df.where(col("mpg") < "25"))
        return df


class join_different_types(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Various Spark join types"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 400
        self.docmd = """
This snippet shows how to use the various join strategies Spark supports. By default the join type is inner. See the diagram at the top of this section for a visual comparison of what these will produce.
"""

    def snippet(self, auto_df):
        # Inner join on one column.
        joined = auto_df.join(auto_df, "carname")

        # Left (outer) join.
        joined = auto_df.join(auto_df, "carname", "left")

        # Left anti (not in) join.
        joined = auto_df.join(auto_df, "carname", "left_anti")

        # Right (outer) join.
        joined = auto_df.join(auto_df, "carname", "right")

        # Full join.
        joined = auto_df.join(auto_df, "carname", "full")

        # Cross join.
        df = auto_df.crossJoin(auto_df)
        return df


class dates_string_to_date(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert an ISO 8601 formatted date string to date type"
        self.category = "Dealing with Dates"
        self.dataset = "UNUSED"
        self.priority = 100
        self.docmd = "IGNORE"

    def snippet(self, df):
        from pyspark.sql.functions import col

        df = spark.sparkContext.parallelize([["2021-01-01"], ["2022-01-01"]]).toDF(
            ["date_col"]
        )
        df = df.withColumn("date_col", col("date_col").cast("date"))
        return df


class dates_string_to_date_custom(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert a custom formatted date string to date type"
        self.category = "Dealing with Dates"
        self.dataset = "UNUSED"
        self.priority = 200
        self.docmd = "IGNORE"

    def snippet(self, df):
        from pyspark.sql.functions import col, to_date

        df = spark.sparkContext.parallelize([["20210101"], ["20220101"]]).toDF(
            ["date_col"]
        )
        df = df.withColumn("date_col", to_date(col("date_col"), "yyyyddMM"))
        return df


class dates_last_day_of_month(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get the last day of the current month"
        self.category = "Dealing with Dates"
        self.dataset = "UNUSED"
        self.priority = 300
        self.docmd = "IGNORE"

    def snippet(self, df):
        from pyspark.sql.functions import col, last_day

        df = spark.sparkContext.parallelize([["2020-01-01"], ["1712-02-10"]]).toDF(
            ["date_col"]
        )
        df = df.withColumn("date_col", col("date_col").cast("date")).withColumn(
            "last_day", last_day(col("date_col"))
        )
        return df


class dates_unix_timestamp_to_date(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert UNIX (seconds since epoch) timestamp to date"
        self.category = "Dealing with Dates"
        self.dataset = "UNUSED"
        self.priority = 1000
        self.docmd = "IGNORE"

    def snippet(self, df):
        from pyspark.sql.functions import col, from_unixtime

        df = spark.sparkContext.parallelize([["1590183026"], ["2000000000"]]).toDF(
            ["ts_col"]
        )
        df = df.withColumn("date_col", from_unixtime(col("ts_col")))
        return df


class dates_complexdate(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a CSV file with complex dates into a DataFrame"
        self.category = "Dealing with Dates"
        self.dataset = "UNUSED"
        self.priority = 1100
        self.docmd = """
Spark's ability to deal with date formats is poor relative to most databases. You can fill in the gap using the `dateparser` Python add-in which is able to figure out most common date formats.
"""

    def snippet(self, df):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import TimestampType
        import dateparser

        # Use the dateparser module to convert many formats into timestamps.
        date_convert = udf(
            lambda x: dateparser.parse(x) if x is not None else None, TimestampType()
        )
        df = (
            spark.read.format("csv")
            .option("header", True)
            .load("data/date_examples.csv")
        )
        df = df.withColumn("parsed", date_convert(df.date))
        return df


class unstructured_json_top_level_map(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Flatten top level text fields in a JSONl document"
        self.category = "Unstructured Analytics"
        self.dataset = "UNUSED"
        self.priority = 100
        self.docmd = """
When Spark loads JSON data into a `DataFrame`, the `DataFrame`'s columns are complex types (`StructType` and `ArrayType`) representing the JSON object.

Spark allows you to traverse complex types in a select operation by providing multiple `StructField` names separated by a `.`.  Names used to in `StructField`s will correspond to the JSON member names.

For example, if you load this document:
```
{
  "Image": {
    "Width":  800,
    "Height": 600,
    "Title":  "View from 15th Floor",
    "Thumbnail": {
       "Url":    "http://www.example.com/image/481989943",
       "Height": 125,
       "Width":  "100"
    },
    "IDs": [116, 943, 234, 38793]
  }
}
```

The resulting DataFrame will have one StructType column named Image. The Image column will have these selectable fields: `Image.Width`, `Image.Height`, `Image.Title`, `Image.Thumbnail.Url`, `Image.Thumbnail.Height`, `Image.Thumbnail.Width`, `Image.IDs`.
"""

    def snippet(self, df):
        from pyspark.sql.functions import col

        # Load JSONl into a DataFrame. Schema is inferred automatically.
        base = spark.read.json("data/financial.jsonl")

        # Extract interesting fields. Alias keeps columns readable.
        target_json_fields = [
            col("symbol").alias("symbol"),
            col("quoteType.longName").alias("longName"),
            col("price.marketCap.raw").alias("marketCap"),
            col("summaryDetail.previousClose.raw").alias("previousClose"),
            col("summaryDetail.fiftyTwoWeekHigh.raw").alias("fiftyTwoWeekHigh"),
            col("summaryDetail.fiftyTwoWeekLow.raw").alias("fiftyTwoWeekLow"),
            col("summaryDetail.trailingPE.raw").alias("trailingPE"),
        ]
        df = base.select(target_json_fields)
        return df


class unstructured_json_column_map(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Flatten top level text fields from a JSON column"
        self.category = "Unstructured Analytics"
        self.dataset = "UNUSED"
        self.priority = 110
        self.docmd = """
If you have JSON text in a DataFrame's column, you can parse that column into its own DataFrame as follows.
"""

    def snippet(self, df):
        from pyspark.sql.functions import col, from_json, schema_of_json

        # quote/escape options needed when loading CSV containing JSON.
        base = (
            spark.read.format("csv")
            .option("header", True)
            .option("quote", '"')
            .option("escape", '"')
            .load("data/financial.csv")
        )

        # Infer JSON schema from one entry in the DataFrame.
        sample_json_document = base.select("financial_data").first()[0]
        schema = schema_of_json(sample_json_document)

        # Parse using this schema.
        parsed = base.withColumn("parsed", from_json("financial_data", schema))

        # Extract interesting fields.
        target_json_fields = [
            col("parsed.symbol").alias("symbol"),
            col("parsed.quoteType.longName").alias("longName"),
            col("parsed.price.marketCap.raw").alias("marketCap"),
            col("parsed.summaryDetail.previousClose.raw").alias("previousClose"),
            col("parsed.summaryDetail.fiftyTwoWeekHigh.raw").alias("fiftyTwoWeekHigh"),
            col("parsed.summaryDetail.fiftyTwoWeekLow.raw").alias("fiftyTwoWeekLow"),
            col("parsed.summaryDetail.trailingPE.raw").alias("trailingPE"),
        ]
        df = parsed.select(target_json_fields)
        return df


class unstructured_json_unnest_complex_array(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Unnest an array of complex structures"
        self.category = "Unstructured Analytics"
        self.dataset = "UNUSED"
        self.priority = 200
        self.docmd = """
When you need to access JSON array elements you will usually use the table generating functions [explode](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html#pyspark.sql.functions.explode) or [posexplode](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.posexplode.html#pyspark.sql.functions.posexplode).

`explode` and `posexplode` are called table generating functions because they produce one output row for each array entry, in other words a row goes in and a table comes out. The output column has the same data type as the data type in the array. When dealing with JSON this data type could be a boolean, integer, float or StructType.

The example below uses `explode` to flatten an array of StructTypes, then selects certain key fields from the output structures.
"""

    def snippet(self, df):
        from pyspark.sql.functions import col, explode

        base = spark.read.json("data/financial.jsonl")

        # Analyze balance sheet data, which is held in an array of complex types.
        target_json_fields = [
            col("symbol").alias("symbol"),
            col("balanceSheetHistoryQuarterly.balanceSheetStatements").alias(
                "balanceSheetStatements"
            ),
        ]
        selected = base.select(target_json_fields)

        # Select a few fields from the balance sheet statement data.
        target_json_fields = [
            col("symbol").alias("symbol"),
            col("col.endDate.fmt").alias("endDate"),
            col("col.cash.raw").alias("cash"),
            col("col.totalAssets.raw").alias("totalAssets"),
            col("col.totalLiab.raw").alias("totalLiab"),
        ]

        # Balance sheet data is in an array, use explode to generate one row per entry.
        df = selected.select("symbol", explode("balanceSheetStatements")).select(
            target_json_fields
        )
        return df


class missing_filter_none_value(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter rows with None or Null values"
        self.category = "Handling Missing Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 100
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.where(col("horsepower").isNull())
        df = auto_df.where(col("horsepower").isNotNull())
        return df


class missing_filter_null_value(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Drop rows with Null values"
        self.category = "Handling Missing Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 200
        self.docmd = """
A `DataFrame`'s [`na`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.na.html#pyspark.sql.DataFrame.na) property returns a special class for dealing with missing values.

This class's [`drop`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.drop.html#pyspark.sql.DataFrameNaFunctions.drop) method returns a new `DataFrame` with nulls omitted. `thresh` controls the number of nulls before the row gets dropped and `subset` controls the columns to consider.
"""

    def snippet(self, auto_df):
        df = auto_df.na.drop(thresh=1, subset=("horsepower",))
        return df


class missing_count_of_null_nan(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Count all Null or NaN values in a DataFrame"
        self.category = "Handling Missing Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, count, isnan, when

        df = auto_df.select(
            [count(when(isnan(c), c)).alias(c) for c in auto_df.columns]
        )
        df = auto_df.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in auto_df.columns]
        )
        return df


class ml_vectorassembler(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Prepare data for training with a VectorAssembler"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 100
        self.preconvert = True
        self.docmd = """
Spark Estimators for tasks like regression, classification or clustering use numeric arrays as inputs. Spark is centered around the idea of a `DataFrame` which is a 2-dimensional structure with strongly-typed columns. You might think these Estimators would take these 2-dimensional structures as inputs but that's not how it works.

Instead these Estimators require special type of column called a Vector Column. The Vector is like an array of numbers packed into a single cell. Combining this with Vectors in other rows in the `DataFrame` gives a 2-dimensional array.

One essential step in using these estimators is to load the data in your `DataFrame` into a `vector` column. This is usually done using a `VectorAssembler`.

This example assembles the cylinders, displacement and acceleration columns from the Auto MPG dataset into a vector column called `features`. If you look at the `features` column in the output you will see it is composed of the values of these source columns. Later examples will use the `features` column to `fit` predictive Models.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler

        vectorAssembler = VectorAssembler(
            inputCols=[
                "cylinders",
                "displacement",
                "acceleration",
            ],
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vectorAssembler.transform(auto_df_fixed)
        assembled = assembled.select(
            ["cylinders", "displacement", "acceleration", "features"]
        )
        print("Data type for features column is:", assembled.dtypes[-1][1])
        return (assembled, dict(truncate=False))


class ml_random_forest_regression(snippet):
    def __init__(self):
        super().__init__()
        self.name = "A basic Random Forest Regression model"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 200
        self.preconvert = True
        self.docmd = """
Random Forest models are popular because they often give good results without a lot of effort. Random Forests can be used for Regression and Classification tasks. This simple example shows building a `RandomForestRegressionModel` to model Miles Per Gallon using a subset of source feature.

The process is:
* Use a `VectorAssembler` to pack interesting `DataFrame` column values into a `Vector`.
* Define the `RandomForestRegressor` estimator with a fixed number of trees. The features column will be the vector column built using the `VectorAssembler` and the value we are trying to predict is `mpg`.
* `fit` the `RandomForestRegressor` estimator using the `DataFrame`. This produces a `RandomForestRegressionModel`.
* Save the `Model`. Later examples will use it.

Graphically this simple process looks like this:
![Diagram of simplified Model fitting](images/mlsimple.webp)

This example does not make predictions, see "Load a model and use it for transformations" or "Load a model and use it for predictions" to see how to make predictions with Models.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor

        vectorAssembler = VectorAssembler(
            inputCols=[
                "cylinders",
                "displacement",
                "horsepower",
            ],
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vectorAssembler.transform(auto_df_fixed)
        assembled = assembled.select(["features", "mpg", "carname"])

        # Define the estimator.
        rf = RandomForestRegressor(
            numTrees=20,
            featuresCol="features",
            labelCol="mpg",
        )

        # Fit the model.
        rf_model = rf.fit(assembled)

        # Save the model.
        rf_model.write().overwrite().save("rf_regression_simple.model")

        return assembled


class ml_hyperparameter_tuning(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Hyperparameter tuning"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 300
        self.preconvert = True
        self.docmd = """
A key factor in the quality of a Random Forest model is a good choice of the number of trees parameter. The previous example arbitrarily set this parameter to 20. In practice it is better to try many parameters and choose the best one.

Spark automates this using using a `CrossValidator`. The `CrossValidator` performs a parallel search of possible parameters and evaluates their performance using random test/training splits of input data. When all parameter combinations are tried the best `Model` is identified and made available in a property called `bestModel`.

Commonly you want to transform your `DataFrame` before your estimator gets it. This is done using a `Pipeline` estimator. When you give a `Pipeline` estimator to a `CrossValidator` the `CrossValidator` will evaluate the final stage in the `Pipeline`.

Graphically this process looks like:
![Diagram of Model fitting with hyperparameter search](images/mloptimized.webp)
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml import Pipeline
        from pyspark.ml.evaluation import RegressionEvaluator
        from pyspark.ml.feature import StringIndexer, VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor
        from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        # Set up our main ML pipeline.
        columns_to_assemble = [
            "cylinders",
            "displacement",
            "acceleration",
        ]
        vector_assembler = VectorAssembler(
            inputCols=columns_to_assemble,
            outputCol="features",
            handleInvalid="skip",
        )

        # Define the model.
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="mpg",
        )

        # Run the pipeline.
        pipeline = Pipeline(stages=[vector_assembler, rf])

        # Hyperparameter search.
        target_metric = "rmse"
        paramGrid = (
            ParamGridBuilder().addGrid(rf.numTrees, list(range(20, 100, 10))).build()
        )
        cross_validator = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=RegressionEvaluator(
                labelCol="mpg", predictionCol="prediction", metricName=target_metric
            ),
            numFolds=2,
            parallelism=4,
        )

        # Run cross-validation to get the best parameters.
        fit_cross_validator = cross_validator.fit(auto_df_fixed)
        best_pipeline_model = fit_cross_validator.bestModel
        best_regressor = best_pipeline_model.stages[1]
        print("Best model has {} trees.".format(best_regressor.getNumTrees))

        # Save the Cross Validator, to capture everything including stats.
        fit_cross_validator.write().overwrite().save("rf_regression_optimized.model")

        # EXCLUDE
        retval = []
        retval.append("Best model has {} trees.".format(best_regressor.getNumTrees))
        print(retval)
        return retval
        # INCLUDE


class ml_string_encode(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Encode string variables as numbers"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 400
        self.preconvert = True
        self.docmd = """
Estimators require numeric inputs. If you have a string column, use `StringIndexer` to convert it to integers using a dictionary that maps the same string to the same integer. Be aware that `StringIndexer` output is not deterministic and values can change if inputs change.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import StringIndexer
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        # Add manufacturer name we will use as a string column.
        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = auto_df_fixed.withColumn(
            "manufacturer", first_word_udf(auto_df_fixed.carname)
        )

        # Encode the manufactor name into numbers.
        indexer = StringIndexer(
            inputCol="manufacturer", outputCol="manufacturer_encoded"
        )
        encoded = (
            indexer.fit(df)
            .transform(df)
            .select(["manufacturer", "manufacturer_encoded"])
        )
        return encoded


class ml_onehot_encode(snippet):
    def __init__(self):
        super().__init__()
        self.name = "One-hot encode a categorical variable"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 410
        self.preconvert = True
        self.docmd = """
Features can be continuous or categorical. Categorical features need to be handled carefully when you fit your estimator. Consider the "Auto MPG" dataset:

* There is a column called displacement. A displacement of 400 is twice as much as a displacement of 200. This is a continuous feature.
* There is a column called origin. Cars originating from the US get a value of 1. Cars originating from Europe get a value of 2. Despite anything you may have read, Europe is not twice as much as the US. Instead this is a categorical feature and no relationship can be learned by comparing values.

If your estimator decides that Europe is twice as much as the US it will make all kinds of other wrong conclusions. There are a few ways of handling categorical features, one popular approach is called "One-Hot Encoding".

If we one-hot encode the origin column we replace it with vectors which encode each possible value of the category. The length of the vector is equal to the number of possible distinct values minus one. At most one of vector's values is set to 1 at any given time. (This is usually called "dummy encoding" which is slightly different from one-hot encoding.) With values encoded this way your estimator won't draw false linear relationships in the feature.

One-hot encoding is easy and may lead to good results but there are different ways to handle categorical values depending on the circumstances. For other tools the pros use look up terms like "deviation coding" or "difference coding".
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import OneHotEncoder

        # Turn the model year into categories.
        year_encoder = OneHotEncoder(
            inputCol="modelyear", outputCol="modelyear_encoded"
        )
        encoded = (
            year_encoder.fit(auto_df_fixed)
            .transform(auto_df_fixed)
            .select(["modelyear", "modelyear_encoded"])
        )
        return (encoded, dict(truncate=False))


class ml_pipeline_encode(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Optimize a model after a data preparation pipeline"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 420
        self.preconvert = True
        self.docmd = """
In general it is best to split data preparation and estimator fitting into 2 distinct pipelines.

Here's a simple examnple of why: imagine you have a string measure that can take 100 different possible values. Before you can `fit` your estimator you need to convert these strings to integers using a `StringIndexer`. The `CrossValidator` will randomly partition your `DataFrame` into training and test sets, then `fit` a `StringIndexer` against the training set to produce a `StringIndexerModel`. If the training set doesn't contain all possible 100 values, when the `StringIndexerModel` is used to `transform` the test set it will encounter unknown values and fail. The `StringIndexer` needs to be `fit` against the full dataset before any estimator fitting. There are other examples and in general the safe choice is to do all data preparation before estimator fitting.

In complex applications different teams are responsible for data preparation (also called "feature engineering") and model development. In this case the feature engineering team will create feature `DataFrame`s and save them into a so-called "Feature Store". A model development team will load `DataFrame`s from the feature store in entirely different applications. The process below divides feature engineering and model development into 2 separate `Pipeline`s but doesn't go so far as to save between Pipelines 1 and 2.

In general this end-to-end looks like:
![End to end data prep and fitting pipelines](images/mlsimpleend2end.webp)
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml import Pipeline
        from pyspark.ml.evaluation import RegressionEvaluator
        from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor
        from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        ### Phase 1.
        # Add manufacturer name we will use as a string column.
        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = auto_df_fixed.withColumn(
            "manufacturer", first_word_udf(auto_df_fixed.carname)
        )

        # Encode the manufactor name into numbers.
        manufacturer_encoder = StringIndexer(
            inputCol="manufacturer", outputCol="manufacturer_encoded"
        )
        # Turn the model year into categories.
        year_encoder = OneHotEncoder(
            inputCol="modelyear", outputCol="modelyear_encoded"
        )

        # Run data preparation as a pipeline.
        data_prep_pipeline = Pipeline(stages=[manufacturer_encoder, year_encoder])
        prepared = data_prep_pipeline.fit(df).transform(df)

        ### Phase 2.
        # Assemble vectors.
        columns_to_assemble = [
            "cylinders",
            "displacement",
            "horsepower",
            "weight",
            "acceleration",
            "manufacturer_encoded",
            "modelyear_encoded",
        ]
        vector_assembler = VectorAssembler(
            inputCols=columns_to_assemble,
            outputCol="features",
            handleInvalid="skip",
        )

        # Define the model.
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="mpg",
        )

        # Define the Pipeline.
        pipeline = Pipeline(stages=[vector_assembler, rf])

        # Run cross-validation to get the best parameters.
        paramGrid = (
            ParamGridBuilder().addGrid(rf.numTrees, list(range(20, 100, 10))).build()
        )
        cross_validator = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=RegressionEvaluator(
                labelCol="mpg", predictionCol="prediction", metricName="rmse"
            ),
            numFolds=2,
            parallelism=4,
        )
        fit_cross_validator = cross_validator.fit(prepared)

        # Save the Cross Validator, to capture everything including stats.
        fit_cross_validator.write().overwrite().save("rf_regression_full.model")


class ml_performance(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Evaluate Model Performance"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 1000
        self.preconvert = True
        self.docmd = """
After you `fit` a model you usually want to evaluate how accurate it is. There are standard ways of evaluating model performance, which vary by the category of model.

Some categories of models:
* Regression models, used when the value to predict is a continuous value.
* Classification models, when when the value to predict can only assume a finite set of values.
* Binary classification models, a special case of classification models that are easier to evaluate.
* Clustering models.

Regression models can be evaluated with `RegressionEvaluator`, which provides these metrics:
* [Root-mean-squared error](https://en.wikipedia.org/wiki/Root-mean-square_deviation) or RMSE.
* [Mean-squared error](https://en.wikipedia.org/wiki/Mean_squared_error) or MSE.
* [R squared](https://en.wikipedia.org/wiki/Coefficient_of_determination) or r2.
* [Mean absolute error](https://en.wikipedia.org/wiki/Mean_absolute_error) or mae.
* [Explained variance](https://en.wikipedia.org/wiki/Explained_variation) or var.

Among these, rmse and r2 are the most commonly used metrics. Lower values of rmse are better, higher values of r2 are better, up to the maximum possible r2 score of 1.

Binary classification models are evaluated using `BinaryClassificationEvaluator`, which provides these measures:
* Area under [Receiver Operating Characteristic](https://en.wikipedia.org/wiki/Receiver_operating_characteristic) curve.
* Area under [Precision Recall](https://en.wikipedia.org/wiki/Precision_and_recall) curve.

The ROC curve is commonly plotted when using binary classifiers. A perfect model would look like an upside-down "L" leading to area under ROC of 1.

Multiclass classification evaluation is much more complex, a `MulticlassClassificationEvaluator` is provided with options including:
* [F1 score](https://en.wikipedia.org/wiki/F-score).
* [Accuracy](https://en.wikipedia.org/wiki/Accuracy_and_precision).
* [True positive rate](https://en.wikipedia.org/wiki/Sensitivity_and_specificity) by label.
* [False positive rate](https://en.wikipedia.org/wiki/Sensitivity_and_specificity) by label.
* [Precision by label](https://en.wikipedia.org/wiki/Precision_and_recall).
* [Recall by label](https://en.wikipedia.org/wiki/Precision_and_recall).
* F measure by label, which computes the F score for a particular label.
* Weighted true positive rate, like true positive rate, but allows each measure to have a weight.
* Weighted false positive rate, like false positive rate, but allows each measure to have a weight.
* Weighted precision, like precision, but allows each measure to have a weight.
* Weighted recall, like recall, but allows each measure to have a weight.
* Weighted f measure, like F1 score, but allows each measure to have a weight.
* [Log loss](https://en.wikipedia.org/wiki/Loss_functions_for_classification) (short for Logistic Loss)
* [Hamming loss](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.hamming_loss.html)

The F1 score is the default. An F1 score of 1 is the best possible score.

For clustering models, Spark offers `ClusteringEvaluator` with one measure:
* [silhouette](https://en.wikipedia.org/wiki/Silhouette_%28clustering%29)

The example below loads 2 regression models fit earlier and compares their metrics. `rf_regression_simple.model` considered 3 columns of the input dataset while `rf_regression_full.model` considered 7. We can expect that `rf_regression_full.model` will have better metrics.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml import Pipeline
        from pyspark.ml.evaluation import RegressionEvaluator
        from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
        from pyspark.ml.regression import RandomForestRegressionModel
        from pyspark.ml.tuning import CrossValidatorModel
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        # Metrics supported by RegressionEvaluator.
        metrics = "rmse mse r2 mae".split()

        # Load the simple model and compute its predictions.
        simple_assembler = VectorAssembler(
            inputCols=[
                "cylinders",
                "displacement",
                "horsepower",
            ],
            outputCol="features",
            handleInvalid="skip",
        )
        simple_input = simple_assembler.transform(auto_df_fixed).select(
            ["features", "mpg"]
        )
        rf_simple_model = RandomForestRegressionModel.load("rf_regression_simple.model")
        simple_predictions = rf_simple_model.transform(simple_input)

        # Load the complex model and compute its predictions.
        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = auto_df_fixed.withColumn(
            "manufacturer", first_word_udf(auto_df_fixed.carname)
        )
        manufacturer_encoder = StringIndexer(
            inputCol="manufacturer", outputCol="manufacturer_encoded"
        )
        year_encoder = OneHotEncoder(
            inputCol="modelyear", outputCol="modelyear_encoded"
        )
        data_prep_pipeline = Pipeline(stages=[manufacturer_encoder, year_encoder])
        prepared = data_prep_pipeline.fit(df).transform(df)
        columns_to_assemble = [
            "cylinders",
            "displacement",
            "horsepower",
            "weight",
            "acceleration",
            "manufacturer_encoded",
            "modelyear_encoded",
        ]
        complex_assembler = VectorAssembler(
            inputCols=columns_to_assemble,
            outputCol="features",
            handleInvalid="skip",
        )
        complex_input = complex_assembler.transform(prepared).select(
            ["features", "mpg"]
        )
        cv_model = CrossValidatorModel.load("rf_regression_full.model")
        best_pipeline = cv_model.bestModel
        rf_complex_model = best_pipeline.stages[-1]
        complex_predictions = rf_complex_model.transform(complex_input)

        # Evaluate performances.
        evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="mpg")
        performances = [
            ["simple", simple_predictions, dict()],
            ["complex", complex_predictions, dict()],
        ]
        for label, predictions, tracker in performances:
            for metric in metrics:
                tracker[metric] = evaluator.evaluate(
                    predictions, {evaluator.metricName: metric}
                )
        print(performances)
        return str(performances)


class ml_feature_importances(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get feature importances of a trained model"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 1010
        self.preconvert = True
        self.docmd = """
Most classifiers and regressors include a property called `featureImportances`. `featureImportances` is an array that:
* Contains floating point values.
* The value corresponds to how important a feature is in the model's predictions relative to other features.
* The sum of the array equals one.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.tuning import CrossValidatorModel

        # Load the model we fit earlier.
        model = CrossValidatorModel.load("rf_regression_full.model")

        # Get the best model.
        best_pipeline = model.bestModel

        # Get the names of assembled columns.
        assembler = best_pipeline.stages[0]
        original_columns = assembler.getInputCols()

        # Get feature importances.
        real_model = best_pipeline.stages[1]
        for feature, importance in zip(original_columns, real_model.featureImportances):
            print("{} contributes {:0.3f}%".format(feature, importance * 100))

        # EXCLUDE
        retval = []
        for feature, importance in zip(original_columns, real_model.featureImportances):
            retval.append("{} contributes {:0.3f}%".format(feature, importance * 100))
        print(retval)
        return retval
        # INCLUDE


class ml_hyperparameter_tuning_plot(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Plot Hyperparameter tuning metrics"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 1100
        self.preconvert = True
        self.docmd = """
An easy way to get a plot of your `DataFrame` is to convert it into a Pandas DataFrame and use the `plot` method Pandas offers. Note that this saves it to the driver's local filesystem and you may need to copy it to another location.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml import Pipeline
        from pyspark.ml.evaluation import RegressionEvaluator
        from pyspark.ml.feature import StringIndexer, VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor
        from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        # Add manufacturer name we will use as a string column.
        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = auto_df_fixed.withColumn(
            "manufacturer", first_word_udf(auto_df_fixed.carname)
        )
        manufacturer_encoded = StringIndexer(
            inputCol="manufacturer", outputCol="manufacturer_encoded"
        )
        encoded_df = manufacturer_encoded.fit(df).transform(df)

        # Set up our main ML pipeline.
        columns_to_assemble = [
            "manufacturer_encoded",
            "cylinders",
            "displacement",
            "horsepower",
            "weight",
            "acceleration",
        ]
        vector_assembler = VectorAssembler(
            inputCols=columns_to_assemble,
            outputCol="features",
            handleInvalid="skip",
        )

        # Define the model.
        rf = RandomForestRegressor(
            numTrees=20,
            featuresCol="features",
            labelCol="mpg",
        )

        # Run the pipeline.
        pipeline = Pipeline(stages=[vector_assembler, rf])

        # Hyperparameter search.
        target_metric = "rmse"
        paramGrid = (
            ParamGridBuilder().addGrid(rf.numTrees, list(range(20, 100, 10))).build()
        )
        crossval = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=RegressionEvaluator(
                labelCol="mpg", predictionCol="prediction", metricName=target_metric
            ),
            numFolds=2,
            parallelism=4,
        )

        # Run cross-validation, get metrics for each parameter.
        model = crossval.fit(encoded_df)

        # Plot results using matplotlib.
        import pandas
        import matplotlib

        parameter_grid = [
            {k.name: v for k, v in p.items()} for p in model.getEstimatorParamMaps()
        ]
        pdf = pandas.DataFrame(
            model.avgMetrics,
            index=[x["numTrees"] for x in parameter_grid],
            columns=[target_metric],
        )
        ax = pdf.plot(style="*-")
        ax.figure.suptitle("Hyperparameter Search: RMSE by Number of Trees")
        ax.figure.savefig("hyperparameters.png")

        return dict(image="hyperparameters.png", alt="Hyperparameter Search")


class ml_covariance(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute correlation matrix"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 1300
        self.preconvert = True
        self.docmd = """
Spark has a few statistics packages like `Correlation`. Like with many Estimators, `Correlation` requires a vector column.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.stat import Correlation

        # Remove non-numeric columns.
        df = auto_df_fixed.drop("carname")

        # Assemble all columns except mpg into a vector.
        feature_columns = list(df.columns)
        feature_columns.remove("mpg")
        vector_col = "features"
        vector_assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol=vector_col,
            handleInvalid="skip",
        )
        df_vector = vector_assembler.transform(df).select(vector_col)

        # Compute the correlation matrix.
        matrix = Correlation.corr(df_vector, vector_col)
        corr_array = matrix.collect()[0]["pearson({})".format(vector_col)].toArray()

        # This part is just for pretty-printing.
        pdf = pandas.DataFrame(
            corr_array, index=feature_columns, columns=feature_columns
        )
        return pdf


class ml_save_model(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a model"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 1400
        self.preconvert = True
        self.docmd = """
To save a model call `fit` on the `Estimator` to build a `Model`, then save the `Model`.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor
        from pyspark.ml.classification import RandomForestClassifier

        vectorAssembler = VectorAssembler(
            inputCols=[
                "cylinders",
                "displacement",
                "horsepower",
            ],
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vectorAssembler.transform(auto_df_fixed)

        # Random test/train split.
        train_df, test_df = assembled.randomSplit([0.7, 0.3])

        # A regression model.
        rf_regressor = RandomForestRegressor(
            numTrees=50,
            featuresCol="features",
            labelCol="mpg",
        )

        # A classification model.
        rf_classifier = RandomForestClassifier(
            numTrees=50,
            featuresCol="features",
            labelCol="origin",
        )

        # Fit the models.
        rf_regressor_model = rf_regressor.fit(train_df)
        rf_regressor_model.write().overwrite().save("rf_regressor_saveload.model")
        rf_classifier_model = rf_classifier.fit(train_df)
        rf_classifier_model.write().overwrite().save("rf_classifier_saveload.model")


class ml_load_model_transform(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a model and use it for transformations"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 1410
        self.preconvert = True
        self.docmd = """
When you are loading a Model, the class you use to call `load` needs to end in `Model`.

The example below loads a `RandomForestRegressionModel` which was the output of the `fit` call on a `RandomForestRegression` estimator. Many people try to load this using the `RandomForestRegression` class but this is the Estimator class and won't work, instead we use the `RandomForestRegressionModel` class.

After we load the Model we can use its `transform` method to convert a `DataFrame` into a new `DataFrame` containing a prediction column. The input `DataFrame` needs to have the same structure as the `DataFrame` use to `fit` the Estimator.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import RandomForestRegressionModel

        # Model type and assembled features need to agree with the trained model.
        rf_model = RandomForestRegressionModel.load("rf_regressor_saveload.model")

        # The input DataFrame needs the same structure we used when we fit.
        vectorAssembler = VectorAssembler(
            inputCols=[
                "cylinders",
                "displacement",
                "horsepower",
            ],
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vectorAssembler.transform(auto_df_fixed)
        predictions = rf_model.transform(assembled).select(
            "carname", "mpg", "prediction"
        )
        return (predictions, dict(truncate=False))


class ml_load_model_predict(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a model and use it for predictions"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 1420
        self.preconvert = True
        self.docmd = """
Some `Model`s support predictions on individual measurements using the `predict` method. The input to `predict` is a `Vector`. The fields in the `Vector` need to match what was used in the `Vector` column when fitting the `Model`.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.linalg import Vectors
        from pyspark.ml.regression import RandomForestRegressionModel

        # Model type and assembled features need to agree with the trained model.
        rf_model = RandomForestRegressionModel.load("rf_regressor_saveload.model")

        input_vector = Vectors.dense([8, 307.0, 130.0])
        prediction = rf_model.predict(input_vector)
        print("Prediction is", prediction)
        return "Prediction is " + str(prediction)


class ml_load_model_predictproba(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a classification model and use it to compute confidences for output labels"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 1430
        self.preconvert = True
        self.docmd = """
Classification `Model`s let you get confidence levels for each possible output class using the `predictProbability` method. This is Spark's equivalent of sklearn's `predict_proba`.
"""

    def snippet(self, auto_df_fixed):
        from pyspark.ml.linalg import Vectors
        from pyspark.ml.classification import RandomForestClassificationModel

        # Model type and assembled features need to agree with the trained model.
        rf_model = RandomForestClassificationModel.load("rf_classifier_saveload.model")

        input_vector = Vectors.dense([8, 307.0, 130.0])
        prediction = rf_model.predictProbability(input_vector)
        print("Predictions are", prediction)
        return "Predictions are " + str(prediction)


class performance_get_spark_version(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get the Spark version"
        self.category = "Performance"
        self.dataset = "UNUSED"
        self.priority = 100

    def snippet(self, df):
        print(spark.sparkContext.version)
        return spark.sparkContext.version


class performance_logging(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Log messages using Spark's Log4J"
        self.category = "Performance"
        self.dataset = "UNUSED"
        self.priority = 110
        self.docmd = """
You can access the JVM of the Spark Context using `_jvm`. Caveats:

- `_jvm` is an internal variable and this could break after a Spark version upgrade.
- This is the JVM running on the Driver node. I'm not aware of a way to access Log4J on Executor nodes.
"""

    def snippet(self, df):
        logger = spark.sparkContext._jvm.org.apache.log4j.Logger.getRootLogger()
        logger.warn("WARNING LEVEL LOG MESSAGE")


class performance_cache(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Cache a DataFrame"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 200
        self.docmd = """
By default a DataFrame is not stored anywhere and is recomputed whenever it is needed. Caching a DataFrame can improve performance if it is accessed many times. There are two ways to do this:

* The DataFrame [`cache`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cache.html#pyspark.sql.DataFrame.cache) method sets the DataFrame's persistence mode to the default (Memory and Disk).
* For more control you can use [`persist`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html#pyspark.sql.DataFrame.persist). `persist` requires a [`StorageLevel`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.StorageLevel.html). `persist` is most typically used to control replication factor.
"""

    def snippet(self, auto_df):
        from pyspark import StorageLevel
        from pyspark.sql.functions import lit

        # Make some copies of the DataFrame.
        df1 = auto_df.where(lit(1) > lit(0))
        df2 = auto_df.where(lit(2) > lit(0))
        df3 = auto_df.where(lit(3) > lit(0))

        print("Show the default storage level (NONE).")
        print(auto_df.storageLevel)

        print("\nChange storage level to Memory/Disk via the cache shortcut.")
        df1.cache()
        print(df1.storageLevel)

        print(
            "\nChange storage level to the equivalent of cache using an explicit StorageLevel."
        )
        df2.persist(storageLevel=StorageLevel(True, True, False, True, 1))
        print(df2.storageLevel)

        print("\nSet storage level to NONE using an explicit StorageLevel.")
        df3.persist(storageLevel=StorageLevel(False, False, False, False, 1))
        print(df3.storageLevel)
        # EXCLUDE
        return [
            "Show the default storage level (NONE).",
            str(auto_df.storageLevel),
            "\nChange storage level to Memory/Disk via the cache shortcut.",
            str(df1.storageLevel),
            "\nChange storage level to the equivalent of cache using an explicit StorageLevel.",
            str(df2.storageLevel),
            "\nSet storage level to NONE using an explicit StorageLevel.",
            str(df3.storageLevel),
        ]
        # INCLUDE


class performance_execution_plan(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Show the execution plan, with costs"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 250
        self.docmd = "IGNORE"

    def snippet(self, auto_df):
        df = auto_df.groupBy("cylinders").count()
        execution_plan = str(df.explain(mode="cost"))
        print(execution_plan)
        return execution_plan


class performance_partition_by_value(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Partition by a Column Value"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.docmd = """
A DataFrame can be partitioned by a column using the `repartition` method. This method requires a target number of partitions and a column name. This allows you to control how data is distributed around the cluster, which can help speed up operations when these operations are able to run entirely on the local partition.
"""

    def snippet(self, auto_df):
        # rows is an iterable, e.g. itertools.chain
        def number_in_partition(rows):
            try:
                first_row = next(rows)
                partition_size = sum(1 for x in rows) + 1
                partition_value = first_row.modelyear
                print(f"Partition {partition_value} has {partition_size} records")
            except StopIteration:
                print("Empty partition")

        df = auto_df.repartition(20, "modelyear")
        df.foreachPartition(number_in_partition)
        # EXCLUDE
        return """Partition 82 has 31 records
Partition 76 has 34 records
Partition 77 has 28 records
Partition 80 has 29 records
Partition 81 has 29 records
Partition 70 has 29 records
Partition 72 has 55 records
Partition 78 has 36 records
Empty partition
Empty partition
Empty partition
Partition 75 has 30 records
Empty partition
Partition 71 has 68 records
Partition 79 has 29 records
Empty partition
Empty partition
Empty partition
Empty partition
Empty partition
"""
        # INCLUDE


class performance_partition_by_range(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Range Partition a DataFrame"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 310
        self.docmd = """
A DataFrame can be range partitioned using `repartitionByRange`. This method requires a number of target partitions and a partitioning key. Range partitioning gives similar benefits to key-based partitioning but may be better when keys can have small numbers of distinct values, which would lead to skew problems in key-based partitioning.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        # rows is an iterable, e.g. itertools.chain
        def count_in_partition(rows):
            my_years = set()
            number_in_partition = 0
            for row in rows:
                my_years.add(row.modelyear)
                number_in_partition += 1
            seen_years = sorted(list(my_years))
            if len(seen_years) > 0:
                seen_values = ",".join(seen_years)
                print(
                    f"This partition has {number_in_partition} records with years {seen_values}"
                )
            else:
                print("Empty partition")

        number_of_partitions = 5
        df = auto_df.repartitionByRange(number_of_partitions, col("modelyear"))
        df.foreachPartition(count_in_partition)
        # EXCLUDE
        return """
This partition has 60 records with years 81,82
This partition has 62 records with years 76,77
This partition has 85 records with years 70,71,72
This partition has 97 records with years 73,74,75
This partition has 94 records with years 78,79,80"""
        # INCLUDE


class performance_partitioning(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Change Number of DataFrame Partitions"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 320
        self.docmd = """
`repartition` a DataFrame to change its number of partitions, potentially moving data across executors in the process. `repartition` accepts 2 optional arguments:
* A number of partitions. If not specified, the system-wide default is used.
* A list of partitioning columns. Data with the same value in these columns will remain in the same partition.

Repartitioning is commonly used to reduce the number of output files when saving a `DataFrame`.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.repartition(col("modelyear"))
        number_of_partitions = 5
        df = auto_df.repartition(number_of_partitions, col("mpg"))
        return df


class performance_reduce_dataframe_partitions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Coalesce DataFrame partitions"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 330
        self.docmd = """
Coalescing reduces the number of partitions in a way that can be much more effecient than using `repartition`.
"""

    def snippet(self, auto_df):
        import math

        target_partitions = math.ceil(auto_df.rdd.getNumPartitions() / 2)
        df = auto_df.coalesce(target_partitions)
        return df


class performance_shuffle_partitions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Set the number of shuffle partitions"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 340
        self.docmd = """
When you have smaller datasets, reducing the number of shuffle partitions can speed up processing and reduce the number of small files created.
"""

    def snippet(self, auto_df):
        # Default shuffle partitions is usually 200.
        grouped1 = auto_df.groupBy("cylinders").count()
        print("{} partition(s)".format(grouped1.rdd.getNumPartitions()))

        # Set the shuffle partitions to 20.
        # This can reduce the number of files generated when saving DataFrames.
        spark.conf.set("spark.sql.shuffle.partitions", 20)

        grouped2 = auto_df.groupBy("cylinders").count()
        print("{} partition(s)".format(grouped2.rdd.getNumPartitions()))
        return ["200 partition(s)", "20 partition(s)"]


class performance_spark_sample(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Sample a subset of a DataFrame"
        self.category = "Performance"
        self.dataset = "UNUSED"
        self.priority = 400
        self.docmd = "IGNORE"

    def snippet(self, df):
        df = (
            spark.read.format("csv")
            .option("header", True)
            .load("data/auto-mpg.csv")
            .sample(0.1)
        )
        return df


class performance_spark_get_configuration(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Print Spark configuration properties"
        self.category = "Performance"
        self.dataset = "UNUSED"
        self.priority = 500

    def snippet(self, df):
        print(spark.sparkContext.getConf().getAll())
        return str(spark.sparkContext.getConf().getAll())


class performance_spark_change_configuration(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Set Spark configuration properties"
        self.category = "Performance"
        self.dataset = "UNUSED"
        self.priority = 510
        self.skip_run = True
        self.docmd = "IGNORE"

    def snippet(self, df):
        key = "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version"
        value = 2

        # Wrong! Settings cannot be changed this way.
        # spark.sparkContext.getConf().set(key, value)

        # Correct.
        spark.conf.set(key, value)

        # Alternatively: Set at build time.
        # Some settings can only be made at build time.
        spark_builder = SparkSession.builder.appName("My App")
        spark_builder.config(key, value)
        spark = spark_builder.getOrCreate()


class performance_concurrent_jobs(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Run multiple concurrent jobs in different pools"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 490
        self.skip_run = True
        self.manual_output = """
Starting job 0 at 2022-03-13 08:53:07.012511
Starting job 1 at 2022-03-13 08:53:07.213019
Starting job 2 at 2022-03-13 08:53:07.416360
Starting job 3 at 2022-03-13 08:53:07.618749
Starting job 4 at 2022-03-13 08:53:07.822736
Starting job 5 at 2022-03-13 08:53:08.023281
Starting job 6 at 2022-03-13 08:53:08.224146
Starting job 7 at 2022-03-13 08:53:08.428519
Job 1 returns Row(cylinders='3', count=800) at 2022-03-13 08:53:08.576396
Starting job 8 at 2022-03-13 08:53:08.631801
Job 6 returns Row(modelyear='73', count=4000) at 2022-03-13 08:53:08.917601
Job 7 returns Row(origin='3', count=6320) at 2022-03-13 08:53:08.941718
Job 3 returns Row(horsepower='102.0', count=160) at 2022-03-13 08:53:09.129134
Job 2 returns Row(displacement='151.0', count=1800) at 2022-03-13 08:53:09.164282
Job 0 returns Row(mpg='20.5', count=660) at 2022-03-13 08:53:09.286087
Job 5 returns Row(acceleration='8.5', count=240) at 2022-03-13 08:53:09.298583
Job 4 returns Row(weight='3574.', count=140) at 2022-03-13 08:53:09.476974
Job 8 returns Row(carname='audi 100 ls', count=60) at 2022-03-13 08:53:09.639598
"""
        self.docmd = """
You can run multiple concurrent Spark jobs by setting `spark.scheduler.pool` before performing an operation. You will need to use multiple threads since operations block.

Pools do not need to be defined ahead of time.
"""

    def snippet(self, auto_df):
        import concurrent.futures
        import datetime
        import time

        other_df = auto_df.toDF(*("_" + c for c in auto_df.columns))
        target_frames = [
            auto_df.crossJoin(other_df.limit((11 - i) * 20))
            .groupBy(column_name)
            .count()
            for i, column_name in enumerate(auto_df.columns)
        ]

        def launch(i, target):
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"pool{i}")
            print("Job", i, "returns", target.first(), "at", datetime.datetime.now())

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for i, target in enumerate(target_frames):
                print("Starting job", i, "at", datetime.datetime.now())
                futures.append(executor.submit(launch, i, target))
                time.sleep(0.2)
            concurrent.futures.wait(futures)
            for future in futures:
                print(future.result())


class performance_metrics(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Publish Metrics to Graphite"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 700
        self.skip_run = True
        self.docmd = """
To publish metrics to Graphite, create a file called graphite_metrics.properties with these contents:

- *.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
- *.sink.graphite.host=<graphite_ip_address>
- *.sink.graphite.port=2003
- *.sink.graphite.period=10
- *.sink.graphite.unit=seconds

Then set spark.metrics.conf to the file graphite_metrics.properties. For example: `$ spark-submit --conf spark.metrics.conf=graphite_metrics.properties myapp.jar`. The documentation has more information about [monitoring Spark jobs](https://spark.apache.org/docs/latest/monitoring.html)
"""

    def snippet(self, auto_df):
        pass


class performance_increase_heap_space(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Increase Spark driver/executor heap space"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 10000
        self.docmd = "IGNORE"

    def snippet(self, df):
        # Memory configuration depends entirely on your runtime.
        # In OCI Data Flow you control memory by selecting a larger or smaller VM.
        # No other configuration is needed.
        #
        # For other environments see the Spark "Cluster Mode Overview" to get started.
        # https://spark.apache.org/docs/latest/cluster-overview.html
        # And good luck!
        # EXCLUDE
        pass
        # INCLUDE


class pandas_spark_dataframe_to_pandas_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert Spark DataFrame to Pandas DataFrame"
        self.category = "Pandas"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, auto_df):
        pandas_df = auto_df.toPandas()
        return pandas_df


class pandas_pandas_dataframe_to_spark_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert Pandas DataFrame to Spark DataFrame with Schema Detection"
        self.category = "Pandas"
        self.dataset = "auto-mpg.csv"
        self.priority = 110

    def snippet(self, auto_df):
        # EXCLUDE
        pandas_df = auto_df.toPandas()
        # INCLUDE
        df = spark.createDataFrame(pandas_df)
        return df


class pandas_pandas_dataframe_to_spark_dataframe_custom(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert Pandas DataFrame to Spark DataFrame using a Custom Schema"
        self.category = "Pandas"
        self.dataset = "auto-mpg.csv"
        self.priority = 120
        self.docmd = """
`createDataFrame` supports Pandas DataFrames as input, but require you to specify the schema manually.
"""

    def snippet(self, auto_df):
        # EXCLUDE
        pandas_df = auto_df.toPandas()
        # INCLUDE
        # This code converts everything to strings.
        # If you want to preserve types, see https://gist.github.com/tonyfraser/79a255aa8a9d765bd5cf8bd13597171e
        from pyspark.sql.types import StructField, StructType, StringType

        schema = StructType(
            [StructField(name, StringType(), True) for name in pandas_df.columns]
        )
        df = spark.createDataFrame(pandas_df, schema)
        return df


class pandas_udaf(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Grouped Aggregation with Pandas"
        self.category = "Pandas"
        self.preconvert = True
        self.priority = 300
        self.dataset = "auto-mpg.csv"
        self.docmd = """
If you need to define a custom analytical function (UDAF), Pandas gives an easy way to do that. Be aware that the aggregation will run on a random executor, which needs to be large enough to hold the entire column in memory.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import pandas_udf
        from pandas import DataFrame

        @pandas_udf("double")
        def mean_udaf(pdf: DataFrame) -> float:
            return pdf.mean()

        df = auto_df.groupby("cylinders").agg(mean_udaf(auto_df["mpg"]))
        return df


class pandas_rescale_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Use a Pandas Grouped Map Function via applyInPandas"
        self.category = "Pandas"
        self.dataset = "auto-mpg.csv"
        self.preconvert = True
        self.priority = 400
        self.docmd = """
You may want to run column-wise operations in Pandas for simplicity or other reasons. The example below shows rescaling a column to lie between 0 and 100 which is clunky in Spark and easy in Pandas. On the other hand if the columns are very tall (you have lots of data) you will run out of memory and your application will crash. Ultimately it's a tradeoff between simplicity and scale.
"""

    def snippet(self, auto_df):
        def rescale(pdf):
            minv = pdf.horsepower.min()
            maxv = pdf.horsepower.max() - minv
            return pdf.assign(horsepower=(pdf.horsepower - minv) / maxv * 100)

        df = auto_df.groupby("cylinders").applyInPandas(rescale, auto_df.schema)
        return df


class pandas_n_rows_from_dataframe_to_pandas(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert N rows from a DataFrame to a Pandas DataFrame"
        self.category = "Pandas"
        self.dataset = "auto-mpg.csv"
        self.priority = 200
        self.docmd = """
If you only want the first rows of a Spark DataFrame to be converted to a Pandas DataFrame, use the `limit` function to select how many you want.

Be aware that rows in a Spark DataFrame have no guaranteed order unless you explicitly order them.
"""

    def snippet(self, auto_df):
        N = 10
        pdf = auto_df.limit(N).toPandas()
        return pdf


class profile_number_nulls(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute the number of NULLs across all columns"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 100
        self.docmd = """
This example creates a new DataFrame consisting of the colunm name and number of NULLs in the column. The example takes advantage of the fact that Spark's `select` method accepts an array. The array is built using a Python [list comprehension](https://docs.python.org/3/tutorial/datastructures.html#list-comprehensions).
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, count, when

        df = auto_df.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in auto_df.columns]
        )
        return df


class profile_numeric_averages(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute average values of all numeric columns"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 200
        self.preconvert = True
        self.docmd = """
This example uses Spark's `agg` function to aggregate multiple columns at once using a dictionary containing column name and aggregate function. This example uses the `avg` aggregate function.
"""

    def snippet(self, auto_df_fixed):
        numerics = set(["decimal", "double", "float", "integer", "long", "short"])
        exprs = {x[0]: "avg" for x in auto_df_fixed.dtypes if x[1] in numerics}
        df = auto_df_fixed.agg(exprs)
        return df


class profile_numeric_min(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute minimum values of all numeric columns"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.preconvert = True
        self.docmd = """
This example uses Spark's `agg` function to aggregate multiple columns at once using a dictionary containing column name and aggregate function. This example uses the `min` aggregate function.
"""

    def snippet(self, auto_df_fixed):
        numerics = set(["decimal", "double", "float", "integer", "long", "short"])
        exprs = {x[0]: "min" for x in auto_df_fixed.dtypes if x[1] in numerics}
        df = auto_df_fixed.agg(exprs)
        return df


class profile_numeric_max(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute maximum values of all numeric columns"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 400
        self.preconvert = True
        self.docmd = """
This example uses Spark's `agg` function to aggregate multiple columns at once using a dictionary containing column name and aggregate function. This example uses the `max` aggregate function.
"""

    def snippet(self, auto_df_fixed):
        numerics = set(["decimal", "double", "float", "integer", "long", "short"])
        exprs = {x[0]: "max" for x in auto_df_fixed.dtypes if x[1] in numerics}
        df = auto_df_fixed.agg(exprs)
        return df


class profile_numeric_median(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute median values of all numeric columns"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 500
        self.preconvert = True
        self.docmd = """
Median can be computed using SQL's `percentile` function with a value of 0.5.
"""

    def snippet(self, auto_df_fixed):
        import pyspark.sql.functions as F

        numerics = set(["decimal", "double", "float", "integer", "long", "short"])
        aggregates = []
        for name, dtype in auto_df_fixed.dtypes:
            if dtype not in numerics:
                continue
            aggregates.append(
                F.expr("percentile({}, 0.5)".format(name)).alias(
                    "{}_median".format(name)
                )
            )
        df = auto_df_fixed.agg(*aggregates)

        return df


class profile_outliers(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Identify Outliers in a DataFrame"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 600
        self.preconvert = True
        self.docmd = """
This example removes outliers using the Median Absolute Deviation. Outliers are identified by variances in a numeric column. Tune outlier sensitivity using z_score_threshold.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, sqrt

        target_column = "mpg"
        z_score_threshold = 2

        # Compute the median of the target column.
        target_df = auto_df.select(target_column)
        target_df.registerTempTable("target_column")
        profiled = sqlContext.sql(
            f"select percentile({target_column}, 0.5) as median from target_column"
        )

        # Compute deviations.
        deviations = target_df.crossJoin(profiled).withColumn(
            "deviation", sqrt((target_df[target_column] - profiled["median"]) ** 2)
        )
        deviations.registerTempTable("deviations")

        # The Median Absolute Deviation
        mad = sqlContext.sql("select percentile(deviation, 0.5) as mad from deviations")

        # Add a modified z score to the original DataFrame.
        df = (
            auto_df.crossJoin(mad)
            .crossJoin(profiled)
            .withColumn(
                "zscore",
                0.6745
                * sqrt((auto_df[target_column] - profiled["median"]) ** 2)
                / mad["mad"],
            )
        )

        df = df.where(col("zscore") > z_score_threshold)
        return df


class timeseries_zero_fill(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Zero fill missing values in a timeseries"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 100
        self.preconvert = True
        self.docmd = """
Imagine you have a time series dataset that contains:

* Customer ID
* Year-Month (YYYY-MM)
* Amount customer spent in that month.

This data is useful to compute things like biggest spender, most frequent buyer, etc.

Imagine though that this dataset doesn't contain a record for customers who didn't buy anything in that month. This will create all kinds of problems, for example the average monthly spend will skew up because no zero values will be included in the average. To answer questions like these we need to create zero-value records for anything that is missing.

In SQL this is handled with the [Partitioned Outer Join](https://www.oracle.com/ocom/groups/public/@otn/documents/webcontent/270646.htm). This example shows you how you can roll you own in Spark:
* Get the distinct list of Customer IDs
* Get the distinct list of dates (all possible YYYY-MM vales)
* Cross-join these two lists to produce all possible customer ID / date combinations
* Perform a right outer join between the original `DataFrame` and the cross joined "all combinations" list
* If the right outer join produces a null, replace it with a zero

The output of this is a row for each customer ID / date combination. The value for spend_dollars is either the value from the original `DataFrame` or 0 if there was no corresponding row in the original `DataFrame`.

You may want to filter the results to remove any rows belonging to a customer before their first actual purchase. Refer to the code for "First Time an ID is Seen" for how to find that information.
"""

    def snippet(self, spend_df):
        from pyspark.sql.functions import coalesce, lit

        # Use distinct values of customer and date from the dataset itself.
        # In general it's safer to use known reference tables for IDs and dates.
        df = spend_df.join(
            spend_df.select("customer_id")
            .distinct()
            .crossJoin(spend_df.select("date").distinct()),
            ["date", "customer_id"],
            "right",
        ).select("date", "customer_id", coalesce("spend_dollars", lit(0)))
        return df


class timeseries_first_seen(snippet):
    def __init__(self):
        super().__init__()
        self.name = "First Time an ID is Seen"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 150
        self.preconvert = True
        self.docmd = "IGNORE"

    def snippet(self, spend_df):
        from pyspark.sql.functions import first
        from pyspark.sql.window import Window

        w = Window().partitionBy("customer_id").orderBy("date")
        df = spend_df.withColumn("first_seen", first("date").over(w))
        return df


class timeseries_running_sum(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Cumulative Sum"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 200
        self.preconvert = True
        self.docmd = """
A comulative sum can be computed using using the standard `sum` function windowed from unbounded preceeding rows to the current row.
"""

    def snippet(self, spend_df):
        from pyspark.sql.functions import sum
        from pyspark.sql.window import Window

        w = (
            Window()
            .partitionBy("customer_id")
            .orderBy("date")
            .rangeBetween(Window.unboundedPreceding, 0)
        )
        df = spend_df.withColumn("running_sum", sum("spend_dollars").over(w))
        return df


class timeseries_running_sum_period(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Cumulative Sum in a Period"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 210
        self.preconvert = True
        self.docmd = """
A comulative sum within particular periods be computed using using the standard `sum` function, windowed from unbounded preceeding rows to the current row and using multiple partitioning keys, one of which represents time periods.
"""

    def snippet(self, spend_df):
        from pyspark.sql.functions import sum, year
        from pyspark.sql.window import Window

        # Add an additional partition clause for the sub-period.
        w = (
            Window()
            .partitionBy(["customer_id", year("date")])
            .orderBy("date")
            .rangeBetween(Window.unboundedPreceding, 0)
        )
        df = spend_df.withColumn("running_sum", sum("spend_dollars").over(w))
        return df


class timeseries_running_average(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Cumulative Average"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 300
        self.preconvert = True
        self.docmd = """
A comulative average can be computed using using the standard `avg` function windowed from unbounded preceeding rows to the current row.
"""

    def snippet(self, spend_df):
        from pyspark.sql.functions import avg
        from pyspark.sql.window import Window

        w = (
            Window()
            .partitionBy("customer_id")
            .orderBy("date")
            .rangeBetween(Window.unboundedPreceding, 0)
        )
        df = spend_df.withColumn("running_avg", avg("spend_dollars").over(w))
        return df


class timeseries_running_average_period(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Cumulative Average in a Period"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 310
        self.preconvert = True
        self.docmd = """
A comulative average within particular periods be computed using using the standard `avg` function, windowed from unbounded preceeding rows to the current row and using multiple partitioning keys, one of which represents time periods.
"""

    def snippet(self, spend_df):
        from pyspark.sql.functions import avg, year
        from pyspark.sql.window import Window

        # Add an additional partition clause for the sub-period.
        w = (
            Window()
            .partitionBy(["customer_id", year("date")])
            .orderBy("date")
            .rangeBetween(Window.unboundedPreceding, 0)
        )
        df = spend_df.withColumn("running_avg", avg("spend_dollars").over(w))
        return df


class fileprocessing_load_files(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load Local File Details into a DataFrame"
        self.category = "File Processing"
        self.dataset = "UNUSED"
        self.priority = 100
        self.docmd = """
This example loads details of local files into a DataFrame. This is a common setup to processing files using `foreach`.
"""

    def snippet(self, df):
        from pyspark.sql.types import (
            StructField,
            StructType,
            LongType,
            StringType,
            TimestampType,
        )
        import datetime
        import glob
        import os

        # Simple: Use glob and only file names.
        files = [[x] for x in glob.glob("/etc/*")]
        df = spark.createDataFrame(files)

        # Advanced: Use os.walk and extended attributes.
        target_path = "/etc"
        entries = []
        walker = os.walk(target_path)
        for root, dirs, files in walker:
            for file in files:
                full_path = os.path.join(root, file)
                try:
                    stat_info = os.stat(full_path)
                    entries.append(
                        [
                            file,
                            full_path,
                            stat_info.st_size,
                            datetime.datetime.fromtimestamp(stat_info.st_mtime),
                        ]
                    )
                except:
                    pass
        schema = StructType(
            [
                StructField("file", StringType(), False),
                StructField("path", StringType(), False),
                StructField("size", LongType(), False),
                StructField("mtime", TimestampType(), False),
            ]
        )
        df = spark.createDataFrame(entries, schema)
        return df


class fileprocessing_load_files_oci(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load Files from Oracle Cloud Infrastructure into a DataFrame"
        self.category = "File Processing"
        self.dataset = "UNUSED"
        self.priority = 200
        self.docmd = """
This example loads details of files in an OCI Object Storage bucket into a DataFrame.
"""

    def snippet(self, df):
        import oci
        from pyspark.sql.types import (
            StructField,
            StructType,
            LongType,
            StringType,
            TimestampType,
        )

        def get_authenticated_client(client):
            config = oci.config.from_file()
            authenticated_client = client(config)
            return authenticated_client

        object_store_client = get_authenticated_client(
            oci.object_storage.ObjectStorageClient
        )

        # Requires an object_store_client object.
        # See https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/api/object_storage/client/oci.object_storage.ObjectStorageClient.html
        input_bucket = "oow_2019_dataflow_lab"
        raw_inputs = object_store_client.list_objects(
            object_store_client.get_namespace().data,
            input_bucket,
            fields="size,md5,timeModified",
        )
        files = [
            [x.name, x.size, x.time_modified, x.md5] for x in raw_inputs.data.objects
        ]
        schema = StructType(
            [
                StructField("name", StringType(), False),
                StructField("size", LongType(), True),
                StructField("modified", TimestampType(), True),
                StructField("md5", StringType(), True),
            ]
        )
        df = spark.createDataFrame(files, schema)
        return df


class fileprocessing_transform_images(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Transform Many Images using Pillow"
        self.category = "File Processing"
        self.dataset = "UNUSED"
        self.priority = 300
        self.docmd = """
In addition to data processing Spark can be used for other types of parallel processing. This example transforms images. The process is:

* Get a list of files.
* Create a DataFrame containing the file names.
* Use `foreach` to call a Python UDF. Each call of the UDF gets a file name.
"""

    def snippet(self, df):
        from PIL import Image
        import glob

        def resize_an_image(row):
            width, height = 128, 128
            file_name = row._1
            new_name = file_name.replace(".png", ".resized.png")
            img = Image.open(file_name)
            img = img.resize((width, height), Image.ANTIALIAS)
            img.save(new_name)

        files = [[x] for x in glob.glob("data/resize_image?.png")]
        df = spark.createDataFrame(files)
        df.foreach(resize_an_image)


class management_delta_table(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save to a Delta Table"
        self.category = "Data Management"
        self.dataset = "auto-mpg.csv"
        self.priority = 100
        self.docmd = """
[Delta Lake](https://delta.io/) is a table format and a set of extensions enabling data management on top of object stores. Delta table format is an extension of [Apache Parquet](https://parquet.apache.org/). "Data management" here means the ability to update or delete table data after it's written without needing to understand the underlying file layout. When you use Delta tables you can treat them much like RDBMS tables.

To create a Delta table save it in Delta format.

Your Spark session needs to be "Delta enabled". See `cheatsheet.py` (the code that generates this cheatsheet) for more information on how to do this.
"""

    def snippet(self, auto_df):
        auto_df.write.mode("overwrite").format("delta").saveAsTable("delta_table")


class management_update_records(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Update records in a DataFrame using Delta Tables"
        self.category = "Data Management"
        self.dataset = "auto-mpg.csv"
        self.priority = 110
        self.docmd = """
The `update` operation behaves like SQL's `UPDATE` statement. `update` is possible on a `DeltaTable` but not on a `DataFrame`.

Be sure to read Delta Lake's documentation on [concurrency control](https://docs.delta.io/latest/concurrency-control.html) before using transactions in any application.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import expr

        output_path = "delta_tests"

        # Currently you have to save/reload to convert from table to DataFrame.
        auto_df.write.mode("overwrite").format("delta").save(output_path)
        dt = DeltaTable.forPath(spark, output_path)

        # Run a SQL update operation.
        dt.update(
            condition=expr("carname like 'Volks%'"), set={"carname": expr("carname")}
        )

        # Convert back to a DataFrame.
        df = dt.toDF()
        return df


class management_merge_tables(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Merge into a Delta table"
        self.category = "Data Management"
        self.dataset = "auto-mpg.csv"
        self.priority = 200
        self.docmd = """
The `merge` operation behaves like SQL's `MERGE` statement. `merge` allows a combination of inserts, updates and deletes to be performed on a table with ACID consistency. `merge` is possible on a `DeltaTable` but not on a `DataFrame`.

Be sure to read Delta Lake's documentation on [concurrency control](https://docs.delta.io/latest/concurrency-control.html) before using transactions in any application.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, expr

        # Save the original data.
        output_path = "delta_tests"
        auto_df.write.mode("overwrite").format("delta").save(output_path)

        # Load data that corrects some car names.
        corrected_df = (
            spark.read.format("csv")
            .option("header", True)
            .load("data/auto-mpg-fixed.csv")
        )

        # Merge the corrected data in.
        dt = DeltaTable.forPath(spark, output_path)
        ret = (
            dt.alias("original")
            .merge(
                corrected_df.alias("corrected"),
                "original.modelyear = corrected.modelyear and original.weight = corrected.weight and original.acceleration = corrected.acceleration",
            )
            .whenMatchedUpdate(
                condition=expr("original.carname <> corrected.carname"),
                set={"carname": col("corrected.carname")},
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Show select table history.
        df = dt.history().select("version operation operationMetrics".split())

        return (df, dict(truncate=False))


class management_version_history(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Show Table Version History"
        self.category = "Data Management"
        self.dataset = "UNUSED"
        self.priority = 300
        self.docmd = """
Delta tables maintain a lot of metadata, ranging from things like operation times, actions, version history, custom metadata and more. This example shows how to use `DeltaTable`'s `history` command to load version history into a `DataFrame` and view it.
"""

    def snippet(self, auto_df):
        # Load our table.
        output_path = "delta_tests"
        dt = DeltaTable.forPath(spark, output_path)

        # Show select table history.
        df = dt.history().select("version timestamp operation".split())
        return df


class management_specific_version(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a Delta Table by Version ID (Time Travel Query)"
        self.category = "Data Management"
        self.dataset = "UNUSED"
        self.priority = 400
        self.docmd = """
To load a specific version of a Delta table, use the `versionAsOf` option. This example also shows how to query the metadata to get available versions.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import desc

        # Get versions.
        output_path = "delta_tests"
        dt = DeltaTable.forPath(spark, output_path)
        versions = (
            dt.history().select("version timestamp".split()).orderBy(desc("version"))
        )
        most_recent_version = versions.first()[0]
        print("Most recent version is", most_recent_version)

        # Load the most recent data.
        df = (
            spark.read.format("delta")
            .option("versionAsOf", most_recent_version)
            .load(output_path)
        )

        # EXCLUDE
        retval = []
        retval.append("Most recent version is " + str(most_recent_version))
        return retval
        # INCLUDE


class management_specific_timestamp(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a Delta Table by Timestamp (Time Travel Query)"
        self.category = "Data Management"
        self.dataset = "UNUSED"
        self.priority = 410
        self.docmd = """
To load a Delta table as of a timestamp, use the `timestampAsOf` option. This example also shows how to query the metadata to get available timestamps.

Usage Notes:
* If the timestamp you specify is earlier than any valid timestamp, the table will fail to load with an error like `The provided timestamp (...) is before the earliest version available to this table`.
* Otherwise, the table version is based on rounding the timestamp you specify down to the nearest valid timestamp less than or equal to the timestamp you specify.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import desc

        # Get versions.
        output_path = "delta_tests"
        dt = DeltaTable.forPath(spark, output_path)
        versions = dt.history().select("version timestamp".split()).orderBy("timestamp")
        most_recent_timestamp = versions.first()[1]
        print("Most recent timestamp is", most_recent_timestamp)

        # Load the oldest version by timestamp.
        df = (
            spark.read.format("delta")
            .option("timestampAsOf", most_recent_timestamp)
            .load(output_path)
        )

        # EXCLUDE
        retval = []
        retval.append("Most recent timestamp is " + str(most_recent_timestamp))
        return retval
        # INCLUDE


class management_compact_table(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compact a Delta Table"
        self.category = "Data Management"
        self.dataset = "UNUSED"
        self.priority = 500
        self.docmd = """
Vacuuming (sometimes called compacting) a table is done by loading the tables' `DeltaTable` and running `vacuum`. This process combines small files into larger files and cleans up old metadata. As of Spark 3.2, 7 days or 168 hours is the minimum retention window.
"""

    def snippet(self, auto_df):
        output_path = "delta_tests"

        # Load table.
        dt = DeltaTable.forPath(spark, output_path)

        # Clean up data older than the given window.
        retention_window_hours = 168
        dt.vacuum(retention_window_hours)

        # Show the new versions.
        df = dt.history().select("version timestamp".split()).orderBy("version")
        return df


class management_write_custom_metadata(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Add custom metadata to a Delta table write"
        self.category = "Data Management"
        self.dataset = "auto-mpg.csv"
        self.priority = 600
        self.docmd = """
Delta tables let your application add custom key/value pairs to writes. You might do this to track workflow IDs or other data to identify the source of writes.
"""

    def snippet(self, auto_df):
        import os
        import time

        extra_properties = dict(
            user=os.environ.get("USER"),
            write_timestamp=time.time(),
        )
        auto_df.write.mode("append").option("userMetadata", extra_properties).format(
            "delta"
        ).save("delta_table_metadata")
        return auto_df


class management_read_custom_metadata(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Read custom Delta table metadata"
        self.category = "Data Management"
        self.dataset = "UNUSED"
        self.priority = 610
        self.docmd = "IGNORE"

    def snippet(self, df):
        dt = DeltaTable.forPath(spark, "delta_table_metadata")
        df = dt.history().select("version timestamp userMetadata".split())
        return (df, dict(truncate=80))


class streaming_connect_kafka_sasl_plain(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Connect to Kafka using SASL PLAIN authentication"
        self.category = "Spark Streaming"
        self.dataset = "UNUSED"
        self.priority = 100
        self.skip_run = True
        self.docmd = """
Replace USERNAME and PASSWORD with your actual values.

This is for test/dev only, for production you should put credentials in a [JAAS Login Configuration File](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html).
"""

    def snippet(self, df):
        options = {
            "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="USERNAME" password="PASSWORD";',
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.bootstrap.servers": "server:9092",
            "group.id": "my_group",
            "subscribe": "my_topic",
        }
        df = spark.readStream.format("kafka").options(**options).load()
        return df


class streaming_csv_windowed(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Create a windowed Structured Stream over input CSV files"
        self.category = "Spark Streaming"
        self.dataset = "UNUSED"
        self.priority = 200
        self.skip_run = True
        self.manual_output = """
-------------------------------------------                                     
Batch: 0
-------------------------------------------
+------------------------------------------+------------+--------------+----------------+-----+
|window                                    |manufacturer|avg_horsepower|avg_timestamp   |count|
+------------------------------------------+------------+--------------+----------------+-----+
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|amc         |131.75        |1.616243250178E9|4    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|chevrolet   |175.0         |1.616243250178E9|4    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|datsun      |88.0          |1.616243250178E9|1    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|dodge       |190.0         |1.616243250178E9|2    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|ford        |159.5         |1.616243250178E9|4    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|plymouth    |155.0         |1.616243250178E9|4    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|pontiac     |225.0         |1.616243250178E9|1    |
+------------------------------------------+------------+--------------+----------------+-----+

-------------------------------------------                                     
Batch: 1
-------------------------------------------
+------------------------------------------+------------+------------------+--------------------+-----+
|window                                    |manufacturer|avg_horsepower    |avg_timestamp       |count|
+------------------------------------------+------------+------------------+--------------------+-----+
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|amc         |119.57142857142857|1.6162432596022856E9|7    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|chevrolet   |140.875           |1.6162432611729999E9|8    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|datsun      |81.66666666666667 |1.616243264838E9    |3    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|dodge       |186.66666666666666|1.6162432575080001E9|3    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|ford        |142.125           |1.6162432623946667E9|9    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|plymouth    |135.0             |1.6162432596022856E9|7    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|pontiac     |168.75            |1.6162432666704998E9|4    |
+------------------------------------------+------------+------------------+--------------------+-----+

-------------------------------------------                                     
Batch: 2
-------------------------------------------
+------------------------------------------+------------+------------------+--------------------+-----+
|window                                    |manufacturer|avg_horsepower    |avg_timestamp       |count|
+------------------------------------------+------------+------------------+--------------------+-----+
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|amc         |119.57142857142857|1.6162432596022856E9|7    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|chevrolet   |140.875           |1.6162432611729999E9|8    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|datsun      |81.66666666666667 |1.616243264838E9    |3    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|dodge       |186.66666666666666|1.6162432575080001E9|3    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|ford        |142.125           |1.6162432623946667E9|9    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|plymouth    |135.0             |1.6162432596022856E9|7    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|pontiac     |168.75            |1.6162432666704998E9|4    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|amc         |150.0             |1.616243297163E9    |2    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|chevrolet   |128.33333333333334|1.616243297163E9    |3    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|datsun      |92.0              |1.616243297163E9    |1    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|dodge       |80.0              |1.616243297163E9    |2    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|ford        |116.25            |1.616243297163E9    |4    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|plymouth    |150.0             |1.616243297163E9    |2    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|pontiac     |175.0             |1.616243297163E9    |1    |
+------------------------------------------+------------+------------------+--------------------+-----+

-------------------------------------------                                     
Batch: 3
-------------------------------------------
+------------------------------------------+------------+------------------+--------------------+-----+
|window                                    |manufacturer|avg_horsepower    |avg_timestamp       |count|
+------------------------------------------+------------+------------------+--------------------+-----+
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|amc         |119.57142857142857|1.6162432596022856E9|7    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|chevrolet   |140.875           |1.6162432611729999E9|8    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|datsun      |81.66666666666667 |1.616243264838E9    |3    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|dodge       |186.66666666666666|1.6162432575080001E9|3    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|ford        |142.125           |1.6162432623946667E9|9    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|plymouth    |135.0             |1.6162432596022856E9|7    |
|{2021-03-20 05:27:00, 2021-03-20 05:28:00}|pontiac     |168.75            |1.6162432666704998E9|4    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|amc         |137.5             |1.616243313837E9    |6    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|chevrolet   |127.44444444444444|1.6162433138370001E9|9    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|datsun      |93.0              |1.6162433096685E9   |2    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|dodge       |115.0             |1.6162433096685E9   |4    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|ford        |122.22222222222223|1.6162433110579998E9|9    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|plymouth    |136.66666666666666|1.616243313837E9    |6    |
|{2021-03-20 05:28:00, 2021-03-20 05:29:00}|pontiac     |202.5             |1.6162433096685E9   |2    |
+------------------------------------------+------------+------------------+--------------------+-----+
"""
        self.docmd = "IGNORE"

    def snippet(self, df):
        from pyspark.sql.functions import avg, count, current_timestamp, window
        from pyspark.sql.types import (
            StructField,
            StructType,
            DoubleType,
            IntegerType,
            StringType,
        )

        input_location = "streaming/input"
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
                StructField("manufacturer", StringType(), True),
            ]
        )
        df = spark.readStream.csv(path=input_location, schema=schema).withColumn(
            "timestamp", current_timestamp()
        )

        aggregated = (
            df.groupBy(window(df.timestamp, "1 minute"), "manufacturer")
            .agg(
                avg("horsepower").alias("avg_horsepower"),
                avg("timestamp").alias("avg_timestamp"),
                count("modelyear").alias("count"),
            )
            .coalesce(10)
        )
        summary = aggregated.orderBy("window", "manufacturer")
        query = (
            summary.writeStream.outputMode("complete")
            .format("console")
            .option("truncate", False)
            .start()
        )
        query.awaitTermination()


class streaming_csv_unwindowed(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Create an unwindowed Structured Stream over input CSV files"
        self.category = "Spark Streaming"
        self.dataset = "UNUSED"
        self.priority = 210
        self.skip_run = True
        self.manual_output = """
-------------------------------------------
Batch: 0
-------------------------------------------
+---------+------------------+-----+
|modelyear|    avg_horsepower|count|
+---------+------------------+-----+
|       70|147.82758620689654|   29|
+---------+------------------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+---------+------------------+-----+
|modelyear|    avg_horsepower|count|
+---------+------------------+-----+
|       71|107.03703703703704|   28|
|       70|147.82758620689654|   29|
+---------+------------------+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+---------+------------------+-----+
|modelyear|    avg_horsepower|count|
+---------+------------------+-----+
|       72|120.17857142857143|   28|
|       71|107.03703703703704|   28|
|       70|147.82758620689654|   29|
+---------+------------------+-----+
"""
        self.docmd = """
This example shows how to deal with an input collection of CSV files. The key thing is you need to specify the schema explicitly, other than that you can use normal streaming operations.
"""

    def snippet(self, df):
        from pyspark.sql.functions import avg, count, desc
        from pyspark.sql.types import (
            StructField,
            StructType,
            DoubleType,
            IntegerType,
            StringType,
        )

        input_location = "streaming/input"
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
                StructField("manufacturer", StringType(), True),
            ]
        )

        df = spark.readStream.csv(path=input_location, schema=schema)
        summary = (
            df.groupBy("modelyear")
            .agg(
                avg("horsepower").alias("avg_horsepower"),
                count("modelyear").alias("count"),
            )
            .orderBy(desc("modelyear"))
            .coalesce(10)
        )
        query = summary.writeStream.outputMode("complete").format("console").start()
        query.awaitTermination()


class streaming_add_timestamp(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Add the current timestamp to a DataFrame"
        self.category = "Spark Streaming"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.docmd = """
Your data needs a timestamp column for windowing. If your source data doesn't include a timestamp you can add one. This example adds the current system time as of DataFrame creation, which may be appropriate if you are not reading historical data.
"""

    def snippet(self, auto_df):
        from pyspark.sql.functions import current_timestamp

        df = auto_df.withColumn("timestamp", current_timestamp())
        return df


class streaming_session_window(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Session analytics on a DataFrame"
        self.category = "Spark Streaming"
        self.dataset = "weblog.csv"
        self.priority = 310
        self.docmd = """
Session windows divide an input stream by both a time dimension and a grouping key. The length of the window depends on how long the grouping key is "active", the length of the window is extended each time the grouping key is seen without a timeout.

This example shows weblog traffic split by IP address, with a 5 minute timeout per session. This sessionization would allow you compute things like average number of visits per session.
"""

    def snippet(self, weblog_df):
        from pyspark.sql.functions import hash, session_window

        hits_per_session = (
            weblog_df.groupBy("ip", session_window("time", "5 minutes"))
            .count()
            .withColumn("session", hash("ip", "session_window"))
        )
        return (hits_per_session, dict(truncate=80))


class streaming_conditional_udf(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Call a UDF only when a threshold is reached"
        self.category = "Spark Streaming"
        self.dataset = "UNUSED"
        self.skip_run = True
        self.priority = 400
        self.docmd = """
It's common you want to call a UDF when measure hits a threshold. You want to call the UDF when a row hits a condition and skip it otherwise. PySpark does not support calling UDFs conditionally (or short-circuiting) as of 3.1.2.

To deal with this put a short-circuit field in the UDF and call the UDF with the condition. If the short-circuit is true return immediately.

This example performs an action when a running average exceeds 100.
"""

    def snippet(self, auto_df):
        from pyspark.sql.types import BooleanType
        from pyspark.sql.functions import udf

        @udf(returnType=BooleanType())
        def myudf(short_circuit, state, value):
            if short_circuit == True:
                return True

            # Log, send an alert, etc.
            return False

        df = (
            spark.readStream.format("socket")
            .option("host", "localhost")
            .option("port", "9090")
            .load()
        )
        parsed = (
            df.selectExpr(
                "split(value,',')[0] as state",
                "split(value,',')[1] as zipcode",
                "split(value,',')[2] as spend",
            )
            .groupBy("state")
            .agg({"spend": "avg"})
            .orderBy(desc("avg(spend)"))
        )
        tagged = parsed.withColumn(
            "below", myudf(col("avg(spend)") < 100, col("state"), col("avg(spend)"))
        )

        tagged.writeStream.outputMode("complete").format(
            "console"
        ).start().awaitTermination()


class streaming_machine_learning(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Streaming Machine Learning"
        self.category = "Spark Streaming"
        self.dataset = "UNUSED"
        self.priority = 500
        self.skip_run = True
        self.docmd = """
MLlib pipelines can be loaded and used in streaming jobs.

When training, define a Pipeline like: `pipeline = Pipeline(stages=[a, b, ...])`

Fit it, then save the resulting model:
- `pipeline_model = pipeline.fit(train_df)`
- `pipeline_model.write().overwrite().save("path/to/pipeline")`

The pipeline model can then be used as shown below.
"""

    def snippet(self, auto_df):
        from pyspark.ml import PipelineModel

        pipeline_model = PipelineModel.load("path/to/pipeline")
        df = pipeline.transform(input_df)
        df.writeStream.format("console").start().awaitTermination()


class streaming_processing_frequency(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Control stream processing frequency"
        self.category = "Spark Streaming"
        self.dataset = "UNUSED"
        self.priority = 600
        self.skip_run = True
        self.docmd = """
Use the processingTime option of trigger to control how frequently microbatches run. You can specify milliseconds or a string interval.
"""

    def snippet(self, auto_df):
        df.writeStream.outputMode("complete").format("console").trigger(
            processingTime="10 seconds"
        ).start().awaitTermination()


class streaming_to_database(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Write a streaming DataFrame to a database"
        self.category = "Spark Streaming"
        self.dataset = "UNUSED"
        self.requires_environment = True
        self.priority = 700
        self.docmd = """
Streaming directly to databases is not supported but you can use foreachBatch to write individual DataFrames to your database. If a task fails you will see the same data with the same epoch_id multiple times and your code will need to handle this.
"""

    def snippet(self, auto_df):
        import time

        pg_database = os.environ.get("PGDATABASE") or "postgres"
        pg_host = os.environ.get("PGHOST") or "localhost"
        pg_password = os.environ.get("PGPASSWORD") or "password"
        pg_user = os.environ.get("PGUSER") or "postgres"
        url = f"jdbc:postgresql://{pg_host}:5432/{pg_database}"
        table = "streaming"
        properties = {
            "driver": "org.postgresql.Driver",
            "user": pg_user,
            "password": pg_password,
        }

        def foreach_batch_function(my_df, epoch_id):
            my_df.write.jdbc(url=url, table=table, mode="Append", properties=properties)

        df = (
            spark.readStream.format("rate")
            .option("rowPerSecond", 100)
            .option("numPartitions", 2)
            .load()
        )
        query = df.writeStream.foreachBatch(foreach_batch_function).start()

        # Wait for some data to be processed and exit.
        for i in range(10):
            time.sleep(5)
            if len(query.recentProgress) > 0:
                query.stop()
                break

        df = spark.read.jdbc(url=url, table=table, properties=properties)
        result = "{} rows written to database".format(df.count())
        print(result)
        return [result]


# Dynamically build a list of all cheats.
cheat_sheet = []
clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
for name, clazz in clsmembers:
    classes = [str(x) for x in inspect.getmro(clazz)[1:]]
    if "<class '__main__.snippet'>" in classes:
        cheat_sheet.append(clazz())


def generate(args):
    # Gather up all the categories and snippets.
    snippets = dict()
    for cheat in cheat_sheet:
        if cheat.category not in snippets:
            snippets[cheat.category] = []
        raw_source = inspect.getsource(cheat.snippet)
        cleaned_source = get_code_snippet(raw_source)
        snippets[cheat.category].append(
            (cheat.name, cheat.priority, cleaned_source, raw_source, cheat)
        )

    # Sort by priority.
    for category, list in snippets.items():
        list = sorted(list, key=lambda x: x[1])
        snippets[category] = list

    # Get info about our categories.
    with open("categories.yaml") as file:
        category_spec = yaml.safe_load(file)

    sorted_categories = sorted(
        snippets.keys(), key=lambda x: category_spec[x]["priority"]
    )

    if args.notebook:
        return generate_notebook(args, snippets, sorted_categories, category_spec)
    else:
        return generate_cheatsheet(args, snippets, sorted_categories, category_spec)


def generate_notebook(args, snippets, sorted_categories, category_spec):
    import nbformat as nbf

    nb = nbf.v4.new_notebook()
    cells = []
    target_file = "cheatsheet.ipynb"

    header_format = "## {}"
    name_format = "**{}**"

    # Info to get the user started.
    preamble = "Run This Code First! This creates the Spark session and loads data."
    preamble = header_format.format(preamble)
    cells.append(nbf.v4.new_markdown_cell(preamble))
    fd = open("notebook_initialization_code.py")
    preamble_code = fd.read()
    cells.append(nbf.v4.new_code_cell(preamble_code))

    for category in sorted_categories:
        list = snippets[category]
        cells.append(
            nbf.v4.new_markdown_cell(
                header_format.format(category_spec[category]["description"])
            )
        )
        for name, priority, source, raw_source, cheat in list:
            if cheat.requires_environment and args.skip_environment:
                continue
            result = cheat.run(show=False)
            if type(result) in (pyspark.sql.dataframe.DataFrame, tuple):
                source += "\ndf.show()"
            cells.append(nbf.v4.new_markdown_cell(name_format.format(name)))
            cells.append(nbf.v4.new_code_cell(source))

    nb["cells"] = cells
    with open(target_file, "w") as f:
        nbf.write(nb, f)


def generate_cheatsheet(args, snippets, sorted_categories, category_spec):
    # TOC Template
    toc_template = """
Table of contents
=================

<!--ts-->
{toc_contents}
<!--te-->
    """

    header = """

{}
{}
"""

    snippet_template = """
```python
{code}
```
"""

    # Generate our markdown.
    toc_content_list = []
    for category in sorted_categories:
        list = snippets[category]
        category_slug = slugify(category)
        toc_content_list.append("   * [{}](#{})".format(category, category_slug))
        for name, priority, source, raw_source, cheat in list:
            name_slug = slugify(name)
            toc_content_list.append("      * [{}](#{})".format(name, name_slug))
    toc_contents = "\n".join(toc_content_list)

    with open("README.md", "w") as fd:
        last_updated = str(datetime.datetime.now())[:-7]
        version = spark.sparkContext.version
        fd.write(
            category_spec["Preamble"]["description"].format(
                version=version, last_updated=last_updated
            )
        )
        fd.write("\n")
        fd.write(toc_template.format(toc_contents=toc_contents))
        for category in sorted_categories:
            list = snippets[category]
            header_text = header.format(category, "=" * len(category))
            fd.write(header_text)
            fd.write(category_spec[category]["description"])
            toc_content_list.append("   * [{}](#{})".format(category, category_slug))
            for name, priority, source, raw_source, cheat in list:
                header_text = header.format(name, "-" * len(name))
                if cheat.requires_environment and args.skip_environment:
                    continue
                fd.write(header_text)
                if cheat.docmd is not None and cheat.docmd != "IGNORE":
                    fd.write(cheat.docmd)
                fd.write(snippet_template.format(code=source))
                result = cheat.run(show=False)
                if result is not None:
                    result_text = get_result_text(result)
                    if result_text.startswith("!["):
                        fd.write("```\n# Code snippet result:\n")
                        fd.write("```\n")
                        fd.write(result_text)
                    else:
                        fd.write("```\n# Code snippet result:\n")
                        fd.write(result_text)
                        if not result_text.endswith("\n"):
                            fd.write("\n")
                        fd.write("```")


def modify_environment(setup=True):
    setup_commands = [
        # Set up a Postgres database.
        "podman pull docker.io/library/postgres:latest",
        "podman run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=password -d postgres:latest",
        "sleep 5",
    ]
    teardown_commands = [
        # Tear down the Postgres database.
        "podman stop postgres",
        "podman rm postgres",
    ]

    # Always run teardown to handle a partially torn-down environment.
    for command in teardown_commands:
        logging.info("Running teardown " + command)
        os.system(command)

    if not setup:
        return

    # Set up the environment we need.
    for command in setup_commands:
        logging.info("Running setup " + command)
        retval = os.system(command)
        if retval != 0:
            raise Exception(command + " returned non-zero exit " + str(retval))


def setup_environment():
    modify_environment(setup=True)


def teardown_environment():
    modify_environment(setup=False)


def all_tests(args):
    for cheat in cheat_sheet:
        if args.category is not None and cheat.category != args.category:
            continue
        if cheat.requires_environment and args.skip_environment:
            continue
        cheat.run()


def dump_priorities():
    for cheat in cheat_sheet:
        print("{},{},{}".format(cheat.category, cheat.name, cheat.priority))


def dump_undocumented():
    count = 0

    for cheat in cheat_sheet:
        # Ignore suppresed cheats.
        if cheat.docmd == "IGNORE":
            continue

        # Ignore trivial cheats.
        source = inspect.getsource(cheat.snippet)
        snippet = get_code_snippet(source).split("\n")
        snippet = [line for line in snippet if not line.startswith("import ")]
        snippet = [line for line in snippet if not line.startswith("from ")]
        snippet = [line for line in snippet if not line.startswith("return")]
        snippet = [line for line in snippet if line != ""]
        if len(snippet) <= 1:
            continue
        if cheat.docmd is None:
            print(cheat.name)
            count += 1
    print(f"{count} undocumented cheats")


def test(test_name):
    for cheat in cheat_sheet:
        if cheat.name == test_name:
            cheat.run()
            source = inspect.getsource(cheat.snippet)
            snippet = get_code_snippet(source)
            print("-- SNIPPET --")
            print(snippet)
            return
    print("No test named " + test_name)
    for cheat in cheat_sheet:
        print("{},{}".format(cheat.category, cheat.name))
    sys.exit(1)


def get_code_snippet(source):
    before_lines = source.split("\n")[1:]
    before_lines = [x[8:] for x in before_lines]
    before_lines = [x for x in before_lines if not x.startswith("return")][:-1]
    logging.debug("-- Snippet before cleaning. --")
    logging.debug("\n".join(before_lines))

    include = True
    after_lines = []
    for line in before_lines:
        if line.strip().startswith("# EXCLUDE"):
            include = False
        if include:
            after_lines.append(line)
        if line.strip().startswith("# INCLUDE"):
            include = True

    cleaned = "\n".join(after_lines)
    logging.debug("-- Snippet after cleaning. --")
    logging.debug(cleaned)
    if len(cleaned) < 3:
        raise Exception("Empty snippet")
    return cleaned


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all-tests", action="store_true")
    parser.add_argument("--category")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--dump-priorities", action="store_true")
    parser.add_argument("--dump-undocumented", action="store_true")
    parser.add_argument("--notebook", action="store_true")
    parser.add_argument("--skip-environment", action="store_true")
    parser.add_argument("--test", action="append")
    args = parser.parse_args()

    # Set the correct directory.
    abspath = os.path.abspath(__file__)
    os.chdir(os.path.dirname(abspath))

    # Set up logging.
    format = "%(asctime)s %(levelname)-8s %(message)s"
    if args.debug:
        logging.basicConfig(format=format, level=logging.DEBUG)
    else:
        logging.basicConfig(format=format, level=logging.INFO)

    # Remove any left over data.
    directories = [
        "header.csv",
        "output.csv",
        "output.parquet",
        "single.csv",
        "spark_warehouse",
    ]
    for directory in directories:
        try:
            shutil.rmtree(directory)
        except Exception as e:
            print(e)
            pass

    if args.dump_priorities:
        dump_priorities()
        return

    if args.dump_undocumented:
        dump_undocumented()
        return

    if not args.skip_environment:
        setup_environment()
    if args.all_tests or args.category:
        all_tests(args)
    elif args.test:
        for this_test in args.test:
            test(this_test)
    else:
        generate(args)
    if not args.skip_environment:
        teardown_environment()


if __name__ == "__main__":
    main()
