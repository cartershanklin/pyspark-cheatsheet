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
    .config("spark.executor.memory", "2G")
    .config("spark.driver.memory", "2G")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", warehouse_path)
    .appName("cheatsheet")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
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

    def snippet(self, df):
        # See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html
        # for a list of supported options.
        df = spark.read.format("csv").option("header", True).load("data/auto-mpg.csv")
        return df


class loadsave_dataframe_from_csv_delimiter(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from a Tab Separated Value (TSV) file"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 110

    def snippet(self, df):
        # See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html
        # for a list of supported options.
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

    def snippet(self, auto_df):
        # See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html
        # for a list of supported options.
        auto_df.write.csv("output.csv")


class loadsave_load_parquet(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from Parquet"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 200

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

    def snippet(self, df):
        # JSON Lines / jsonl format uses one JSON document per line.
        # If you have data with mostly regular structure this is better than nesting it in an array.
        # See https://jsonlines.org/
        df = spark.read.json("data/weblog.jsonl")
        return df


class loadsave_save_catalog(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame into a Hive catalog table"
        self.category = "Accessing Data Sources"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, auto_df):
        auto_df.write.mode("overwrite").saveAsTable("autompg")


class loadsave_load_catalog(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a Hive catalog table into a DataFrame"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 510

    def snippet(self, df):
        # Load the table previously saved.
        df = spark.table("autompg")
        return df


class loadsave_load_sql(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from a SQL query"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 520

    def snippet(self, df):
        # Load the table previously saved.
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

    def snippet(self, df):
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
        return df


class loadsave_write_oracle(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Write a DataFrame to an Oracle DB table using a Wallet"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 10100
        self.skip_run = True

    def snippet(self, df):
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


class loadsave_read_postgres(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Read a Postgres table into a DataFrame"
        self.category = "Accessing Data Sources"
        self.dataset = "UNUSED"
        self.priority = 11000
        self.skip_run = True

    def snippet(self, df):
        # You need a compatible postgresql JDBC JAR.
        pg_database = os.environ.get("PGDATABASE")
        pg_host = os.environ.get("PGHOST")
        pg_password = os.environ.get("PGPASSWORD")
        pg_user = os.environ.get("PGUSER")
        table = "test"

        properties = {
            "driver": "org.postgresql.Driver",
            "user": pg_user,
            "password": pg_password,
        }
        url = f"jdbc:postgresql://{pg_host}:5432/{pg_database}"
        df = spark.read.jdbc(url=url, table=table, properties=properties)
        return df


class loadsave_write_postgres(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Write a DataFrame to a Postgres table"
        self.category = "Accessing Data Sources"
        self.dataset = "auto-mpg.csv"
        self.priority = 11100
        self.skip_run = True

    def snippet(self, auto_df):
        # You need a compatible postgresql JDBC JAR.
        pg_database = os.environ.get("PGDATABASE")
        pg_host = os.environ.get("PGHOST")
        pg_password = os.environ.get("PGPASSWORD")
        pg_user = os.environ.get("PGUSER")
        table = "autompg"

        properties = {
            "driver": "org.postgresql.Driver",
            "user": pg_user,
            "password": pg_password,
        }
        url = f"jdbc:postgresql://{pg_host}:5432/{pg_database}"
        auto_df.write.jdbc(url=url, table=table, mode="Append", properties=properties)


class loadsave_dataframe_from_csv_provide_schema(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Provide the schema when loading a DataFrame from CSV"
        self.category = "Data Handling Options"
        self.dataset = "UNUSED"
        self.priority = 100

    def snippet(self, df):
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

    def snippet(self, auto_df):
        # See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html
        # for a list of supported options.
        auto_df.coalesce(1).write.csv("header.csv", header="true")


class loadsave_single_output_file(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame in a single CSV file"
        self.category = "Data Handling Options"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, auto_df):
        auto_df.coalesce(1).write.csv("single.csv")


class loadsave_dynamic_partitioning(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save DataFrame as a dynamic partitioned table"
        self.category = "Data Handling Options"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

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

    def snippet(self, df):
        """
        Enabling dynamic partitioning lets you add or overwrite partitions
        based on DataFrame contents.

        Without dynamic partitioning the overwrite will overwrite the entire
        table.

        With dynamic partitioning, partitions with keys in the DataFrame are
        overwritten, but partitions not in the DataFrame are untouched.
        """

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        your_dataframe.write.mode("overwrite").insertInto("your_table")


class loadsave_money(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a CSV file with a money column into a DataFrame"
        self.category = "Data Handling Options"
        self.dataset = "UNUSED"
        self.priority = 600

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

    def snippet(self, auto_df):
        # To rename all columns, use toDF with the desired column names in
        # the argument list. This example puts an X in front of all column
        # names.
        df = auto_df.toDF(*["X" + name for name in auto_df.columns])
        return df


class dfo_column_to_python_list(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert a DataFrame column to a Python list"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 710

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

    def snippet(self, auto_df):
        # thresh controls the number of nulls before the row gets dropped.
        # subset controls the columns to consider.
        df = auto_df.na.drop(thresh=2, subset=("horsepower",))
        return df


class missing_count_of_null_nan(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Count all Null or NaN values in a DataFrame"
        self.category = "Handling Missing Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, auto_df):
        from pyspark.sql.functions import col, count, isnan, when

        df = auto_df.select(
            [count(when(isnan(c), c)).alias(c) for c in auto_df.columns]
        )
        df = auto_df.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in auto_df.columns]
        )
        return df


class ml_linear_regression(snippet):
    def __init__(self):
        super().__init__()
        self.name = "A basic Linear Regression model"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg.csv"
        self.priority = 100
        self.preconvert = True

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import LinearRegression

        vectorAssembler = VectorAssembler(
            inputCols=[
                "cylinders",
                "displacement",
                "horsepower",
                "weight",
                "acceleration",
            ],
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vectorAssembler.transform(auto_df_fixed)
        assembled = assembled.select(["features", "mpg", "carname"])

        # Random test/train split.
        train_df, test_df = assembled.randomSplit([0.7, 0.3])

        # Define the model.
        lr = LinearRegression(
            featuresCol="features",
            labelCol="mpg",
            maxIter=10,
            regParam=0.3,
            elasticNetParam=0.8,
        )

        # Train the model.
        lr_model = lr.fit(train_df)

        # Stats for training.
        print(
            "RMSE={} r2={}".format(
                lr_model.summary.rootMeanSquaredError, lr_model.summary.r2
            )
        )

        # Make predictions.
        df = lr_model.transform(test_df)
        return df


class ml_random_forest_regression(snippet):
    def __init__(self):
        super().__init__()
        self.name = "A basic Random Forest Regression model"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg.csv"
        self.priority = 110
        self.preconvert = True

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor
        from pyspark.ml.evaluation import RegressionEvaluator

        vectorAssembler = VectorAssembler(
            inputCols=[
                "cylinders",
                "displacement",
                "horsepower",
                "weight",
                "acceleration",
            ],
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vectorAssembler.transform(auto_df_fixed)
        assembled = assembled.select(["features", "mpg", "carname"])

        # Random test/train split.
        train_df, test_df = assembled.randomSplit([0.7, 0.3])

        # Define the model.
        rf = RandomForestRegressor(
            numTrees=20,
            featuresCol="features",
            labelCol="mpg",
        )

        # Train the model.
        rf_model = rf.fit(train_df)

        # Make predictions.
        df = rf_model.transform(test_df)

        # Evaluate the model.
        r2 = RegressionEvaluator(
            labelCol="mpg", predictionCol="prediction", metricName="r2"
        ).evaluate(df)
        rmse = RegressionEvaluator(
            labelCol="mpg", predictionCol="prediction", metricName="rmse"
        ).evaluate(df)
        print("RMSE={} r2={}".format(rmse, r2))

        return df


class ml_random_forest_classification(snippet):
    def __init__(self):
        super().__init__()
        self.name = "A basic Random Forest Classification model"
        self.category = "Machine Learning"
        self.dataset = "covtype.parquet"
        self.priority = 200

    def snippet(self, covtype_df):
        from pyspark.ml.classification import RandomForestClassifier
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        from pyspark.ml.feature import VectorAssembler

        label_column = "cover_type"
        vectorAssembler = VectorAssembler(
            inputCols=covtype_df.columns,
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vectorAssembler.transform(covtype_df)

        # Random test/train split.
        train_df, test_df = assembled.randomSplit([0.7, 0.3])

        # Define the model.
        rf = RandomForestClassifier(
            numTrees=50,
            featuresCol="features",
            labelCol=label_column,
        )

        # Train the model.
        rf_model = rf.fit(train_df)

        # Make predictions.
        predictions = rf_model.transform(test_df)

        # Select (prediction, true label) and compute test error
        evaluator = MulticlassClassificationEvaluator(
            labelCol=label_column, predictionCol="prediction", metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        print("Test Error = %g" % (1.0 - accuracy))
        df = predictions.select([label_column, "prediction"])
        return df


class ml_string_encode(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Encode string variables before using a VectorAssembler"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 300
        self.preconvert = True

    def snippet(self, auto_df_fixed):
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import StringIndexer, VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor
        from pyspark.ml.evaluation import RegressionEvaluator
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        # Add manufacturer name we will use as a string column.
        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = auto_df_fixed.withColumn(
            "manufacturer", first_word_udf(auto_df_fixed.carname)
        )

        # Strings must be indexed or we will get:
        # pyspark.sql.utils.IllegalArgumentException: Data type string of column manufacturer is not supported.
        #
        # We also encode outside of the main pipeline or else we risk getting:
        #  Caused by: org.apache.spark.SparkException: Unseen label: XXX. To handle unseen labels, set Param handleInvalid to keep.
        #
        # This is because training data is selected randomly and may not have all possible categories.
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

        # Random test/train split.
        train_df, test_df = encoded_df.randomSplit([0.7, 0.3])

        # Define the model.
        rf = RandomForestRegressor(
            numTrees=20,
            featuresCol="features",
            labelCol="mpg",
        )

        # Run the pipeline.
        pipeline = Pipeline(stages=[vector_assembler, rf])
        model = pipeline.fit(train_df)

        # Make predictions.
        df = model.transform(test_df).select("carname", "mpg", "prediction")

        # Select (prediction, true label) and compute test error
        rmse = RegressionEvaluator(
            labelCol="mpg", predictionCol="prediction", metricName="rmse"
        ).evaluate(df)
        print("RMSE={}".format(rmse))

        return df


class ml_feature_importances(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get feature importances of a trained model"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 310
        self.preconvert = True

    def snippet(self, auto_df_fixed):
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import StringIndexer, VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor
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

        # Random test/train split.
        train_df, test_df = encoded_df.randomSplit([0.7, 0.3])

        # Define the model.
        rf = RandomForestRegressor(
            numTrees=20,
            featuresCol="features",
            labelCol="mpg",
        )

        # Run the pipeline.
        pipeline = Pipeline(stages=[vector_assembler, rf])
        model = pipeline.fit(train_df)

        # Make predictions.
        predictions = model.transform(test_df).select("carname", "mpg", "prediction")

        # Get feature importances.
        real_model = model.stages[1]
        for feature, importance in zip(
            columns_to_assemble, real_model.featureImportances
        ):
            print("{} contributes {:0.3f}%".format(feature, importance * 100))

        # EXCLUDE
        retval = []
        for feature, importance in zip(
            columns_to_assemble, real_model.featureImportances
        ):
            retval.append("{} contributes {:0.3f}%".format(feature, importance * 100))
        print(retval)
        return retval
        # INCLUDE


class ml_automated_feature_vectorization(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Automatically encode categorical variables"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 400
        self.preconvert = True

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler, VectorIndexer
        from pyspark.ml.regression import RandomForestRegressor
        from pyspark.sql.functions import countDistinct

        # Remove non-numeric columns.
        df = auto_df_fixed.drop("carname")

        # Profile this DataFrame to get a good value for maxCategories.
        grouped = df.agg(*(countDistinct(c) for c in df.columns))
        grouped.show()

        # Assemble all columns except mpg into a vector.
        feature_columns = list(df.columns)
        feature_columns.remove("mpg")
        vector_assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vector_assembler.transform(df)

        # From profiling the dataset, 15 is a good value for max categories.
        indexer = VectorIndexer(
            inputCol="features", outputCol="indexed", maxCategories=15
        )
        indexed = indexer.fit(assembled).transform(assembled)

        # Build and train the model.
        train_df, test_df = indexed.randomSplit([0.7, 0.3])
        rf = RandomForestRegressor(
            numTrees=50,
            featuresCol="features",
            labelCol="mpg",
        )
        rf_model = rf.fit(train_df)

        # Get feature importances.
        for feature, importance in zip(feature_columns, rf_model.featureImportances):
            print("{} contributes {:0.3f}%".format(feature, importance * 100))

        # Make predictions.
        df = rf_model.transform(test_df).select("mpg", "prediction")
        return df


class ml_hyperparameter_tuning(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Hyperparameter tuning"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 410
        self.preconvert = True

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
        fit_cross_validator = cross_validator.fit(encoded_df)
        best_pipeline_model = fit_cross_validator.bestModel
        best_regressor = best_pipeline_model.stages[1]
        print("Best model has {} trees.".format(best_regressor.getNumTrees))

        # Save the Cross Validator, to capture everything including stats.
        fit_cross_validator.write().overwrite().save("fit_cross_validator.model")

        # Or, just save the best model.
        best_pipeline_model.write().overwrite().save("best_pipeline_model.model")

        # EXCLUDE
        retval = []
        retval.append("Best model has {} trees.".format(best_regressor.getNumTrees))
        print(retval)
        return retval
        # INCLUDE


class ml_hyperparameter_tuning_plot(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Plot Hyperparameter tuning metrics"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 415
        self.preconvert = True

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


class ml_random_forest_classification_hyper(snippet):
    def __init__(self):
        super().__init__()
        self.name = "A Random Forest Classification model with Hyperparameter Tuning"
        self.category = "Machine Learning"
        self.dataset = "covtype.parquet"
        self.priority = 420

    def snippet(self, covtype_df):
        from pyspark.ml.classification import RandomForestClassifier
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml import Pipeline
        from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

        label_column = "cover_type"
        vector_assembler = VectorAssembler(
            inputCols=covtype_df.columns,
            outputCol="features",
            handleInvalid="skip",
        )

        # Define the model.
        rf = RandomForestClassifier(
            numTrees=50,
            featuresCol="features",
            labelCol=label_column,
        )

        # Run the pipeline.
        pipeline = Pipeline(stages=[vector_assembler, rf])

        # Hyperparameter search.
        paramGrid = (
            ParamGridBuilder().addGrid(rf.numTrees, list(range(50, 80, 10))).build()
        )
        crossval = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=MulticlassClassificationEvaluator(
                labelCol=label_column, predictionCol="prediction"
            ),
            numFolds=2,
            parallelism=4,
        )

        # Run cross-validation and choose the best set of parameters.
        model = crossval.fit(covtype_df)

        # Identify the best hyperparameters.
        real_model = model.bestModel.stages[1]
        print("Best model has {} trees.".format(real_model.getNumTrees))


class ml_covariance(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute correlation matrix"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 600
        self.preconvert = True

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
        self.priority = 10
        self.preconvert = True

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor

        vectorAssembler = VectorAssembler(
            inputCols=[
                "cylinders",
                "displacement",
                "horsepower",
                "weight",
                "acceleration",
            ],
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vectorAssembler.transform(auto_df_fixed)

        # Random test/train split.
        train_df, test_df = assembled.randomSplit([0.7, 0.3])

        # Define the model.
        rf = RandomForestRegressor(
            numTrees=50,
            featuresCol="features",
            labelCol="mpg",
        )

        # Train the model.
        rf_model = rf.fit(train_df)
        rf_model.write().overwrite().save("rf_regression.model")


class ml_load_model(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a model and use it for predictions"
        self.category = "Machine Learning"
        self.dataset = "auto-mpg-fixed.csv"
        self.priority = 20
        self.preconvert = True

    def snippet(self, auto_df_fixed):
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import RandomForestRegressionModel

        # Model type and assembled features need to agree with the trained model.
        rf_model = RandomForestRegressionModel.load("rf_regression.model")
        vectorAssembler = VectorAssembler(
            inputCols=[
                "cylinders",
                "displacement",
                "horsepower",
                "weight",
                "acceleration",
            ],
            outputCol="features",
            handleInvalid="skip",
        )
        assembled = vectorAssembler.transform(auto_df_fixed)

        predictions = rf_model.transform(assembled).select(
            "carname", "mpg", "prediction"
        )
        return predictions


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


# XXX: Should find some solution for hard-coded output.
class performance_partition_by_range(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Range Partition a DataFrame"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 310

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

    def snippet(self, auto_df):
        from pyspark.sql.functions import col

        df = auto_df.repartition(col("modelyear"))
        number_of_partitions = 5
        df = auto_df.repartitionByRange(number_of_partitions, col("mpg"))
        return df


class performance_reduce_dataframe_partitions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Coalesce DataFrame partitions"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 330

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

    def snippet(self, auto_df):
        """
        To publish metrics to Graphite, create a file called graphite_metrics.properties with these contents:

        *.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
        *.sink.graphite.host=<graphite_ip_address>
        *.sink.graphite.port=2003
        *.sink.graphite.period=10
        *.sink.graphite.unit=seconds

        Then set spark.metrics.conf to the file graphite_metrics.properties. For example:
        $ spark-submit --conf spark.metrics.conf=graphite_metrics.properties myapp.jar

        Read more about monitoring Spark jobs at https://spark.apache.org/docs/latest/monitoring.html
        """


class performance_increase_heap_space(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Increase Spark driver/executor heap space"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 10000

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

    def snippet(self, auto_df):
        # This approach uses the Median Absolute Deviation.
        # Outliers are based on variances in a single numeric column.
        # Tune outlier sensitivity using z_score_threshold.
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

    def snippet(self, df):
        # EXCLUDE
        import oci

        def get_authenticated_client(client):
            config = oci.config.from_file()
            authenticated_client = client(config)
            return authenticated_client

        object_store_client = get_authenticated_client(
            oci.object_storage.ObjectStorageClient
        )
        # INCLUDE
        from pyspark.sql.types import (
            StructField,
            StructType,
            LongType,
            StringType,
            TimestampType,
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

    def snippet(self, auto_df):
        auto_df.write.mode("overwrite").format("delta").saveAsTable("delta_table")


class management_update_records(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Update records in a DataFrame using Delta Tables"
        self.category = "Data Management"
        self.dataset = "auto-mpg.csv"
        self.priority = 110

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


class streaming_connect_kafka_sasl_plain(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Connect to Kafka using SASL PLAIN authentication"
        self.category = "Spark Streaming"
        self.dataset = "UNUSED"
        self.priority = 100
        self.skip_run = True

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

    def snippet(self, auto_df):
        from pyspark.sql.functions import current_timestamp

        df = auto_df.withColumn("timestamp", current_timestamp())
        return df


class streaming_conditional_udf(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Call a UDF only when a threshold is reached"
        self.category = "Spark Streaming"
        self.dataset = "UNUSED"
        self.skip_run = True
        self.priority = 400

    def snippet(self, auto_df):
        """
        It's common you want to call a UDF when measure hits a threshold. You want
        to call the UDF when a row hits a condition and skip it otherwise. PySpark
        does not support calling UDFs conditionally (or short-circuiting) as of 3.1.2.

        To deal with this put a short-circuit field in the UDF and call the UDF
        with the condition. If the short-circuit is true return immediately.

        This example performs an action when a running average exceeds 100.
        """
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

    def snippet(self, auto_df):
        """
        MLlib pipelines can be loaded and used in streaming jobs.

        When training, define a Pipeline like:
            pipeline = Pipeline(stages=[a, b, ...])

        Fit it, then save the resulting model:
            pipeline_model = pipeline.fit(train_df)
            pipeline_model.write().overwrite().save("path/to/pipeline")

        The pipeline model can then be used as shown below.
        """

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

    def snippet(self, auto_df):
        """
        Use the processingTime option of trigger to control how frequently microbatches
        run. You can specify milliseconds or a string interval.
        """
        df.writeStream.outputMode("complete").format("console").trigger(
            processingTime="10 seconds"
        ).start().awaitTermination()


# Dynamically build a list of all cheats.
cheat_sheet = []
clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
for name, clazz in clsmembers:
    classes = [str(x) for x in inspect.getmro(clazz)[1:]]
    if "<class '__main__.snippet'>" in classes:
        cheat_sheet.append(clazz())


def generate(type):
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

    if type == "markdown":
        return generate_cheatsheet(snippets, sorted_categories, category_spec)
    else:
        return generate_notebook(snippets, sorted_categories, category_spec)


def generate_notebook(snippets, sorted_categories, category_spec):
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
            if cheat.skip_run:
                continue
            result = cheat.run(show=False)
            if type(result) in (pyspark.sql.dataframe.DataFrame, tuple):
                source += "\ndf.show()"
            cells.append(nbf.v4.new_markdown_cell(name_format.format(name)))
            cells.append(nbf.v4.new_code_cell(source))

    nb["cells"] = cells
    with open(target_file, "w") as f:
        nbf.write(nb, f)


def generate_cheatsheet(snippets, sorted_categories, category_spec):
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
                fd.write(header_text)
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


def all_tests(category=None):
    for cheat in cheat_sheet:
        if category is not None and cheat.category != category:
            continue
        cheat.run()


def dump_priorities():
    for cheat in cheat_sheet:
        print("{},{},{}".format(cheat.category, cheat.name, cheat.priority))


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
    parser.add_argument("--notebook", action="store_true")
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

    if args.all_tests or args.category:
        all_tests(args.category)
    elif args.dump_priorities:
        dump_priorities()
    elif args.test:
        for this_test in args.test:
            test(this_test)
    elif args.notebook:
        generate("notebook")
    else:
        generate("markdown")


if __name__ == "__main__":
    main()
