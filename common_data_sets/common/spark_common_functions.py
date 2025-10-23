from datetime import datetime as dt, timedelta as td, date
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, expr
import logging
import boto3
import botocore.exceptions as bex
import os
import yaml
import io
import pandas as pd
import time
from typing import List

# Variables

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SPARK_GLUE_DTYPE_MAPPING = {'boolean': 'BooleanType', 'tinyint': 'IntegerType', 'smallint': 'IntegerType',
                            'int': 'IntegerType', 'integer': 'IntegerType', 'bigint': 'IntegerType',
                            'double': 'DoubleType', 'float': 'FloatType', 'decimal': 'DecimalType',
                            'char': 'StringType', 'varchar': 'StringType', 'string': 'StringType',
                            'date': 'DataType', 'timestamp': 'TimestampType', 'struct': 'StructType',
                            'str': 'StringType'
                            }


# S3 Path resolver

def format_s3_path(bucket_name: str, output_path: str, query_type: str, output_format: str = None) -> str:
    """
    :param bucket_name: S3 bucket name
    :param output_path: S3 folder prefix
    :param output_format: output format
    :param query_type: sub folder name
    :return: S3 path URI
    """
    if (output_format):
        s3_path = f"s3a://{bucket_name}/{output_path}/{output_format}/{query_type}/"
    else:
        s3_path = f"s3a://{bucket_name}/{output_path}/{query_type}/"
    return s3_path

def format_s3_path_for_pcm(bucket_name: str, prefix_name: str) -> str:
    """
    :param bucket_name: S3 bucket name
    """
    return f"s3://{bucket_name}/pcm/{prefix_name}/"

def get_bucket_key(s3_uri: str):
    bucket_name = s3_uri.split('/')[2]
    key = s3_uri.split(bucket_name)[1]
    return bucket_name, key


def copy_all_s3_object(bucket_name: str, source_prefix: str, tgt_prefix: str, force: bool = False):
    s3_resource = boto3.resource('s3')
    src_bucket_object = s3_resource.Bucket(bucket_name)

    try:
        for obj in src_bucket_object.objects.filter(Prefix=source_prefix):
            copy_source = {'Bucket': bucket_name,
                           'Key': obj.key}
            tgt_path = os.path.join(tgt_prefix, obj.key.split(source_prefix)[-1])
            src_bucket_object.copy(copy_source, tgt_path)
    except bex.ClientError as ce:
        if force:
            raise bex.ClientError({'Error': {'Code': "404", 'Message': "Source path Doesn't exist"}}, "ArchiveTable")
        elif ce.response['Error']['Code'] == "404":
            logger.error("Object Doesn't exists")
        else:
            logger.error("Error while copying data check error logs for details")


def delete_s3_object(bucket_name: str, del_prefix: str, force: bool = False):
    s3_client = boto3.resource('s3')
    bucket_name = s3_client.Bucket(bucket_name)
    try:
        for key in bucket_name.objects.filter(Prefix=del_prefix):
            key.delete()
    except bex.ClientError as ce:
        if force:
            raise bex.ClientError({'Error': {'Code': "404", 'Message': "Source path Doesn't exist"}}, "DeleteObject")
        elif ce.response['Error']['Code'] == "404":
            logger.error("Object Doesn't exists")
        else:
            logger.error("Error while deleting data check error logs for details")


def move_s3_files(src_bucket: str, src_folder: str, dest_bucket: str, dest_folder: str, delete_flag=True):
    s3 = boto3.client('s3')

    # List all files in the source folder
    response = s3.list_objects_v2(Bucket=src_bucket, Prefix=src_folder)
    if 'Contents' not in response:
        print("No files found in the source folder.")
        return

    for obj in response['Contents']:
        src_key = obj['Key']
        dest_key = dest_folder + src_key[len(src_folder):]

        # Copy the file to the destination bucket
        s3.copy_object(
            Bucket=dest_bucket,
            CopySource={'Bucket': src_bucket, 'Key': src_key},
            Key=dest_key
        )

        # Delete the file from the source bucket
        if delete_flag:
            s3.delete_object(Bucket=src_bucket, Key=src_key)
            logger.info(f"Moved {src_key} to {dest_key}")

    logger.info("All files have been moved successfully.")


def get_s3_object(s3_bucket: str, key: str):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=s3_bucket, Key=key)

    if response.__contains__("ContentLength"):
        if response["ContentLength"] > 0:
            return response
        else:
            logger.error(f"{key} in {s3_bucket} is empty")
            return False
    else:
        logger.error(f"{key} doesn't exist in {s3_bucket}")
        return False

def get_glue_table_location(database_name, table_name, region_name="eu-west-2"):
    glue = boto3.client("glue", region_name=region_name)

    try:
        response = glue.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        location = response["Table"]["StorageDescriptor"]["Location"]
        return location
    except Exception as e:
        print(f"Error fetching Glue table location: {e}")
        return None

def save_spark_dfs_as_excel(df_to_sheet_mapping: dict, bucket_name: str, output_prefix: str):
    s3 = boto3.resource('s3')
    with io.BytesIO() as output:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for report_name, df in df_to_sheet_mapping.items():
                df = df.toPandas()
                if len(report_name) > 30:
                    logger.info(f"Renaming the report from {report_name} to {report_name[:30]}")
                    report_name = report_name[:30]
                logger.info("")
                df.to_excel(writer, sheet_name=report_name)
        data = output.getvalue()
    logger.info(f"Saving {output_prefix} into {bucket_name}")
    s3.Bucket(bucket_name).put_object(Key=output_prefix, Body=data)


def read_excel_as_spark_df(spark: SparkSession, src_bucket: str, filepath: str, header=0):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=src_bucket, Key=filepath)
    try:
        pd_df = pd.read_excel(io.BytesIO(obj['Body'].read()), header=header)
        spark_df = spark.createDataFrame(pd_df)
    except bex as E:
        logger.error(f"Exception occurred while reading the excel from {src_bucket}/{filepath}")
        spark_df = create_empty_df(spark)

    return spark_df


# Python Functions

def get_last_month_date():
    """
    :return: Date - (Today-30 days)
    """
    return (dt.now() + td(days=-30)).date()


def get_last_year_month(months: int = 1) -> str:
    """
    :return: Date - (current_month - months)
    """
    last_month = date.today()
    for month in range(months):
        last_month = last_month.replace(day=1) - td(days=1)
    return last_month.strftime('%Y%m')


def get_last_year(years: int = 1):
    """
    :return: Date - (Today - years)
    """
    last_year = date.today()
    for year in range(years):
        last_year = last_year.replace(day=1, month=1) - td(days=1)
    return last_year.strftime('%Y')


def parse_filter_id_list_param(filter_id_list):
    """
    :param filter_id_list: list of claim ids on which data needs to be filtered
    :return: list of claim ids if claim ids are passed else None
    """
    filter_id_list = filter_id_list.replace("'", "")
    filter_id_list = filter_id_list.replace(" ", "")
    try:
        if filter_id_list == "ALL" or filter_id_list == "":
            return None
        id_list = filter_id_list.split(",")
        return id_list
    except SystemError as e:
        return None


def compare_list(list1, list2) -> list:
    missing_item = list()
    for item in list1:
        if item not in list2:
            missing_item.append(item)
    return missing_item


def get_file_path(file_name: str):
    curr_dict = os.getcwd()
    resource_path = ''
    for root, dir_name, files in os.walk(curr_dict):
        if file_name in files:
            resource_path = os.path.join(root, file_name)
            return resource_path
    return resource_path


def read_yaml(file_path, source_type: str = "s3"):
    try:
        assert {source_type.lower()}.issubset({"s3", "local"})

        if source_type == "s3":
            yaml_dict_obj = yaml.safe_load(file_path["Body"])
        else:
            with open(file_path) as m_yml:
                yaml_dict_obj = yaml.safe_load(m_yml)

        return yaml_dict_obj

    except AssertionError as exc:
        logger.error("Invalid source type, expected file types are -> s3 & local")
        return exc
    except yaml.YAMLError as exc:
        logger.error("Error While reading Yaml File")
        return exc


def apply_filter(df: DataFrame, filters, sep=",") -> DataFrame:
    filter_cond = ""

    if isinstance(filters, str):
        filters = filters.split(sep)

    for f in filters:
        if "=" in f:
            value = f.split("=")[1].replace("'", "")
            if filters.index(f) < (len(filters) - 1):
                filter_cond += f"({f.split('=')[0]}=='{value}') and "
            else:
                filter_cond += f"({f.split('=')[0]}=='{value}')"
        elif filters.index(f) < (len(filters) - 1):
            filter_cond += f"({f}) and "
        else:
            filter_cond += f"({f})"
    logger.info(f"applied filters {filter_cond}")

    return df.filter(expr(filter_cond))


# Dataframe Creation Functions

def create_empty_df(spark: SparkSession):
    """
    :return: empty Spark DataFrame
    """
    emptyRDD = spark.sparkContext.emptyRDD()
    schema = StructType([])
    df = spark.createDataFrame(emptyRDD, schema)
    return df


def create_dataframe_from_file(spark: SparkSession, file_name, file_type='csv', properties: dict = None) -> DataFrame:
    reader = spark.read
    if properties:
        for key, values in properties.items():
            reader = reader.option(key, values)

    if file_type.upper() == 'CSV' or file_type.upper() == 'TXT':
        return reader.csv(file_name)

    if file_type.upper() == 'JSON':
        return reader.json(file_name)

    if file_type.upper() == 'PARQUET':
        return reader.json(file_name)

    logger.error("In valid file format, expected file format -> txt, csv, json, parquet")

    return create_empty_df(spark)


# Dataframe Access Functions

def get_columns(df) -> list:
    """
    :param df:
    :return: list of columns from configs
    """
    return df.columns


def select_df_columns(df, keep_list: list):
    """
    :param df: Spark DataFrame
    :param keep_list: list of columns
    :return: Spark DataFrame
    """
    df_list = [colm.lower() for colm in df.columns]
    for colm in keep_list:
        if colm.lower() not in df_list:
            keep_list.remove(colm)
    return df.select(keep_list)


def is_empty(df: DataFrame):
    if df.head(1):
        return False
    else:
        return True


def is_non_empty(df: DataFrame):
    if df.head(1):
        return True
    else:
        return False


def struct_to_dict(schema: StructType) -> dict:
    dtype_mapping = dict()
    for typ in schema:
        dtype_mapping[typ.name] = typ.dataType
    return dtype_mapping


# Dataframe Modification Functions

def rename_columns(df: DataFrame, col_mapping: dict):
    for old_name, new_name in col_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
        else:
            logger.warning(f"column {old_name} not in present in DataFrame")
    return df


def reformat_df(df, schema: StructType):
    """
    :param schema: StrucType holding the data types & column names
    :param df: Spark DataFrame
    :return: Spark DataFrame
    """
    dtype_column_mapping = struct_to_dict(schema)
    column_list = list(dtype_column_mapping.keys())
    for colm in column_list:
        if colm in [i.lower() for i in df.columns]:
            df = df.withColumn(colm, col(colm).cast(dtype_column_mapping[colm]))
        elif colm not in [i.lower() for i in df.columns]:
            df = df.withColumn(colm, lit(None).cast(dtype_column_mapping[colm]))
    return df.select(column_list)


def reformat_df_schema(df, schema: StructType):
    """
    This method will reformat the schema of data frame base on struct type
    :param df:
    :param schema:
    :return:
    """
    dtype_mapping = struct_to_dict(schema)
    df_columns = df.columns
    for column, dtype in dtype_mapping.items():
        if column in df_columns:
            df = df.withColumn(column, col(column).cast(dtype))
    return df


def align_dataframes_to_max_schema(dfs: List[DataFrame]) -> List[DataFrame]:
    """
    Takes a list of PySpark DataFrames and returns the same list with all DataFrames
    aligned to a unified schema (union of all columns), adding missing columns as nulls.
    """
    if not dfs:
        return []

    # Step-1 → Collect all unique column names across all dataframes
    all_columns_set = set()
    for df in dfs:
        all_columns_set.update(df.columns)

    all_columns = sorted(list(all_columns_set))

    logger.debug(f"All columns: {all_columns}")

    # Step-2 → Align each dataframe to have all columns
    dfs_aligned = []
    for df in dfs:
        current_cols = set(df.columns)
        missing_cols = all_columns_set - current_cols

        # Add missing columns as null
        for col in missing_cols:
            df = df.withColumn(col, lit(None))

        # Reorder columns to match the unified schema
        df = df.select(*all_columns)

        dfs_aligned.append(df)

    return dfs_aligned

def replace_column_name_chars(df: DataFrame, char_mapping: dict) -> DataFrame:
    mapping_table = str.maketrans(char_mapping)
    col_mapping = dict()
    for cols in df.columns:
        col_mapping[cols] = cols.translate(mapping_table).lower()

    return rename_columns(df, col_mapping)


def df_to_list(df: DataFrame, col_name):
    # return df.select(col_name).rdd.flatMap(lambda x: x).collect()
    return [val[0] for val in df.select(col_name).collect()]


def str_to_list(input_str: str, sep: str) -> list:
    """
    input_str: input string
    sep: separate
    """
    return [val.strip() for val in input_str.split(sep)]


def merge_str_list(*args, sep: str) -> list:
    merge_list = list()
    for p in args:
        if p:
            merge_list += str_to_list(p, sep)
    return list(set(merge_list))


def list_to_str(input_list: str, sep=',', prefix='') -> str:
    if not input_list:
        return ""
    op_str = ""
    if input_list:
        op_str += prefix

    for itm in input_list:
        if input_list.index(itm) < len(input_list) - 1:
            op_str += str(itm) + sep
        else:
            op_str += str(itm)

    return op_str


def flat_list(input_list: list, output_list=None):
    if output_list is None:
        output_list = []
    logger.info(f"flattening {str(input_list)}")
    for itm in input_list:
        if isinstance(itm, list):
            flat_list(itm, output_list)
        else:
            output_list.append(itm)

    logger.info(f"returning {str(output_list)}")
    return output_list


# Glue common functions

def get_glue_table_schema(table_name: str, db_name: str, region_name: str = 'eu-west-2') -> dict:  # No coverage
    glue_client = boto3.client('glue', region_name=region_name)
    schema = dict()
    try:
        table_details = glue_client.get_table(DatabaseName=db_name, Name=table_name)
        table = table_details.get('Table')

        # get the columns
        columns = table.get('StorageDescriptor').get('Columns')

        # add partition columns
        if table.get('PartitionKeys'):
            columns += table.get('PartitionKeys')

        if columns:
            for column in columns:
                schema[column['Name'].lower()] = column['Type'].lower()
            return schema
        else:
            return dict()
    except bex.ClientError:
        logger.error(table_name + ' not present in ' + db_name)


def get_table_columns(table_name: str, db_name: str, region_name: str = 'eu-west-2') -> list:  # No coverage
    schema = get_glue_table_schema(table_name, db_name, region_name)
    if schema:
        return list(schema.keys())


def map_glue_spark_dtypes(schema: dict):
    spark_schema = dict()
    for column_name, dtype in schema.items():
        dtype = str(dtype)
        if '(' in dtype:
            n_dtype = dtype[: dtype.index('(')]
        else:
            n_dtype = dtype
        spark_schema[column_name] = (schema[column_name]
                                     .replace(n_dtype, SPARK_GLUE_DTYPE_MAPPING.get(str(n_dtype), dtype)))

    return spark_schema


def run_glue_job(job_name, region_name="eu-west-2", job_params: dict = None):
    glue_client = boto3.client("glue", region_name=region_name)
    status = "running"
    try:
        job_id = glue_client.start_job_run(JobName=job_name, Arguments=job_params)
        status_detail = glue_client.get_job_run(JobName=job_name, RunId=job_id.get("JobRunId"))
        while status.upper() == "RUNNING":
            status = status_detail.get("JobRun").get("JobRunState")
        return status
    except bex as e:
        raise Exception("boto3 client error in run_glue_job_get_status: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in run_glue_job_get_status: " + e.__str__())


def get_tables(db_name: str, expression: str = '', region_name='eu-west-2', return_partitions=False):
    # catalog_name: str,
    """
    catalog_name: Glue Catalog Name
    db_name: Glue Database Name
    expression: Regex to filter tables
    region_name: Glue catalog region
    return: table list
    """
    next_token = ""
    glue_client = boto3.client("glue", region_name=region_name)
    db_table_list = dict()
    try:
        while True:
            response = glue_client.get_tables(
                DatabaseName=db_name,
                NextToken=next_token,
                Expression=expression
            )

            if 'TableList' in response.keys():
                for tbl in response['TableList']:
                    if "PartitionKeys" in tbl.keys():
                        part_cl_names = [n['Name'] for n in tbl['PartitionKeys']]
                        db_table_list[tbl['Name']] = part_cl_names

                next_token = response.get('NextToken')

                if next_token is None:
                    break
        if not return_partitions:
            db_table_list = db_table_list.keys()

    except bex.ClientError:
        logger.error(db_name + ' not present')
        return dict()

    return db_table_list


def update_col_names(df):
    remove_col_name_space = (column.replace(' ', '_') for column in df.columns)
    df = df.toDF(*remove_col_name_space)
    return df


def run_athena_sql(sql_query, spark_session):
    """
    :param sql_query: query required to run against source database
    :param spark_session: Spark Session
    """
    try:
        df = spark_session.sql(sql_query)
        input_df = update_col_names(df)
        return input_df

    except Exception as e:
        raise Exception("Reading data from athena tables and creation of DataFrame failed")


def run_athena_query(client, query, database, s3_output):
    # Start the Athena query
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output
        }
    )
    return response['QueryExecutionId']


def get_query_status(client, query_execution_id):
    # Wait for the query to complete
    status = 'RUNNING'
    while status in ['RUNNING']:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED':
            print("Query succeeded!")
        elif status == 'FAILED':
            raise Exception(
                "Query failed: {}".format(response['QueryExecution']['Status']['StateChangeReason']))
        else:
            time.sleep(2)


def read_athena_view_boto3(spark, query: str, database: str, s3_output: str, region_name='eu-west-2'):
    athena_client = boto3.client('athena', region_name=region_name)

    query_execution_id = run_athena_query(athena_client, query, database, s3_output)
    get_query_status(athena_client, query_execution_id)

    spark_df = create_dataframe_from_file(spark, s3_output, file_type='csv',
                                          properties={"header": "true", "inferSchema": "true"})

    return spark_df
