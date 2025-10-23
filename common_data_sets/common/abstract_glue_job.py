import sys
from abc import ABC
from .abstract_pyspark_job import AbstractPySparkJob
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from common_data_sets.common.spark_common_functions import create_empty_df, logger
from common_data_sets.common.custom_exceptions import GlueTableNotFoundExceptions
import os
from .spark_common_functions import is_non_empty, is_empty, get_glue_table_location
from functools import reduce


class AbstractGlueJob(AbstractPySparkJob, ABC):
    def __init__(self, params=None):
        super().__init__()
        self.args = sys.argv
        self.params = params
        self.etl_args = self.get_glue_args()
        self.bucket_name = self.etl_args.get("OUTPUT_BUCKET", "")
        self.code_bucket = self.etl_args.get("CONF_BUCKET", "")
        self.code_path = '/'.join(self.etl_args.get("CONF_FILE_PREFIX", "").split('/')[::-1][1:][::-1])
        self.output_path = self.etl_args.get("OUTPUT_BUCKET_PREFIX", "")
        self.db_name = self.etl_args.get("CATALOG_DB_NAME", "default")
        self.DEBUG = self.etl_args.get("DEBUG", False)
        self.part_predicate = self.etl_args.get("PARTITION_PREDICATE", "")
        self.filter_id_list = self.etl_args.get("FILTER_ID_LIST", "ALL")

    @property
    def job_type(self) -> str:
        return "aws-glue-pyspark"

    def get_glue_args(self) -> dict:  # pragma: no cover
        if self.args and self.params:
            return getResolvedOptions(self.args, self.params)
        else:
            return dict()

    def init_spark_job(self):
        spark = self.glue_context.sparkSession  # SparkSession.builder.appName(self.app_name).getOrCreate()
        # spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        spark.conf.set("appName", self.app_name)
        return spark

    @property
    def glue_context(self):
        return GlueContext(self.sc)

    @property
    def job(self):
        return Job(self.glue_context)

    def commit_job(self):  # no cover
        """
        :return: None
        This function will start execution of Glue job
        """
        self.job.commit()

    def pre_validations(self):
        pass

    def post_validations(self, *args):
        pass

    def execute(self):
        self.logger.info("Starting pre-validations for " + self.app_name)
        self.pre_validations()
        self.logger.info("Completed pre-validations for " + self.app_name)
        self.logger.info("Running the module " + self.app_name)
        self._execute()
        self.logger.info("Execution completed for module " + self.app_name)

    @property
    def is_debug(self):
        if self.DEBUG == "true" or self.DEBUG == True or self.DEBUG == "True":
            return True
        return False

    def read_table(self, table_class, force: bool = False,dynamic_frame=False, s3_path_read=False,
                   region_name="eu-west-2", **kwargs) -> DataFrame:  # no cover
        """
        :param table_class: Glue table class/function
        :param force: bool
        :return:
        """
        table = table_class(**kwargs)

        df = self.read_glue_table(table.schema_name, table.table_name, table.partition_predicate, table.columns,
                                  table.distinct_flag, force, dynamic_frame, s3_path_read, region_name)
        return df

    def read_table_list(self, table_class, force: bool = False, dynamic_frame=False, s3_path_read=False,
                        region_name="eu-west-2", **kwargs) -> DataFrame:  # no cover
        """
        :param table_class: Glue table class/function
        :param force: bool
        :return:
        """
        table = table_class(**kwargs)

        tbl_df_list = []

        for tbl in table.table_list:
            tbl = self.read_glue_table(table.schema_name, tbl, table.partition_predicate, table.columns,
                                       False, force, dynamic_frame, s3_path_read, region_name)
            if is_non_empty(tbl):
                tbl_df_list.append(tbl)

        table_union_df = reduce(DataFrame.unionAll, tbl_df_list)

        if table.distinct_flag:
            self.logger.info(f"Distinct flag set to true Distinct record count: {table_union_df.distinct().count()} "
                             f"Non-Distinct record count: {table_union_df.count()}")
            table_union_df = table_union_df.distinct()


        return table_union_df

    def read_glue_table(self, schema_name, table_name, partition_predicate="", columns="", distinct_flag=False,
                        force: bool = False, dynamic_frame=False, s3_path_read=False,region_name="eu-west-2", **kwargs):  # no cover
        """
        :param schema_name: str
        :param table_name: str
        :param partition_predicate: str
        :param columns: str
        :param distinct_flag: bool
        :param force: bool
        :return: DataFrame
        """
        try:
            if dynamic_frame:

                logger.info(f"Creating dynamic df for table {table_name} in {schema_name} db for {partition_predicate}")
                if not partition_predicate:
                    partition_predicate = ""

                df = (self.glue_context.create_dynamic_frame.from_catalog(
                    database=schema_name,
                    table_name=table_name,
                    push_down_predicate=partition_predicate,
                    additional_options={"inferSchema": "true"})).toDF()
                if columns:
                    df = df.select(columns)  # _fields(paths=columns)
                if distinct_flag:
                    self.logger.info(f"returning distinct records for {table_name}"
                                     f"distinct record count: {df.distinct().count()}"
                                     f"non-distinct record count: {df.count()}")
                    df = df.distinct()

            elif s3_path_read:
                table_full_path = get_glue_table_location(schema_name, table_name, region_name)
                if partition_predicate:
                    if len(partition_predicate.split(' and ')) > 1:
                        partition_predicate = '/'.join(partition_predicate.split(' and '))
                    table_full_path = os.path.join(table_full_path, partition_predicate)
                    self.logger.info(f"Creating Spark Dataframe from S3 {table_full_path}")
                if columns:
                    df = self.spark.read.parquet(table_full_path).select(columns)
                else:
                    df = self.spark.read.parquet(table_full_path)
                if distinct_flag:
                    self.logger.info(f"returning distinct records for {table_name}"
                                     f"distinct record count: {df.distinct().count()}"
                                     f"non-distinct record count: {df.count()}")
                    df = df.distinct()

            else:
                logger.info(f"Creating Spark Dataframe for table {table_name} in {schema_name} db with filters "
                            f"{partition_predicate}")
                logger.warn("############### SPARK SQL READ FAILS SILENTLY IF YOUR JOB GETS STUCK CHANGE READ TYPE "
                            "###############")

                if not columns:
                    columns = '*'
                else:
                    columns = ','.join(columns)

                query = f"select {columns} from `{schema_name}`.`{table_name}`"

                if partition_predicate:
                    query += f" where {partition_predicate}"

                if distinct_flag:
                    self.logger.info(f"returning distinct records for {table_name} "
                                     f"distinct record count: {self.spark.sql(query).distinct().count()} "
                                     f"non-distinct record count: {self.spark.sql(query).count()}")
                    query = query.replace("select", "select distinct")
                    df = self.spark.sql(query)

                else:
                    df = self.spark.sql(query)

                logger.info(f"Running query: {query}")

            if is_empty(df):
                if force:
                    raise GlueTableNotFoundExceptions(table_name)
                else:
                    self.logger.warn(f"creating an empty dataframe for {schema_name}.{table_name}")
                    df = create_empty_df(self.spark)

        except:  # EntityNotFoundException
            if force:
                raise GlueTableNotFoundExceptions(table_name)
            df = create_empty_df(self.spark)
            self.logger.warn(f"creating an empty dataframe for {schema_name}.{table_name}")
        return df

    def read_athena_view_spark(self, view_class, filter_condition=None, **kwargs):
        view_details = view_class(**kwargs)
        view_name = view_details.view_name
        columns = view_details.columns
        db_name = view_details.schema_name
        distinct_flag = view_details.distinct_flag

        logger.info(f"Creating Spark Dataframe athena view  for {view_name} from {db_name}")


        if not columns:
            columns = '*'
        else:
            columns = ','.join(columns)

        query = f"select {columns} from `{db_name}`.`{view_name}`"

        if filter_condition:
            if len(filter_condition.split(',')) > 1:
                filter_condition = ' and '.join(filter_condition.split(','))
            query += f" where {filter_condition}"

        athena_view_df = self.spark.sql(query)

        if distinct_flag:
            athena_view_df = athena_view_df.distinct()

        return athena_view_df
