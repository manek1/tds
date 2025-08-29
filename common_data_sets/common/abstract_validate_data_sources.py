from abc import ABC

from .spark_common_functions import *
from common_data_sets.common.abstract_glue_job import AbstractGlueJob
from common_data_sets.common.custom_exceptions import *
import botocore.exceptions as bex
from pyspark.sql import types as T
from pyspark.sql import functions as F
import sys


class AbstractValidateDataSource(AbstractGlueJob, ABC):
    def __init__(self, params):
        super().__init__()
        self.args = sys.argv
        self.params = params
        self.etl_args = self.get_glue_args()
        self.conf_path = self.etl_args.get("CONF_BUCKET", "")
        self.conf_file_name = self.etl_args.get("CONF_FILE_PREFIX", "")
        conf_details = self.get_configs()
        self.datasource_type = conf_details.get('datasource_type')
        self.database_name = conf_details.get('database_name')
        self.table_list = conf_details.get('table_list')
        self.out_bucket_name = conf_details.get('out_bucket_name')
        if not self.out_bucket_name:
            self.out_bucket_name = self.etl_args.get("OUTPUT_BUCKET", "")
        self.tables_part_mapping = dict()
        self.tables_to_column_mapping = conf_details.get('tables_to_column_mapping')
        self.tables_to_filter_mapping = conf_details.get('tables_to_filter_mapping')
        self.ignore_table_list = conf_details.get('ignore_table_list', list())
        self.counts_on_partitions = conf_details.get("counts_on_partitions")
        self.output_path = conf_details.get("output_path")
        self.output_table_name = conf_details.get("output_table_name", None)
        self.output_schema = conf_details.get("output_schema")
        self.output_name = conf_details.get("output_dataset_name")
        self.output_type = conf_details.get("output_type")
        self.force_first_run = conf_details.get("force_first_run")
        self.threshold = conf_details.get("threshold")
        self.ids_filter = conf_details.get("ids_filter", "")
        self.thr_val_map = conf_details.get("table_to_threshold_mapping")
        self.is_first_run = False
        self.spark = self.init_spark_job()
        self.spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    def get_configs(self):  # No Coverage
        """
        This method will read the yaml config file & validated if mandatory configs are present
        returns: yaml object
        """
        logger.info(f"Reading config file from location {self.conf_path}/{self.conf_file_name}")
        conf_details = get_s3_object(self.conf_path, self.conf_file_name)
        if not conf_details:
            logger.error(f"Conf file not present {self.conf_path}/{self.conf_file_name} exiting with error")
            raise InvalidConfigFileException

        conf_yaml_object = read_yaml(conf_details)
        logger.info(f"Starting count validations for {conf_yaml_object}")
        if {"database_name", "table_list", "datasource_type", "output_path"}.issubset(set(conf_yaml_object.keys())):

            return conf_yaml_object
        else:
            logger.error(f"data comparison failed for {self.app_name} because of wrong config file")
            logger.error("Config file should have database_name", "table_list", "datasource_type", "output_path")
            raise InvalidConfigFileException(config_file_name=self.conf_file_name)

    def get_previous_record_counts(self, partition, properties=None):  # No Coverage
        """
        This method will get the record counts from previous comparison
        returns: Spark DataFrame
        """
        if properties is None:
            properties = {"header": True}

        storage_type = self.output_type.lower()
        try:
            assert {storage_type}.issubset({"glue", "csv", "txt", "parquet"})
        except AssertionError:
            logger.error('Source Data Validation frame work only supports "glue", "csv", "txt", "parquet" output')
            return None

        if storage_type.lower() == 'glue' and self.output_schema:
            try:
                logger.info(f"reading data from schema {self.output_schema} for table {self.output_name}")
                dataset_df = self.read_glue_table(self.output_schema, self.output_name, partition_predicate=partition)
            except GlueTableNotFoundExceptions:
                dataset_df = create_empty_df(self.spark)
                logger.error("Object Doesn't exists, considering as first run")

        elif storage_type.lower() == 'csv' or storage_type == 'txt':
            try:
                logger.info(f"reading csv data from {self.output_path} for {self.output_name}")
                full_output_path = os.path.join(self.output_path, self.output_name)
                dataset_df = create_dataframe_from_file(self.spark, file_name=full_output_path, file_type='csv',
                                                        properties=properties)
            except bex.ClientError as ce:
                if ce.response['Error']['Code'] == "404":
                    logger.error("Object Doesn't exists, considering as first run")
                dataset_df = create_empty_df(self.spark)

        elif storage_type.lower() == 'parquet':
            try:
                logger.info(f"reading parquet data from {self.output_path} for {self.output_name}")
                full_output_path = os.path.join(self.output_path, self.output_name)
                dataset_df = create_dataframe_from_file(self.spark, file_name=full_output_path, file_type='parquet')
            except bex.ClientError as ce:
                if ce.response['Error']['Code'] == "404":
                    logger.error("Object Doesn't exists, considering as first run")
                dataset_df = create_empty_df(self.spark)

        else:
            dataset_df = create_empty_df(self.spark)
            logger.info(f"creating empty dataframe {self.output_path} for {self.output_name}")

        if is_empty(dataset_df):
            self.is_first_run = True

        return dataset_df

    def generate_count_query(self):  # No Coverage
        """
        This method will generate a sql query for getting counts from tables based on configurations
        return: String
        """
        cq = ""
        if self.table_list:
            logger.info(f"table list {str(self.table_list)}")
            for tbl in self.table_list:
                if tbl in self.ignore_table_list:
                    continue
                part_cols = list_to_str(self.tables_part_mapping.get(tbl))
                if self.table_list.index(tbl) < len(self.table_list) - 1:
                    if part_cols:
                        cq += f"""select '{tbl}' as table_name, {part_cols}, count(1) as record_count from `{self.database_name}`.`{tbl}` {self.ids_filter } group by {part_cols} union """

                    else:
                        cq += f"""select '{tbl}' as table_name, count(1) as record_count from  `{self.database_name}`.`{tbl}` {self.ids_filter } union all """

                else:
                    if part_cols:
                        cq += f"""select '{tbl}' as table_name, {part_cols}, count(*) as record_count from `{self.database_name}`.`{tbl}` {self.ids_filter } group by {part_cols} """

                    else:
                        cq += f"""select '{tbl}' as table_name, count(*) as record_count from `{self.database_name}`.`{tbl}` {self.ids_filter } """
        logger.info(f"Returning query {cq}")
        return cq

    def get_current_record_counts(self):  # No Coverage
        """
        This method will get the current record counts  from configured tables
        returns: Spark DataFrame
        """
        if len(self.table_list) == 1 and "all" in [t.lower() for t in self.table_list]:
            if self.datasource_type == "glue":
                if self.counts_on_partitions:
                    self.tables_part_mapping = get_tables(db_name=self.database_name, return_partitions=True)
                    self.table_list = list(self.tables_part_mapping.keys())
                else:
                    self.table_list = list(get_tables(db_name=self.database_name))

            # Can add more data source here

        elif not self.tables_part_mapping and self.counts_on_partitions:
            temp_tables_part_mapping = get_tables(db_name=self.database_name, return_partitions=True)
            logger.info(f"Table to partition mapping {str(temp_tables_part_mapping)}")
            for tbl in self.table_list:
                self.tables_part_mapping[tbl] = temp_tables_part_mapping[tbl]

        if self.table_list:
            logger.info(f"Running count validation of tables {self.table_list}")
        else:
            raise EmptyDataSetException(self.database_name, 'Database is not present or no tables in db')

        rec_query = self.generate_count_query()

        if self.datasource_type == "glue":
            rec_cnt_df = run_athena_sql(rec_query, self.spark)
        else:
            rec_cnt_df = create_empty_df(self.spark)

        return rec_cnt_df

    def compare_record_counts(self, partition='', properties=None):  # No Coverage
        """
        This method will create a comparison report for list of tables
        returns: Spark DataFrame
        """
        prev_count_df = self.get_previous_record_counts(partition, properties=properties)
        crr_count_df = self.get_current_record_counts()

        if self.is_first_run or self.force_first_run:
            logger.info("No previous comparison found considering it as a first run")
            op_df = (crr_count_df
                     .withColumnRenamed("record_count", "curr_rec_count")
                     .withColumn("prev_rec_count", lit(0).cast(T.IntegerType()))
                     .withColumn("increase_count", lit(0).cast(T.IntegerType()))
                     .withColumn("per_diff", lit(0).cast(T.IntegerType()))
                     .withColumn("load_time", F.current_timestamp()))

            return op_df

        join_cols = ["table_name"]

        part_cols = list(set(flat_list(list(self.tables_part_mapping.values()))))

        logger.info(str(part_cols))
        logger.info(str(prev_count_df.columns))

        if self.tables_part_mapping:
            for clm in part_cols:
                if clm in prev_count_df.columns:
                    logger.info(f"Adding joining columns {str(part_cols)}")
                    join_cols.append(clm)

        logger.info(f"Joining columns {str(join_cols)}")
        op_df = prev_count_df.join(crr_count_df, on=join_cols, how="full")

        op_df = (op_df
                 .drop(op_df.prev_rec_count)
                 .withColumnRenamed("curr_rec_count", "prev_rec_count")
                 .withColumnRenamed("record_count", "curr_rec_count")
                 .withColumn("increase_count", col("curr_rec_count") - col("prev_rec_count"))
                 .withColumn("per_diff",
                             ((col("curr_rec_count") - col("prev_rec_count")) / col("prev_rec_count")) * 100)
                 .withColumn("load_time", F.current_timestamp()))

        select_col_list = op_df.columns
        select_col_list.remove('curr_rec_count')
        select_col_list.insert(select_col_list.index('prev_rec_count'), 'curr_rec_count')

        op_df.printSchema()
        op_df.select(select_col_list).show(5)

        return op_df.select(select_col_list)

    def apply_threshold(self, df: DataFrame, diff_col='per_diff'):
        failed_threshold = list()
        ignore_tables = list()

        if self.threshold:
            if self.thr_val_map:
                ignore_tables.extend(self.thr_val_map.keys())

            n_df1 = (df
                     .where(((col(diff_col) > self.threshold) | (col(diff_col) < -self.threshold))
                            & ~(col('table_name').isin(ignore_tables)))
                     .select('table_name')
                     .distinct()
                     )

            failed_threshold.extend(df_to_list(n_df1, 'table_name'))

        if self.thr_val_map:
            for clm, thr in self.thr_val_map.items():
                logger.info(f"Applying threshold {thr} on {clm}")
                n_df = (df
                        .where(((col(diff_col) > thr) | (col(diff_col) < -thr)) & (col('table_name') == clm))
                        .select('table_name')
                        .distinct()
                        )

                failed_threshold.extend(df_to_list(n_df, 'table_name'))

        failed_threshold = flat_list(failed_threshold)

        if failed_threshold:
            return failed_threshold
        else:
            logger.info(f"Completed the threshold validation for {self.app_name}")
