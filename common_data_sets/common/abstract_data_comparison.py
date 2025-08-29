from abc import ABC, abstractmethod

from .abstract_glue_job import AbstractGlueJob
from .abstract_pyspark_job import AbstractPySparkJob
import os
from .spark_common_functions import *
from pyspark.sql.functions import col, date_format, to_timestamp, DataFrame
import yaml
from .custom_exceptions import InvalidConfigFileException, EmptyDataSetException
import sys


class AbstractDataComparison(AbstractGlueJob, ABC):
    def __init__(self, params):
        super().__init__()
        self.args = sys.argv
        self.params = params
        self.etl_args = self.get_glue_args()
        self.conf_path = self.etl_args.get("CONF_BUCKET", "")
        self.conf_file_name = self.etl_args.get("CONF_FILE_PREFIX", "")
        self.out_bucket_name = self.etl_args.get("OUTPUT_BUCKET", "")
        self.output_path = self.etl_args.get("OUTPUT_BUCKET_PREFIX", "")

        logger.info(f"{self.conf_path}, {self.conf_file_name}, {self.out_bucket_name}, {self.output_path}")

        tds_conf, correlate_conf, clm_mapping, column_specific_validations = self.get_configs()

        # get source details
        self.tds_dataset_name = tds_conf.get("tds_dataset_name", None)
        self.tds_storage_type = tds_conf.get("tds_storage_type", "glue")
        self.tds_dataset_path = tds_conf.get("tds_dataset_path", None)
        self.tds_dataset_schema = tds_conf.get("correlate_dataset_type", "intl-euro-trusted-data-sets")
        self.tds_partition = tds_conf.get("tds_partition", "")

        # get target details
        self.correlate_dataset_name = correlate_conf.get("correlate_dataset_name", None)
        self.correlate_storage_type = correlate_conf.get("correlate_storage_type", None)
        self.correlate_dataset_path = correlate_conf.get("correlate_dataset_path", None)
        self.correlate_dataset_schema = correlate_conf.get("correlate_dataset_schema", None)
        self.correlate_partitions: str = correlate_conf.get("correlate_partitions", "")

        # get column mapping
        self.column_mapping: dict = clm_mapping
        self.columns_to_compare: list = tds_conf.get("columns_to_compare", ["*"])
        self.metrics_to_compare: list = tds_conf.get("metrics_to_compare", None)
        self.validations_list: list = tds_conf.get("validations_list", ["compare"])
        self.dup_check_cols: list = tds_conf.get("duplicate_check_column_list", [])
        self.distinct_check_cols: list = tds_conf.get("distinct_check_column_list", [])
        # Column specific checks
        self.dimensions_to_metrics_mapping = column_specific_validations.get("dimensions_to_metrics_mapping")
        self.columns_to_validations_mapping = column_specific_validations.get("columns_to_validations_mapping")

        # get filers
        self.tds_data_filters = tds_conf.get("tds_data_filters", None)
        self.correlate_data_filters = correlate_conf.get("correlate_data_filters", None)

        # get date columns
        self.tds_date_column = tds_conf.get("tds_date_column")
        self.tds_date_format = tds_conf.get("tds_date_format")
        self.correlate_date_column = correlate_conf.get("correlate_date_column", None)
        self.correlate_date_format = correlate_conf.get("correlate_date_format", None)
        self.join_on_date = tds_conf.get("join_with_date", "YES")

        # get join condition
        self.join_condition = tds_conf.get("tds_to_correlate_join_condition", "")
        self.join_sep = tds_conf.get("join_sep", ",")

        # get properties
        self.tds_properties = tds_conf.get("tds_read_properties", {})
        self.crr_properties = correlate_conf.get("correlate_read_properties", {})

    def get_configs(self):
        conf_details = get_s3_object(self.conf_path, self.conf_file_name)
        logger.info(f"Reading config file from location {self.conf_path}/{self.conf_file_name}")
        conf_yaml_object = read_yaml(conf_details)
        if {"tds_dataset_conf", "correlate_dataset_conf", "tds_to_correlate_column_mapping"} \
                .issubset(set(conf_yaml_object.keys())):

            return conf_yaml_object.get("tds_dataset_conf", {}), conf_yaml_object.get("correlate_dataset_conf", {}), \
                   conf_yaml_object.get("tds_to_correlate_column_mapping", {}), \
                   conf_yaml_object.get("column_specific_validations", {})
        else:
            logger.error(f"Data Comparison Failed For {self.tds_dataset_name}")
            logger.error("Config file should have tds_dataset_conf, correlate_dataset_conf, column_mapping")
            raise InvalidConfigFileException

    def prepare_data(self, dataset_name: str, storage_type: str, dataset_schema: str = None, filters: str = None,
                     dataset_path: str = None, partition='', properties: dict = None):
        try:
            storage_type = storage_type.lower()
            assert {storage_type}.issubset({"glue", "csv", "txt", "parquet"})
        except AssertionError:
            logger.error('TDS data comparison frame work only supports "glue", "csv", "txt", "parquet" data')

        if storage_type.lower() == 'glue':
            logger.info(f"reading data from schema {dataset_schema} for table {dataset_name}")
            dataset_df = self.read_glue_table(dataset_schema, dataset_name, partition_predicate=partition, force=True)

        elif storage_type.lower() == 'csv' or storage_type == 'txt':
            logger.info(f"reading csv data from {dataset_path} for {dataset_name}")
            dataset_df = create_dataframe_from_file(self.spark, file_name=dataset_path, file_type='csv',
                                                    properties=properties)

        elif storage_type.lower() == 'parquet':
            dataset_df = create_dataframe_from_file(self.spark, file_name=dataset_path, file_type='parquet',
                                                    properties=properties)
            logger.info(f"reading parquet data from {dataset_path} for {dataset_name}")

        else:
            dataset_df = create_empty_df(self.spark)
            logger.info(f"creating empty dataframe {dataset_path} for {dataset_name}")

        if filters:
            logger.info(f"Applying filter {filters} on {dataset_name}")
            dataset_df = apply_filter(dataset_df, filters)

        return dataset_df

    def prepare_tds_data(self, tds_data_filters, properties=None) -> DataFrame:
        try:
            logger.info(f"Creating TDS dataframe with {tds_data_filters}")
            tds_df = self.prepare_data(self.tds_dataset_name, self.tds_storage_type, self.tds_dataset_schema,
                                       tds_data_filters, self.tds_dataset_path, self.tds_partition, properties)

            if is_empty(tds_df):
                raise EmptyDataSetException(f"{self.tds_dataset_name}")

            return tds_df

        except EmptyDataSetException:
            logger.error(f'Unable to get data for tds {self.tds_dataset_name} with filters {tds_data_filters}')

    def prepare_correlate_data(self, correlate_data_filters, properties=None) -> DataFrame:
        try:
            logger.info(f"Creating correlate dataframe with {correlate_data_filters}")
            correlate_df = self.prepare_data(self.correlate_dataset_name, self.correlate_storage_type,
                                             self.correlate_dataset_schema, correlate_data_filters,
                                             self.correlate_dataset_path, self.correlate_partitions, properties)

            if is_empty(correlate_df):
                raise EmptyDataSetException(f"{self.tds_dataset_name}")

            return correlate_df

        except EmptyDataSetException:
            logger.error(
                f'Unable to get data for tds {self.correlate_dataset_name} with filters {correlate_data_filters}')

    @staticmethod
    def merge_df(df1: DataFrame, df2: DataFrame, join_condition, metrics_mapping: dict = None, sep=",") -> DataFrame:
        join_cond = list()
        if isinstance(join_condition, str):
            join_condition = join_condition.split(sep)

        for cond in join_condition:
            if "=" in cond:
                old_col_name = cond.split("=")[0]
                new_col_name = cond.split("=")[1]
            else:
                old_col_name = cond
                new_col_name = cond

            if old_col_name in df2.columns:
                df2 = df2.withColumnRenamed(old_col_name, new_col_name)
                join_cond.append(new_col_name)

            elif old_col_name in df1.columns:
                df1 = df1.withColumnRenamed(old_col_name, new_col_name)
                join_cond.append(new_col_name)
        logger.info(f"Merging dataframe using {join_cond}")
        if not join_cond:
            logger.error(f"Either of the joining columns {join_condition} is not present in data frames")
            logger.error(f"columns in 1st df {str(df1.columns)}")
            logger.error(f"columns in 2nd df {str(df2.columns)}")
            raise InvalidConfigFileException
        merged_df = df1.join(df2, on=join_cond, how="full")

        select_column_list = merged_df.columns

        if metrics_mapping:
            for tds_col, crr_col in metrics_mapping.items():
                select_column_list.remove(crr_col)
                select_column_list.insert(select_column_list.index(tds_col) + 1, crr_col)
                select_column_list.insert(select_column_list.index(crr_col) + 1, f"difference_{tds_col}")

                merged_df = (merged_df
                             .withColumn(f"difference_{tds_col}", merged_df[tds_col] - merged_df[crr_col]))

        return merged_df.select(select_column_list)

    def get_joining_columns(self, join_columns, sep=None) -> list:
        output_columns = list()
        logger.info(f"Getting joining columns from {join_columns}")
        if not sep:
            sep = self.join_sep

        if isinstance(join_columns, str):
            join_columns = join_columns.split(sep)

        for cond in join_columns:
            if "=" in cond:
                output_columns.extend(cond.split("="))
            else:
                output_columns.append(cond)
        logger.info(f"Got Joining columns {list(set(output_columns))}")
        return list(set(output_columns))

    @staticmethod
    def map_columns(column_list: list, column_mapping: dict) -> list:
        """
        column_list: list of columns
        column_mapping: dict with columns in column_list as key & new column name as value
        return: list with new column names
        """
        mapped_columns = [column_mapping[column] for column in column_list if column in list(column_mapping.keys())]
        return list(set(mapped_columns))

    def rename_duplicate_columns(self, df1: DataFrame, col_list: list, prefix: str, ignore_column: list = None) \
            -> DataFrame:
        join_cond_col = []
        if self.join_condition:
            join_cond_col = self.get_joining_columns(self.join_condition)

        logger.info(f"Renaming duplicate columns")
        for _ in df1.columns:
            if _ not in join_cond_col and _ not in ignore_column and _ in col_list:
                df1 = df1.withColumnRenamed(_, f"{prefix}_{_}")
                logger.info(f"Renamed {_} to {prefix}_{_}")

        return df1

    def get_dup_check_cols(self) -> list:
        if not self.dup_check_cols:
            return self.get_joining_columns(self.join_condition)
        return self.dup_check_cols

    def get_dist_check_cols(self) -> dict:
        dist_check_dict = dict()
        if not self.distinct_check_cols:
            for colm in self.columns_to_compare:
                dist_check_dict[colm] = colm
        else:
            for colm in self.distinct_check_cols:
                dist_check_dict[colm] = colm

        return dist_check_dict

    def get_tds_crr_metrics_mapping(self, metrics: list, crr_columns: list, prefix: str):
        tds_crr_metrics = dict()
        for metric in metrics:
            if metric in crr_columns:
                tds_crr_metrics[f"{prefix}_{metric}"] = self.column_mapping[metric]
            else:
                tds_crr_metrics[metric] = self.column_mapping[metric]

        return tds_crr_metrics

    def roll_up_metrix(self, df: DataFrame, columns_to_compare: list, metrics_to_compare: list, date_column: str = None,
                       dt_format: str = None, correlate_data_flag=False) -> DataFrame:

        # creating new list as list is always call by reference
        roll_up_columns = list(columns_to_compare)
        roll_up_metrix = list(metrics_to_compare)

        if correlate_data_flag:
            roll_up_metrix = self.map_columns(roll_up_metrix, self.column_mapping)
            roll_up_columns = self.map_columns(roll_up_columns, self.column_mapping)
            logger.info(f"Rolling up data for correlate dataset for {roll_up_metrix} on {roll_up_columns} "
                        f"using {date_column}")
        else:
            logger.info(f"Rolling up data for tds dataset for {roll_up_metrix} on {roll_up_columns} "
                        f"using {date_column}")

        if date_column:
            logger.info(f"Adding date columns into {roll_up_columns}")
            if dt_format:
                df = (df.withColumn(date_column, to_timestamp(df[date_column], dt_format)))
            df = (df
                  .withColumn('roll_up_year', date_format(col(date_column), 'yyyy'))
                  .withColumn('roll_up_month', date_format(col(date_column), 'MM'))
                  .withColumn('record_count', lit(1)))

            roll_up_columns += ['roll_up_year', 'roll_up_month']
            roll_up_metrix += ['record_count']

        df.fillna(value=0.000001, subset=metrics_to_compare)
        df = df.groupby(roll_up_columns).sum(*roll_up_metrix)

        for column in roll_up_metrix:
            df = df.withColumnRenamed('sum(' + column + ')', column)
        return df

    def create_comparison_report(self, tds_df: DataFrame, correlate_df: DataFrame, columns_to_compare: list,
                                 metrics_to_compare: list, tds_date_column: str, tds_date_format: str,
                                 correlate_date_column: str, correlate_date_format: str, join_condition: str,
                                 is_rolled_up: bool = False) -> DataFrame:

        logger.info(f"Creating comparison for {columns_to_compare}")
        if not is_rolled_up:
            tds_roll_up_df = self.roll_up_metrix(tds_df, columns_to_compare, metrics_to_compare,
                                                 tds_date_column, tds_date_format)

            logger.info(f"rolled up tds data for {columns_to_compare}")
            tds_roll_up_df.show()

            correlate_roll_up_df = self.roll_up_metrix(correlate_df, columns_to_compare, metrics_to_compare,
                                                       correlate_date_column, correlate_date_format,
                                                       correlate_data_flag=True)
            logger.info(f"rolling up correlate data for {columns_to_compare}")
            correlate_roll_up_df.show()

        else:
            tds_roll_up_df = tds_df
            correlate_roll_up_df = correlate_df

        jc = self.get_joining_columns(join_condition)

        tds_roll_up_df = self.rename_duplicate_columns(tds_roll_up_df, correlate_roll_up_df.columns, "tds",
                                                       ignore_column=["roll_up_year", "roll_up_month"] + jc)

        tds_crr_metrics_mapping = self.get_tds_crr_metrics_mapping(metrics_to_compare,
                                                                   correlate_roll_up_df.columns, "tds")

        if self.join_on_date.upper() == "YES" and correlate_date_column and tds_date_column:
            if isinstance(join_condition, str):
                join_condition += ",roll_up_year,roll_up_month"
            elif isinstance(join_condition, list):
                join_condition.extend(["roll_up_year", "roll_up_month"])
            logger.info(f"adding date columns into join condition up correlate data for {columns_to_compare}")

        comparison_df = self.merge_df(tds_roll_up_df, correlate_roll_up_df, join_condition,
                                      metrics_mapping=tds_crr_metrics_mapping)

        return comparison_df

    def create_overall_comparison_report(self, tds_df, correlate_df) -> DataFrame:
        logger.info(f"Creating over all comparison")
        compare_df = self.create_comparison_report(tds_df, correlate_df, self.columns_to_compare,
                                                   self.metrics_to_compare, self.tds_date_column, self.tds_date_format,
                                                   self.correlate_date_column, self.correlate_date_format,
                                                   self.join_condition)

        return compare_df

    @staticmethod
    def get_value_by_key(config: list, column_name: str) -> str:
        for m in config:
            if isinstance(m, dict) and column_name in m.keys():
                op_val = [val for val in m.values()][0]
                return op_val
        return ''

    @staticmethod
    def get_key_by_value(my_dict: dict, val: str) -> str:
        for key, value in my_dict.items():
            if val == value:
                return key
        return ''

    def compare_distinct_values(self, tds_df: DataFrame, correlate_df: DataFrame, column_mapping: dict) -> dict:

        output_df_mapping = dict()
        new_col_mapping = dict()
        logger.info("Started Distinct Value Comparison")
        for key, value in column_mapping.items():
            if key == value and key not in self.metrics_to_compare:
                new_col_mapping[key] = f"correlate_df_{value}"
            elif key not in self.metrics_to_compare:
                new_col_mapping[key] = value

        for column in column_mapping.keys():
            tds_col_name = column
            cr_col_name = self.column_mapping[column]
            logger.info(f"Comparing {tds_col_name} with {cr_col_name}")
            if tds_col_name in tds_df.columns:
                column = (tds_df.select(tds_col_name)
                          .distinct()
                          .withColumn(f"join_{tds_col_name}", col(tds_col_name))
                          .withColumnRenamed(tds_col_name, f"tds_{tds_col_name}")
                          )

                new_col_mapping[tds_col_name] = (correlate_df.select(cr_col_name)
                                                 .distinct()
                                                 .withColumn(f"join_{tds_col_name}", col(cr_col_name))
                                                 )

                column = (column
                          .join(new_col_mapping[tds_col_name], [f"join_{tds_col_name}"], how="full")
                          .drop(f"join_{tds_col_name}")
                          )

                output_df_mapping[f"{tds_col_name}_distinct"] = column
                logger.info(f"Completed Comparing {tds_col_name} with {cr_col_name}")

        return output_df_mapping

    def check_duplicates(self, df1: DataFrame, df2: DataFrame, key_columns: list = None) -> DataFrame:

        if key_columns is None:
            key_columns = ["*"]
            df2_key_col = ["*"]
            logger.info(f"Started Duplicated Value Check on all columns")
        else:
            key_columns = list(set(key_columns))
            df2_key_col = self.map_columns(key_columns, self.column_mapping)
            logger.info(f"Started Duplicate Value Check on {key_columns} & {df2_key_col}")

        tds_key_cols = [cl for cl in key_columns if cl in df1.columns]

        crr_key_cols = [cl for cl in key_columns if cl in df2.columns]

        dup_df1 = (df1
                   .groupBy(tds_key_cols).count()
                   .where(col("count") > 1))

        dup_df2 = (df2
                   .groupBy(crr_key_cols).count()
                   .where(col("count") > 1))

        if len(key_columns) != len(df2_key_col) and "*" not in key_columns:
            for cl in key_columns:
                if cl not in dup_df2.columns:
                    dup_df2 = dup_df2.withColumn(cl, col(self.column_mapping.get(cl)))
                elif cl not in dup_df1.columns:
                    dup_df1 = dup_df1.withColumn(cl,
                                                 col(list(self.column_mapping.keys())
                                                     [list(self.column_mapping.values()).index(cl)])
                                                 )

        dup_df1 = dup_df1.withColumnRenamed("count", "tds_duplicate_counts")
        dup_df2 = dup_df2.withColumnRenamed("count", "corr_duplicate_counts")

        logger.info(f"Completed Duplicated Value Check on {key_columns}")

        return self.merge_df(dup_df1, dup_df2, key_columns)

    def get_tds_column_configs(self, column_name, date_flag=True) -> tuple:
        metric = ""
        metrix = [mt for mt in self.dimensions_to_metrics_mapping[column_name] if isinstance(mt, str)]
        other_conf = [mt for mt in self.dimensions_to_metrics_mapping[column_name] if not isinstance(mt, str)]
        logger.info(f"Getting column leve configuration from tds data for {column_name}")

        # Get metrix as string
        for m in metrix:
            if metrix.index(m) < len(metrix) - 1:
                metric += f"{m},"
            else:
                metric += f"{m}"

        # Get filter condition as string
        filter_cond = self.get_value_by_key(other_conf, "tds_filter")
        date_column = self.get_value_by_key(other_conf, "tds_date_column")
        c_date_format = self.get_value_by_key(other_conf, "tds_date_format")

        if not date_column and date_flag:
            date_column = self.tds_date_column
            c_date_format = self.tds_date_format

        logger.info(f"Returning metric(s) {metric}, filter condition {filter_cond}, date_column {date_column}"
                    f"date format {c_date_format}")

        return metric, filter_cond, date_column, c_date_format

    def get_correlate_column_configs(self, column_name, date_flag=True):
        tds_col_name = self.get_key_by_value(self.column_mapping, column_name)

        metric = ""
        metrix = [mt for mt in self.dimensions_to_metrics_mapping[tds_col_name] if isinstance(mt, str)]
        other_conf = [mt for mt in self.dimensions_to_metrics_mapping[tds_col_name] if not isinstance(mt, str)]

        logger.info(f"Getting column leve configuration from correlate date for {column_name}")

        # Get metrix as string
        for m in metrix:
            if metrix.index(m) < len(metrix) - 1:
                metric += f"{self.column_mapping[m]},"
            else:
                metric += f"{self.column_mapping[m]}"

        # Get filter condition as string
        filter_cond = self.get_value_by_key(other_conf, "correlate_filter")
        date_column = self.get_value_by_key(other_conf, "correlate_date_column")
        c_date_format = self.get_value_by_key(other_conf, "correlate_date_format")

        if not date_column and date_flag:
            date_column = self.correlate_date_column
            c_date_format = self.correlate_date_format

        logger.info(f"Returning metric(s) {metric}, filter condition {filter_cond}, date_column {date_column}"
                    f"date format {c_date_format}")

        return metric, filter_cond, date_column, c_date_format

    def get_column_jc(self, column_name: str) -> str:
        logger.info(f"getting join condition from column level conf for {column_name}")
        column_conf = [mt for mt in self.dimensions_to_metrics_mapping[column_name] if isinstance(mt, dict)]
        join_condition = self.get_value_by_key(column_conf, "join_condition")
        return join_condition

    def get_validation_list(self, column_name: str = None) -> list:
        if column_name:
            return list(self.columns_to_validations_mapping[column_name])
        return self.validations_list

    @abstractmethod
    def start_data_comparison(self):
        pass
