from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType
from .spark_common_functions import logger, df_to_list, get_table_columns, struct_to_dict, get_glue_table_schema, \
    compare_list, map_glue_spark_dtypes, get_last_year_month, get_last_year, is_non_empty
from pyspark.sql import Window
from pyspark.sql.functions import col, date_format, to_timestamp, concat, lit, lag, when
from abc import ABC, abstractmethod
from common_data_sets.common.custom_exceptions import InvalidTableSchemaError, ColumnNotFoundError, \
    GlueTableNotFoundExceptions


class AbstractDataQualityChecks(ABC):
    def __init__(self, df=None, module_name=None):
        self._module_name = module_name
        self.df: DataFrame = df

    def check_duplicate(self, dedup_col: list = None):
        if dedup_col is None:
            dedup_col = ["*"]
        return self.df.select(dedup_col).distinct().count() == self.df.select(dedup_col).count()

    def check_null_data(self, null_col: list):
        for column in null_col:
            if self.df.where(col(column).isNull()).count() > 0:
                return False
        return True

    def check_df_schema(self, schema: StructType):
        return self.df.schema == schema

    def check_values(self, col_name: str, val_list: list):
        if set(val_list).issubset(set(df_to_list(self.df, col_name))):
            return True
        else:
            return False

    def check_range(self, col_name, start_value, end_value):
        return self.df.where(col(col_name).between(start_value, end_value)).count()

    def check_count(self, expected_count, test="equal"):
        df_cnt = self.df.count()
        if test.lower() == "equal":
            return df_cnt == expected_count
        elif test.lower() == "greater":
            return df_cnt >= expected_count
        elif test.lower() == "less":
            return df_cnt <= expected_count

    @staticmethod
    def check_table_columns(table, force=False):  # No coverage
        columns = table.columns
        actual_columns = get_table_columns(table.table_name, table.schema_name)
        if actual_columns:
            if set(columns).issubset(actual_columns):
                logger.info("All required columns are present in table -> " + table.table_name)
                return True
            else:
                logger.error("Required columns " + str(columns) + " columns present " + str(actual_columns))
                if force:
                    raise ColumnNotFoundError(table.table_name, str(compare_list(columns, actual_columns)))
                return False
        else:
            logger.error("Table Not found " + table.__repr__)
            if force:
                raise GlueTableNotFoundExceptions(table)
            return False

    @staticmethod
    def check_table_schema(table, force=False):  # No coverage
        schema = struct_to_dict(table.schema)
        table_schema = get_glue_table_schema(table.table_name, table.schema_name)
        table_schema = map_glue_spark_dtypes(table_schema)

        for col_name, dtype in schema.items():
            if table_schema.get(col_name.lower()) != str(dtype).replace('()', ''):
                logger.error(f"Expected {str(dtype)} got {table_schema.get(col_name.lower())}  for column {col_name} "
                             f"in table {table.table_name}")
                if force:
                    raise InvalidTableSchemaError(table.table_name)
                return False
        logger.info("Table schema & required schema is matching for table -> " + table.table_name)
        return True

    def create_df_summary(self, archive_df: DataFrame, key_columns: list, metrics: list, date_column: str = None,
                          dt_format: str = None):
        """
        :param archive_df: Archive Data
        :param key_columns: list of columns to be grouped on
        :param metrics: list of columns need to be roller up
        :param date_column: Date column for report generation
        :param dt_format: date format
        :return: SparkDataFrame
        """
        mom_select_column_list = key_columns + ['report_year', 'report_month']
        yoy_select_column_list = key_columns + ['report_year']
        date_col = date_column
        current_df = self.df
        yoy_key_cols = key_columns + ['report_year']
        mom_key_cols = key_columns + ['report_year', 'report_month']
        metrics.append('record_count')
        yoy_current_df = self.rollup_metrics(current_df, yoy_key_cols, metrics, 'current_yoy_sum_',
                                             date_column=date_col, dt_format=dt_format)
        yoy_previous_df = self.rollup_metrics(archive_df, yoy_key_cols, metrics, 'previous_yoy_sum_',
                                              date_column=date_col, dt_format=dt_format)
        mom_current_df = self.rollup_metrics(current_df, mom_key_cols, metrics, 'current_mom_sum_',
                                             date_column=date_col, dt_format=dt_format)
        mom_previous_df = self.rollup_metrics(archive_df, mom_key_cols, metrics, 'previous_mom_sum_',
                                              date_column=date_col, dt_format=dt_format)

        yoy_summary_df = yoy_current_df.join(yoy_previous_df, on=yoy_key_cols, how="inner")
        mom_summary_df = mom_current_df.join(mom_previous_df, on=mom_key_cols, how="inner")

        for column in metrics:
            mom_select_column_list.extend(['current_mom_sum_' + column, 'previous_mom_sum_' + column,
                                           'per_change_mom_sum_' + column])

            yoy_select_column_list.extend(['current_yoy_sum_' + column,
                                           'previous_yoy_sum_' + column, 'per_change_yoy_sum_' + column])

            yoy_summary_df = yoy_summary_df.withColumn('per_change_yoy_sum_' + column,
                                                       lit(((yoy_summary_df["current_yoy_sum_" + column] -
                                                             yoy_summary_df["previous_yoy_sum_" + column])
                                                            / yoy_summary_df["previous_yoy_sum_" + column]) * 100))

            mom_summary_df = mom_summary_df.withColumn('per_change_mom_sum_' + column,
                                                       lit(((mom_summary_df["current_mom_sum_" + column] -
                                                             mom_summary_df["previous_mom_sum_" + column])
                                                            / mom_summary_df["previous_mom_sum_" + column]) * 100))
        # get data other than current year for yoy summary df
        # get data only current year data for mom summary df
        last_year = get_last_year()
        yoy_summary_df = yoy_summary_df.filter(col('report_year') <= last_year)

        mom_summary_df = mom_summary_df.filter(col('report_year') > last_year)

        return yoy_summary_df.select(yoy_select_column_list), mom_summary_df.select(mom_select_column_list)

    @staticmethod
    def rollup_metrics(df, key_cols, metrics, renaming_key, date_column: str = None, dt_format: str = None,
                       ignore_date: bool = True):

        if date_column:
            if dt_format:
                df = (df.withColumn(date_column, to_timestamp(df[date_column], dt_format)))
            df = (df
                  .withColumn('report_year', date_format(col(date_column), 'yyyy'))
                  .withColumn('report_month', date_format(col(date_column), 'MM'))
                  .withColumn('record_count', lit(1)))
        else:
            logger.error("No Date Column/Year Column Passed, Returning empty dataframe")
            return False

        if not ignore_date:
            key_cols += ['report_year', 'report_month', 'report_quarter']

        df.fillna(value=0.000001, subset=metrics)
        df = df.groupby(key_cols).sum(*metrics)
        for column in metrics:
            df = df.withColumnRenamed('sum(' + column + ')', renaming_key + column)
        return df

    def calculate_yoy_change(self, key_columns: list, metrics: list, date_column: str = None, dt_format: str = None,
                             year_column: str = None):
        """
        :param key_columns: list of columns to be grouped on
        :param metrics: list of columns need to be roller up
        :param date_column: date column name
        :param dt_format: date format
        :param year_column: year columns
        :return: SparkDataFrame
        """
        yoy_df = self.df
        metrics = metrics.copy()

        if date_column:
            if date_format:
                yoy_df = (yoy_df.withColumn(date_column, to_timestamp(yoy_df[date_column], dt_format)))
            yoy_df = (yoy_df.withColumn('report_year', date_format(col(date_column), 'yyyy')))

        elif year_column:
            yoy_df = (yoy_df.withColumn('report_year', yoy_df[year_column]))

        else:
            logger.error("No Date Column/Year Column Passed, Returning empty dataframe")
            return False

        yoy_df = yoy_df.withColumn('record_count', lit(1))
        metrics.append('record_count')

        select_column_list = key_columns + ['report_year']
        yoy_df = yoy_df.groupBy(select_column_list).sum(*metrics)

        for column in metrics:
            metrics[metrics.index(column)] = 'yoy_sum_' + column
            yoy_df = yoy_df.withColumnRenamed('sum(' + column + ')', 'yoy_sum_' + column)
            select_column_list.extend(['yoy_sum_' + column, 'per_change_yoy_sum_' + column])

        yoy_df = yoy_df.fillna(value=0.000001, subset=metrics)

        select_column_list.append('previous_year')

        yoy_window = Window.partitionBy(*key_columns).orderBy('report_year')

        yoy_df = yoy_df.withColumn('previous_year', lit(lag(col('report_year'), 1).over(yoy_window)))

        for column in metrics:
            yoy_df = (yoy_df
                      .withColumn('per_change_' + column,
                                  when(
                                      lag(column, 1).over(yoy_window) != 0,
                                      lit(
                                          (
                                                  (yoy_df[column] - lag(column, 1).over(yoy_window))
                                                  / lag(column, 1).over(yoy_window)
                                          ) * 100
                                      )
                                  ).otherwise(None)
                                  )
                      )

        return yoy_df.select(select_column_list).where(col('previous_year').isNotNull())

    def calculate_mom_change(self, key_columns: list, metrics: list,
                             year_column: str = None, month_column: str = None, date_column: str = None,
                             dt_format: str = None):
        """
        :param month_column: month column name
        :param key_columns: list of columns to be grouped on
        :param metrics: list of columns need to be roller up
        :param date_column: date column name
        :param dt_format: date format
        :param year_column: year columns
        :return: SparkDataFrame
        """

        mom_df = self.df
        metrics = metrics.copy()

        if year_column and month_column:
            mom_df = mom_df.withColumn('report_year', lit(mom_df[year_column])
                                       .cast(StringType()))
            mom_df = mom_df.withColumn('report_month', lit(mom_df[month_column])
                                       .cast(StringType()))
        elif date_column:
            if date_format:
                mom_df = (mom_df.withColumn(date_column, to_timestamp(mom_df[date_column], dt_format)))

            mom_df = (mom_df.withColumn('report_year', date_format(col(date_column), 'yyyy'))
                      .withColumn('report_month', date_format(col(date_column), 'MM')))
        else:
            logger.error("No Date Month Column Passed, Exiting with error")
            return False

        mom_df = mom_df.withColumn('record_count', lit(1))
        metrics.append('record_count')

        select_column_list = key_columns + ['report_year', 'report_month']

        mom_df = mom_df.groupBy(select_column_list).sum(*metrics)
        select_column_list.extend(['previous_year', 'previous_month'])

        for column in metrics:
            mom_df = mom_df.withColumnRenamed('sum(' + column + ')', 'mom_sum_' + column)
            metrics[metrics.index(column)] = 'mom_sum_' + column
            select_column_list.extend(['mom_sum_' + column, 'per_change_mom_sum_' + column])

        mom_df = mom_df.fillna(value=0.0000001, subset=metrics)

        mom_window = Window.partitionBy(*key_columns).orderBy(col('report_year'), col('report_month'))

        mom_df = (mom_df
                  .withColumn('previous_year', lit(lag('report_year', 1).over(mom_window)))
                  .withColumn('previous_month', lit(lag('report_month', 1).over(mom_window)))
                  )

        for column in metrics:
            mom_df = (mom_df
                      .withColumn('per_change_' + column,
                                  when(lag(column, 1).over(mom_window) != 0,
                                       lit(((mom_df[column] - lag(column, 1).over(mom_window)) / lag(column, 1).over(
                                           mom_window)) * 100))
                                  .otherwise(None))
                      )

        return mom_df.select(select_column_list).where(col('previous_month').isNotNull())

    def apply_threshold(self, spark, final_report_df: DataFrame, __metrics__: list, __threshold__: list):
        """
        :param spark: SparkSession
        :param final_report_df: SparkDataFrame
        :param __metrics__: list of columns need to be roller up
        :param __threshold__: list of thresholds
        :return: SparkDataFrame
        """
        thr_df = spark.createDataFrame(__threshold__)
        year_filter = get_last_year(int(thr_df.select("prior_years").collect()[0][0]))
        year_month_filter = get_last_year_month(int(thr_df.select("prior_months").collect()[0][0]))
        prior_year_threshold = int(thr_df.select("prior_year_threshold").collect()[0][0])
        prior_month_threshold = int(thr_df.select("prior_month_threshold").collect()[0][0])
        current_year_threshold = int(thr_df.select("current_year_threshold").collect()[0][0])
        current_month_threshold = int(thr_df.select("current_month_threshold").collect()[0][0])

        current_year_filter, current_month_filter = self.create_yoy_mom_filter(__metrics__, current_year_threshold,
                                                                               current_month_threshold)
        prior_year_filter, prior_month_filter = self.create_yoy_mom_filter(__metrics__, prior_year_threshold,
                                                                           prior_month_threshold)

        final_report_df.createOrReplaceTempView("threshold_final_report")

        prior_year_report_df = spark.sql(f"select * from threshold_final_report where ({prior_year_filter}) and "
                                         f"report_year is not null and report_year <= {year_filter}"
                                         )

        print(f"select * from threshold_final_report where ({prior_year_filter}) and "
              f"report_year is not null and report_year <= {year_filter}"
              )

        current_year_report_df = spark.sql(f"select * from threshold_final_report where ({prior_month_filter} and "
                                           f"report_year > {year_filter} "
                                           f"and concat(report_year, report_month) <= {year_month_filter}) "
                                           f"or (concat(report_year, report_month) > {year_month_filter} and "
                                           f"{current_month_filter})")

        print(f"select * from threshold_final_report where ({prior_month_filter} and "
              f"report_year > {year_filter} "
              f"and concat(report_year, report_month) <= {year_month_filter}) "
              f"or (concat(report_year, report_month) > {year_month_filter} and "
              f"{current_month_filter})"
              )

        if is_non_empty(prior_year_report_df):
            logger.error("post validations failed, " + str(prior_year_report_df.count()))
        if is_non_empty(current_year_report_df):
            logger.error("post validations failed, " + str(current_year_report_df.count()))

        return prior_year_report_df.union(current_year_report_df).distinct()

    @staticmethod
    def create_yoy_mom_filter(__metrics__: list, yoy_thr, mom_thr):
        mom_filter_cond = ""
        yoy_filter_cond = ""
        for column in __metrics__:
            if __metrics__.index(column) < len(__metrics__) - 1:
                yoy_filter_cond += f"abs(per_change_yoy_sum_{column}) > {yoy_thr} or "
                mom_filter_cond += f"abs(per_change_mom_sum_{column}) > {mom_thr} or "
            else:
                yoy_filter_cond += f"abs(per_change_yoy_sum_{column}) > {yoy_thr} "
                mom_filter_cond += f"abs(per_change_mom_sum_{column}) > {mom_thr} "

        return yoy_filter_cond, mom_filter_cond

    @staticmethod
    def merge_yoy_mom_change(mom_df: DataFrame, yoy_df: DataFrame, join_condition: list, join_type: str):
        final_change_df = yoy_df.join(mom_df, on=join_condition, how=join_type)
        return final_change_df

    @abstractmethod
    def validate(self, *args):
        pass
