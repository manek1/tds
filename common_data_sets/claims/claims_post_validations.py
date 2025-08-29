from common_data_sets.common.spark_common_functions import logger, is_empty, format_s3_path
from .table_mapping import claims
from pyspark.sql.functions import when, col, lit
from common_data_sets.common.abstract_data_quality_checks import AbstractDataQualityChecks
from common_data_sets.common.custom_exceptions import PostValidationError


class ClaimsPostValidations(AbstractDataQualityChecks):
    def __init__(self, claims_df, spark=None, **kwargs):
        super().__init__(df=claims_df, module_name='claims')
        self.spark = spark
        self.bucket_name = kwargs.get('bucket_name')
        self.error_path = "analytics-dataset/error"

    def validate(self, archive_df):
        if self.check_df_schema(schema=claims().schema):
            logger.info("Completed the schema validation for -> " + self._module_name)
        else:
            logger.error("DF schema is not matching with expected schema -> " + self._module_name)
            logger.error(f"Expected Schema -> {str(claims().schema)} '\n'"
                         f" Actual Schema -> {self.df._jdf.schema().treeString()}")
            raise PostValidationError(self._module_name)

        key_values = self.threshold_validation(archive_df)

        return key_values

    def threshold_validation(self, archive_df):
        mom_report_df, yoy_report_df = self.create_df_summary(archive_df, claims().key_columns,
                                                              claims().metrics,
                                                              date_column=claims().date_column)

        final_report_df = self.merge_yoy_mom_change(mom_report_df, yoy_report_df, ["sourcesystemid", 'report_year'],
                                                    join_type='full')

        threshold_df = self.apply_threshold(self.spark, final_report_df, claims().metrics, claims().threshold)

        if is_empty(threshold_df):
            logger.info("Completed The threshold validations for -> " + self._module_name)
            return None
        else:
            logger.error("Threshold validation failed for -> " + self._module_name)
            s3_error_path = format_s3_path(self.bucket_name, self.error_path, self._module_name + '_threshold_report')
            threshold_df = threshold_df.withColumn("report_type",
                                                   when(col("report_month").isNull(), lit("yoy")).otherwise("mom"))

            (threshold_df.write
             .format('parquet')
             .partitionBy(claims().partition_columns)
             .mode('overwrite')
             .save(s3_error_path))

            key_values = [key[0] for key in threshold_df.select("sourcesystemid").distinct().collect()]
            return key_values
