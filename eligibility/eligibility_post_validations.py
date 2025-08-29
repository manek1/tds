from common_data_sets.common.spark_common_functions import logger, is_empty, format_s3_path
from pyspark.sql.functions import when, col, lit
from .table_mapping import eligibility_output
from common_data_sets.common.abstract_data_quality_checks import AbstractDataQualityChecks
from common_data_sets.common.custom_exceptions import PostValidationError


class EligibilityPostValidations(AbstractDataQualityChecks):
    def __init__(self, eligibility_df, spark=None, **kwargs):
        super().__init__(df=eligibility_df, module_name='eligibility')
        self.spark = spark
        self.bucket_name = kwargs.get('bucket_name')
        self.error_path = "analytics-dataset/error"

    def validate(self, previous_run_df):
        if self.check_duplicate():
            logger.info("No duplicate data in -> " + self._module_name)
        else:
            logger.error("Exact duplicates in -> " + self._module_name)
            raise PostValidationError(self._module_name)

        if self.check_df_schema(schema=eligibility_output().schema):
            logger.info("Completed the schema validation for -> " + self._module_name)
        else:
            logger.error("DF schema is not matching with expected schema -> " + self._module_name)
            logger.error(f"Expected Schema -> {str(eligibility_output().schema)} '\n'"
                         f"Actual Schema -> {self.df._jdf.schema().treeString()}")
            raise PostValidationError(self._module_name)

        if previous_run_df.head(1):
            key_values = self.threshold_validation(previous_run_df)
        else:
            key_values = None

        return key_values

    def threshold_validation(self, previous_df):
        mom_report_df, yoy_report_df = self.create_df_summary(previous_df,
                                                              eligibility_output().key_columns,
                                                              eligibility_output().metrics,
                                                              date_column=eligibility_output().date_column)

        final_report_df = self.merge_yoy_mom_change(mom_report_df, yoy_report_df, ["sourcesystemid", 'report_year'],
                                                    'full')

        threshold_df = self.apply_threshold(self.spark, final_report_df, eligibility_output().metrics,
                                            eligibility_output().threshold)
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
             .partitionBy(eligibility_output().partition_columns)
             .mode('overwrite')
             .save(s3_error_path))

            key_values = [key[0] for key in threshold_df.select("sourcesystemid").distinct().collect()]
            return key_values
