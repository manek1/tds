"""
    Purpose : Process analytics ready dataset from raw ingested data for nbb triangle report
"""
import os

from common_data_sets.common.abstract_glue_job import AbstractGlueJob
from .table_mapping import *
from .nbb_report_transfomation import *
from pyspark.sql.functions import current_timestamp, to_date, col, upper
from common_data_sets.common.spark_common_functions import reformat_df, format_s3_path
from .nbb_triangle_report_dq import run_triangle_report_dq


class NbbReport(AbstractGlueJob):
    def __init__(self, params):
        super(NbbReport, self).__init__(params)
        self.transform = NbbTriangleReport(self.spark, self.logger, self.is_debug)

    @property
    def app_name(self) -> str:
        return "nbb_report"

    def pre_validations(self):
        pass

    def create_traingle_report_df(self):
        claims_nbb_df = (self.read_table(nbb_claims, True)
                         .where(upper(col('claimsriskarrangement')) == 'FULLY INSURED')
                         )
        exchangerate_df = self.read_table(exchangerate, True)

        final_i2r_df = self.transform.build_incurred_to_received(claims_nbb_df, exchangerate_df)
        final_i2p_df = self.transform.build_incurred_to_paid(claims_nbb_df, exchangerate_df)
        final_r2p_df = self.transform.build_received_to_paid(claims_nbb_df, exchangerate_df)

        triangle_report_df = self.transform.build_final_nbb_extract(final_i2r_df, final_i2p_df, final_r2p_df)

        return triangle_report_df

    def _execute(self):
        self.logger.info(f"Execution Started for {self.app_name}")
        triangle_report_df = self.create_traingle_report_df()
        triangle_report = nbb_claims_triangle_report(partition_columns=self.etl_args.get('PARTITION_COLUMNS', ''))
        tirangle_report_path = os.path.join(self.app_name, 'nbb_claims_triangle_report')
        s3_output_path = format_s3_path(self.bucket_name, self.output_path, tirangle_report_path, output_format='parquet')

        triangle_report_df = reformat_df(triangle_report_df, triangle_report.schema)

        results = run_triangle_report_dq(triangle_report_df, strict=False)

        self.logger.info(f"DQ results Completed please refer {results}")

        self.write_df_to_s3(triangle_report_df, s3_output_path,
                            partition=triangle_report.partition_columns,
                            redshift_partition_col='triangle_report_type')

        triangle_report_df.show(5)

        if self.is_debug:
            s3_csv_path = format_s3_path(self.bucket_name, self.output_path, self.app_name, output_format='csv')
            self.write_df_to_s3(triangle_report_df, s3_csv_path, out_format='csv',
                                partition=triangle_report.partition_columns,
                                redshift_partition_col='triangle_report_type')

        self.logger.info("Job executed successfully")

def main(*params, **kwargs) -> None:
    param = ['CATALOG_DB_NAME', 'OUTPUT_BUCKET', 'OUTPUT_BUCKET_PREFIX', 'PARTITION_COLUMNS', 'DEBUG',
             'PARTITION_PREDICATE']

    NbbReport(param).execute()
