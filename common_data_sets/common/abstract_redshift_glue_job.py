import sys
from abc import ABC, abstractmethod
from .abstract_pyspark_job import AbstractPySparkJob
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


class AbstractRedshiftGlueJob(AbstractPySparkJob, ABC):
    def __init__(self, params=None):
        super().__init__()
        self.args = sys.argv
        self.params = params
        self.etl_args = self.get_glue_args()
        self.output_bucket = self.etl_args.get("output_bucket", "")
        self.table_s3_paths = self.etl_args.get("source_s3_data_path", "")
        self.source_type = self.etl_args.get("source_type", "")
        self.database_user_name = self.etl_args.get("database_user", "")
        self.redshift_cluster_name = self.etl_args.get("redshift_cluster_name", "")
        self.glue_catalog_db_name = self.etl_args.get("glue_catalog_db_name", "")
        self.dest_table_schema = self.etl_args.get("dest_table_schema", "")
        self.table_type = self.etl_args.get("table_type", "")
        self.table_dist_col = self.etl_args.get("table_dist_col", "")
        self.table_sort_col = self.etl_args.get("table_sort_col", "")
        self.redshift_role = self.etl_args.get("iam_role", "")
        self.redshift_db_name = self.etl_args.get("redshift_db_name", "")
        self.source_file_partition_cols = self.etl_args.get("source_file_partition_cols", "")

    @property
    def job_type(self) -> str:
        return "aws-redshift-data-transfer"

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

    def execute(self):
        self.logger.info("Running the module " + self.app_name)
        self._execute()
        self.logger.info("Execution completed for module " + self.app_name)

    @property
    def is_debug(self):
        return False
