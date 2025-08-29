import sys
from abc import ABC, abstractmethod
from .abstract_pyspark_job import AbstractPySparkJob
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


class AbstractPCMGlueJob(AbstractPySparkJob, ABC):
    def __init__(self, params=None):
        super().__init__()
        self.args = sys.argv
        self.params = params
        self.etl_args = self.get_glue_args()
        self.bucket_name = self.etl_args.get("OUTPUT_BUCKET", "")
        self.output_path = self.etl_args.get("OUTPUT_BUCKET_PREFIX", "")
        self.process_type = self.etl_args.get("PROCESS_TYPE", "")
        self.pso_lambda_fun_name = self.etl_args.get("pso_lambda_fun_name", "")
        self.unmatched_claims_data_s3_path = self.etl_args.get("unmatched_claims_data_s3_path", None)

    @property
    def job_type(self) -> str:
        return "aws-pcm-job"

    def get_glue_args(self) -> dict:  # pragma: no cover
        if self.args and self.params:
            return getResolvedOptions(self.args, self.params)
        else:
            return dict()

    def init_spark_job(self):
        spark = self.glue_context.sparkSession
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
