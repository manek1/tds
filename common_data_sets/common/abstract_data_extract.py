import io
import sys
import time
from abc import ABC

import boto3

from .abstract_glue_job import AbstractGlueJob


class AbstractDataExtract(AbstractGlueJob, ABC):
    def __init__(self, params):
        super().__init__(params)
        self.args = sys.argv
        self.params = params
        self.etl_args = self.get_glue_args()
        self.extract_type = self.etl_args.get("EXTRACT_TYPE", "spark")
        self.region_name = self.etl_args.get("REGION_NAME", "eu-west-2")
        self.athena_client = boto3.client('athena', region_name=self.region_name)
        self.db_name = self.etl_args.get("SOURCE_DATABASE", "intl-euro-trusted-data-sets")
        self.query_file = self.etl_args.get("QUERY_PATH", "spark")
        self.extract_name = self.etl_args.get("APP_NAME", "provider_claims_tableau")
        # self.write_option = self.etl_args.get("write_option", "spark")
        self.output_format = self.etl_args.get("OUTPUT_FORMAT", "parquet")
        # self.output_s3_location = f"s3a://{self.bucket_name}/{self.output_path}/{self.extract_app_name}"

    def read_query(self, unload: bool = True, op_format: str = 'PARQUET'):
        s3_client = boto3.client("s3")
        self.logger.info(f"Reading the file {self.query_file} from bucket {self.code_bucket}")

        try:
            response = s3_client.get_object(Bucket='intl-euro-uk-data-dev', Key=self.query_file)
        except Exception as e:
            self.logger.error(f"Failed to fetch the file {self.query_file}: {str(e)}")
            return False

        if "ContentLength" in response and response["ContentLength"] > 0:
            file_content = response['Body'].read().decode()
            query = file_content
            if unload:
                query = f"""UNLOAD ({file_content})
                TO '{self.output_s3_location}' WITH (format = {op_format});"""
            return query
        else:
            self.logger.warning(f"File {self.query_file} is empty or has no ContentLength.")
            return False

    # Function to Run Athena Query
    def run_athena_query(self, query, database):
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            }
        )
        return response['QueryExecutionId']

    # Function to Wait for Query Completion
    def wait_for_query_to_complete(self, query_execution_id):
        while True:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            if state == 'SUCCEEDED':
                self.logger.info("Query succeeded")
                return response
            elif state == 'FAILED':
                raise Exception("Query failed: " + response['QueryExecution']['Status']['StateChangeReason'])
            elif state == 'CANCELLED':
                raise Exception("Query was cancelled")
            time.sleep(5)  # Poll every 5 seconds

    def execute_query(self, query, database):
        try:
            self.logger.info("Running Athena UNLOAD query...")
            query_execution_id = self.run_athena_query(query, database)
            self.logger.info(f"Query Execution ID: {query_execution_id}")

            self.logger.info("Waiting for query to complete...")
            query_execution_result = self.wait_for_query_to_complete(query_execution_id)

            self.logger.info(str(query_execution_result))

            self.logger.info(f"Query results exported to S3 in Parquet format at: {self.output_s3_location}")

        except Exception as e:
            self.logger.info(f"Error: {e}")
