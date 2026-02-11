from common_data_sets.common.abstract_glue_job import AbstractGlueJob
from .table_mapping import *
from .claims_transformations import ClaimsTransformations
from common_data_sets.common.custom_exceptions import PostValidationError, ThresholdValidationError
from pyspark.sql.functions import current_timestamp

class Claims(AbstractGlueJob):
    def __init__(self, params):
        super(Claims, self).__init__(params)
        self.transform = ClaimsTransformations(self.spark, self.logger, self.is_debug)

    @property
    def app_name(self) -> str:
        return "claims"

    def prepare_core_claims_data(self):
        claimline_df = self.read_table(claimline, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        claiminvoice_df = self.read_table(claiminvoice, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        diagnosisclaim_df = self.read_table(diagnosisclaim, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        member_df = self.read_table(member, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        customer_df = self.read_table(customer, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        claimpayment_df = self.read_table(claimpayment, True, dynamic_frame=True, partition_predicate=self.part_predicate)

        member_df = self.transform.core_create_member_df(member_df, customer_df)
        claim_df = self.transform.core_create_claims_df(claimline_df, claiminvoice_df, diagnosisclaim_df)
        claim_df = self.transform.core_calculate_paid_amount(claim_df, claimpayment_df)

        self.write_table(df=claim_df, table_config=claims_stage1_temp, mode="overwrite")

    def enrich_claims_with_lookups(self):
        df = self.read_table(claims_stage1_temp, dynamic_frame=False, partition_predicate=self.part_predicate)

        df = self.transform.enrich_add_plan_codes(df)
        df = self.transform.enrich_add_gender_codes(df)
        df = self.transform.enrich_add_case_type(df)
        df = self.transform.reorder_and_filter_final_columns(df, claims)
        df = df.withColumn("recordcreatedtimestamp", current_timestamp())

        self.write_table(df=df, table_config=claims, mode="overwrite")

    def run(self):
        try:
            self.logger.info("Running Stage 1: Core Claims Processing")
            self.prepare_core_claims_data()
            self.logger.info("Running Stage 2: Enrichment and Final Output")
            self.enrich_claims_with_lookups()
        except (PostValidationError, ThresholdValidationError) as e:
            self.logger.error(f"Validation Error: {str(e)}")
            raise
        except Exception as ex:
            self.logger.error(f"Unhandled Exception: {str(ex)}")
            raise
