from pyspark.sql.functions import broadcast, col
from pyspark.sql import Window

class ClaimsTransformations:
    def __init__(self, spark, logger, is_debug):
        self.spark = spark
        self.logger = logger
        self.is_debug = is_debug

    # ------------------- STAGE 1: CORE METHODS -------------------

    def core_create_member_df(self, member_df, customer_df):
        self.logger.info("Joining member with customer on memberid")
        return member_df.join(customer_df, "memberid", "left")

    def core_create_claims_df(self, claimline_df, claiminvoice_df, diagnosisclaim_df):
        self.logger.info("Joining claimline with claiminvoice and diagnosisclaim")
        df = claimline_df.join(claiminvoice_df, "claimid", "left")
        df = df.join(diagnosisclaim_df, "claimid", "left")
        return df

    def core_calculate_paid_amount(self, claim_df, claimpayment_df):
        self.logger.info("Joining claim_df with claimpayment on claimid")
        return claim_df.join(claimpayment_df, "claimid", "left")

    # ------------------- STAGE 2: ENRICHMENT METHODS -------------------

    def enrich_add_plan_codes(self, df):
        self.logger.info("Broadcast joining with dim_plan")
        plan_df = self.spark.table("refined.dim_plan")
        return df.join(broadcast(plan_df), "planid", "left")

    def enrich_add_gender_codes(self, df):
        self.logger.info("Broadcast joining with dim_gender")
        gender_df = self.spark.table("refined.dim_gender")
        return df.join(broadcast(gender_df), "gendercode", "left")

    def enrich_add_case_type(self, df):
        self.logger.info("Broadcast joining with dim_case_type")
        case_type_df = self.spark.table("refined.dim_case_type")
        return df.join(broadcast(case_type_df), "casetypecode", "left")

    def enrich_add_invoice_status_codes(self, df):
        self.logger.info("Broadcast joining with dim_invoice_status")
        status_df = self.spark.table("refined.dim_invoice_status")
        return df.join(broadcast(status_df), "statuscode", "left")

    # ------------------- FINAL COLUMNS -------------------

    def reorder_and_filter_final_columns(self, df, table_config):
        self.logger.info("Reordering and selecting final columns as per table_mapping.claims")
        return df.select([col(c) for c in table_config["columns"]])
