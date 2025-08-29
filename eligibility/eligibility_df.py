"""
    Purpose : Process analytics ready dataset from raw ingested data for claims and eligibility
"""

from common_data_sets.common.abstract_glue_job import AbstractGlueJob
from .table_mapping import *
from common_data_sets.common.spark_common_functions import *
from common_data_sets.eligibility.source_queries.midb_queries import *
from .eligibility_pre_validations import EligibilityPreValidation
from common_data_sets.common.custom_exceptions import PreValidationError, InvalidTableSchemaError, ColumnNotFoundError
from .eligibility_post_validations import EligibilityPostValidations
from .eligibility_transformation import EligibilityTransformation
from common_data_sets.common.custom_exceptions import PostValidationError, ThresholdValidationError
from pyspark.sql.functions import current_timestamp
from datetime import date, datetime as dt


class Eligibility(AbstractGlueJob):
    def __init__(self, params):
        super(Eligibility, self).__init__(params)
        self.transform = EligibilityTransformation(self.spark, self.is_debug)

    @property
    def app_name(self) -> str:
        return "eligibility"

    def pre_validations(self):
        pre_val = EligibilityPreValidation()
        try:
            pre_val.validate()
        except (InvalidTableSchemaError, ColumnNotFoundError):
            raise PreValidationError(self.app_name)

    def post_validations(self, eligibility_current_df, eligibility_previous_df):
        key_values = EligibilityPostValidations(eligibility_current_df, self.spark,
                                                bucket_name=self.bucket_name).validate(eligibility_previous_df)
        if key_values:
            self.logger.error("Claims Post Validation Failed for source_system ->" + str(key_values))
            return key_values
        else:
            return None

    def create_eligibility_df(self):
        client_df = self.read_table(client, True, partition_predicate=self.part_predicate)
        client_sector_code_df = self.read_table(client_internal_industry_sector_code_info, True)
        ccra_df = self.read_table(clientclaimsriskarrangementcode_info, True)
        policy_df = self.read_table(policy, True, partition_predicate=self.part_predicate)
        policy_status_df = self.read_table(policystatus)
        client_contract_df = self.read_table(clientcontract)
        client_df = self.transform.create_ancestor_cols(client_df)
        client_df = self.transform.create_client_policy_data(client_df, policy_df, client_contract_df, ccra_df,
                                                             client_sector_code_df, policy_status_df)

        country_df = self.read_table(country, True, partition_predicate=self.part_predicate).dropDuplicates(
            ["countrycode", "country"])

        cor_df = self.transform.create_country_of_res_df(self.read_table(countryofresidence, True,
                                                                         partition_predicate=self.part_predicate),
                                                         country_df)

        if self.is_debug:
            self.logger.info("Created Country Of Res Df with Record Count" + str(cor_df.count()))

        membercoverage_df = self.read_table(membercoverage, True, partition_predicate=self.part_predicate)
        groupcoverage_df = self.read_table(groupcoverage, True, partition_predicate=self.part_predicate)
        legalentity_df = self.read_table(legalentity, True, partition_predicate=self.part_predicate)
        lineofbusiness_df = self.read_table(lineofbusiness, partition_predicate='')
        nationality_df = self.read_table(nationality, partition_predicate=self.part_predicate)
        package_df = self.read_table(package, partition_predicate=self.part_predicate)
        product_df = self.read_table(product, partition_predicate=self.part_predicate)

        nation_df = self.transform.create_nationality_df(nationality_df, country_df)

        legalentity_df = self.transform.add_legal_segment_market_data(legalentity_df)

        product_map_df = self.read_table(product_id_map, dynamic_frame=True)

        coverage_df = self.transform.create_coverage_df(membercoverage_df, groupcoverage_df, legalentity_df,
                                                        lineofbusiness_df, package_df, product_df, product_map_df)

        if self.is_debug:
            self.logger.info("Created coverage_df  with Record Count" + str(coverage_df.count()))

        member_refined_df = self.read_table(member, True, partition_predicate=self.part_predicate)
        customer_df = self.read_table(customer, True, partition_predicate=self.part_predicate)
        party_address_df = self.read_table(party_address_info, True, partition_predicate=self.part_predicate)
        party_address_df = self.transform.create_party_df(party_address_df)
        clientstaffcategory_df = self.read_table(clientstaffcategory, True, partition_predicate=self.part_predicate)
        family_unit_df = self.read_table(familyunit, partition_predicate=self.part_predicate)
        family_unit_typecode_df = self.read_table(familyunittypecode)

        insteller_df = self.read_table(vw_aws_c_InsTelling)

        member_df = self.transform.create_member_df(member_refined_df, customer_df, clientstaffcategory_df,
                                                    family_unit_df, party_address_df, country_df, insteller_df)

        if self.is_debug:
            self.logger.info("Created member_df with Record Count" + str(member_df.count()))

        eligibility_refined_df = self.read_table(eligibility, True, partition_predicate=self.part_predicate)
        leave_app_query, cancel_dt_app_query, det_app_query = leave_app_df_query, cancel_date_df_query, dets_df_query

        leave_app_df = self.read_sql(leave_app_query)
        logger.info(f"leave app df is created and count is {leave_app_df.count()}")
        cancel_dt_app_df = self.read_sql(cancel_dt_app_query)
        logger.info(f"cancel_dt_app df is created and count is {cancel_dt_app_df.count()}")
        det_app_df = self.read_sql(det_app_query)
        logger.info(f"det_app_query df is created and count is {det_app_df.count()}")
        # actisure path for elig_todate and elig_from_date
        eligibility_refined_df = self.transform.define_elig_to_date(eligibility_refined_df, leave_app_df,
                                                                    cancel_dt_app_df,
                                                                    det_app_df)

        member_id_list = parse_filter_id_list_param(self.filter_id_list)

        if self.etl_args.get('DATA_FILTERS', 7):
            rolling_year = dt.now().year - int(self.etl_args.get('DATA_FILTERS', '').split('=')[1])
            eligibilityfromdate = date(rolling_year, 12, 31).strftime('%Y-%m-%d')
            logger.info("Filtering out eligibility  data from " + str(eligibilityfromdate))
            eligibilitytodate = date(rolling_year, 12, 31).strftime('%Y-%m-%d')
            logger.info("Filtering out eligibility data till " + str(eligibilitytodate))
        else:
            eligibilityfromdate = '1000-01-01'
            eligibilitytodate = '9999-01-01'

        eligibility_df = self.transform.prepare_eligibility_df(eligibility_refined_df, coverage_df, member_df,
                                                               family_unit_typecode_df, member_id_list,
                                                               eligibility_from_date=eligibilityfromdate,
                                                               eligibility_to_date=eligibilitytodate,
                                                               dateformat='yyyy-MM-dd')
        if self.is_debug:
            self.logger.info("Created eligibility_df with Record Count" + str(eligibility_df.count()))
        eligibility_cor_df = self.transform.create_eligibility_final_df(eligibility_df, client_df, cor_df)
        plan_df = self.read_table(elg_plan, partition_predicate=self.part_predicate)
        eligibility_plan_df = self.transform.add_line_coverage(eligibility_cor_df, plan_df)

        sourcesystem_df = self.read_table(source_system_info)
        eligibility_src_name_df = self.transform.add_source_name(sourcesystem_df, eligibility_plan_df)

        eligibility_final_df = (self.transform.add_nationality(nation_df, eligibility_src_name_df)
                                .withColumn("sysbatchdttm", current_timestamp())
                                )

        edw_pt_df = self.read_table(product_tree)
        edw_elig_df = self.read_table(edw_elig)
        eligibility_final_df = self.transform.add_edw_pt_cols(eligibility_final_df, edw_pt_df, edw_elig_df)

        '''adding respective descriptive columns to code form columns'''
        areaofcover_df = self.read_table(area_of_cover_info, True).dropDuplicates(["areaofcover", "areaofcovercode"])
        cbsc_df = self.read_table(client_business_segment_code_info, True).dropDuplicates(
            ["clientbusinesssegment", "clientbusinesssegmentcode"])
        cpfa_df = self.read_table(client_premium_funding_arrangement_code_info, True).dropDuplicates(
            ["premiumfundingarrangement", "premiumfundingarrangementcode"])
        pricing_method_df = self.read_table(client_pricing_method_code_info, True).dropDuplicates(
            ["pricingmethod", "pricingmethodcode"])
        loc_df = self.read_table(lineofcovercode, True).dropDuplicates(["lineofcovercode", "lineofcover"])
        eligibility_final_df = self.transform.add_descriptive_value_cols(eligibility_final_df, areaofcover_df, cbsc_df,
                                                                         cpfa_df, pricing_method_df, loc_df)

        date_cols = ['policystartdate', 'policyenddate', 'clientcontractstartdate', 'clientcontractenddate',
                     'clientinceptiondate', 'clientterminationdate', 'dateofbirth', 'countryofresidencestartdate',
                     'countryofresidenceenddate', 'eligibilityfromdate', 'eligibilitytodate', 'record_from_date',
                     'record_to_date', 'leavedate', 'memberterminationdate', 'policydetend',
                     'eligibilityterminationdate', 'memberadditiondate']
        eligibility_final_df = self.transform.convert_datetime_to_date_format(eligibility_final_df, date_cols)
        eligibility_final_df = self.transform.define_custom_cols(eligibility_final_df)
        return eligibility_final_df

    def _execute(self):
        self.logger.info(f"Execution Started for {self.app_name}")
        eligibility_df = self.create_eligibility_df()
        eligblity_op = eligibility_output(partition_columns=self.etl_args.get('PARTITION_COLUMNS', ''))
        s3_output_path = format_s3_path(self.bucket_name, self.output_path, self.app_name, 'parquet')

        s3_archive_prefix = os.path.join(self.output_path.split('/')[0], 'archive/parquet', self.app_name + '_archive')
        s3_source_prefix = os.path.join(self.output_path, 'parquet', self.app_name)

        s3_archive_input_path = os.path.join("s3a://" + self.bucket_name, s3_archive_prefix)

        # Delete older Archival
        delete_s3_object(self.bucket_name, s3_archive_prefix)

        # Old data Archival
        copy_all_s3_object(self.bucket_name, s3_source_prefix + '/', s3_archive_prefix)

        # Final eligibility df
        eligibility_current_df = reformat_df(eligibility_df, eligblity_op.schema)

        # previous df for threshold report generation
        '''
        eligibility_previous_df = self.read_s3_file(s3_archive_input_path, schema=eligblity_op.schema)
        eligibility_previous_df = reformat_df(eligibility_previous_df, eligblity_op.schema)
        key_value = self.post_validations(eligibility_current_df, eligibility_previous_df)'''

        key_value = None

        try:
            # Claims post load validations
            if key_value:
                eligibility_error_df = eligibility_current_df.where(col("sourcesystemid").isin(key_value))
                s3_error_path = format_s3_path(self.bucket_name, "analytics-dataset/error",
                                               query_type=(self.app_name + '_error'))
                self.write_df_to_s3(eligibility_error_df, s3_error_path, partition=eligblity_op.partition_columns)

                eligibility_non_error_df = eligibility_current_df.where(~col("sourcesystemid").isin(key_value))
                self.write_df_to_s3(eligibility_non_error_df, s3_output_path,
                                    partition=eligblity_op.partition_columns)
                raise PostValidationError(self.app_name)

            else:
                self.write_df_to_s3(eligibility_current_df, s3_output_path,
                                    partition=eligblity_op.partition_columns, redshift_partition_col='sourcesystemid')

                src_tgt_mapping = {'sourcesystemid=1': 'eligibility_sourcesystem_1',
                                   'sourcesystemid=2': 'eligibility_sourcesystem_2',
                                   'sourcesystemid=3': 'eligibility_sourcesystem_3',
                                   'sourcesystemid=4': 'eligibility_sourcesystem_4',
                                   'sourcesystemid=5': 'eligibility_sourcesystem_5',
                                   'sourcesystemid=6': 'eligibility_sourcesystem_6',
                                   'sourcesystemid=7': 'eligibility_sourcesystem_7',
                                   'sourcesystemid=8': 'eligibility_sourcesystem_8'}

                for src, tgt in src_tgt_mapping.items():
                    self.logger.info(f'Adding {src} to {tgt}')
                    src_prefix = os.path.join(self.output_path, 'parquet', self.app_name, src)
                    tgt_prefix = os.path.join(self.output_path, 'parquet','eligibility_temp', tgt)
                    delete_s3_object(self.bucket_name, tgt_prefix)
                    copy_all_s3_object(self.bucket_name, src_prefix + '/', tgt_prefix)

                if self.is_debug:
                    s3_csv_path = format_s3_path(self.bucket_name, self.output_path, self.app_name, output_format='csv')
                    self.write_df_to_s3(eligibility_current_df, s3_csv_path, out_format='csv',
                                        partition=eligblity_op.partition_columns)

                self.logger.info("Job executed successfully")

        except PostValidationError:
            self.logger.error("eligibility Post Validation Failed for source system id ->" + str(key_value))
            self.logger.error("Using Archive data for source system id ->" + str(key_value))
            self.logger.error("Failing the Glue job ->" + str(key_value))
            raise ThresholdValidationError(self.app_name, "sourcesystemid", key_value)


def main(*params, **kwargs) -> None:
    param = ['CATALOG_DB_NAME', 'OUTPUT_BUCKET', 'OUTPUT_BUCKET_PREFIX', 'PARTITION_COLUMNS', 'FILTER_ID_LIST', 'DEBUG',
             'PARTITION_PREDICATE', 'DATA_FILTERS', 'CONF_BUCKET', 'CONF_FILE_PREFIX']
    Eligibility(param).execute()
