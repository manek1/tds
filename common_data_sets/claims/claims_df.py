from common_data_sets.common.abstract_glue_job import AbstractGlueJob
from .table_mapping import *
from .claims_transformations import ClaimsTransformations
from .claims_pre_validations import ClaimsPreValidations
from .claims_post_validations import ClaimsPostValidations
from common_data_sets.common.custom_exceptions import PostValidationError, ThresholdValidationError
from common_data_sets.common.spark_common_functions import *
from pyspark.sql.functions import current_timestamp
from datetime import date, datetime as dt
from common_data_sets.common.configs import config

DATE_FORMAT_WITH_TS = '%Y-%m-%d %H:%M:%S'


class Claims(AbstractGlueJob):
    def __init__(self, params):
        super(Claims, self).__init__(params)
        self.transform = ClaimsTransformations(self.spark, self.logger, self.is_debug)
        """
        self.spark.conf.set("spark.sql.shuffle.partitions", "50")
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "10")
        self.spark.conf.set("spark.sql.parquet.output.committer.class",
               "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
        """


    @property
    def app_name(self) -> str:
        return "claims"

    def prepare_claims_data(self):

        cor_raw_df = self.read_table(countryofresidence, True, dynamic_frame=True,
                                     partition_predicate=self.part_predicate)
        country_df = self.read_table(country, dynamic_frame=True, partition_predicate=self.part_predicate)
        cor_df = self.transform.create_country_of_res_df(cor_raw_df, country_df)

        member_raw_df = self.read_table(member, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        customer_raw_df = self.read_table(customer, True, dynamic_frame=True, partition_predicate=self.part_predicate)

        family_unit_df = self.read_table(family_unit, force=True, dynamic_frame=True,
                                         partition_predicate=self.part_predicate)
        nationality_df = self.read_table(nationality, force=True, dynamic_frame=True,
                                         partition_predicate=self.part_predicate)

        ins_telling_df = self.read_table(vw_aws_c_instelling, dynamic_frame=True)

        member_ins_df = self.transform.create_member_ins_telling_df(member_raw_df, ins_telling_df)

        family_unit_type_df = self.read_table(family_unit_type, dynamic_frame=True)
        family_type_df = self.transform.create_family_type_df(family_unit_df, family_unit_type_df)
        clientstaffcategory_df = self.read_table(clientstaffcategory, True, dynamic_frame=True,
                                                 partition_predicate=self.part_predicate)

        member_df = self.transform.create_member_df(cor_df, member_ins_df, clientstaffcategory_df, customer_raw_df,
                                                    family_type_df, nationality_df, country_df)

        rdr_exchangerate_df = self.read_table(rdr_exchangerate, dynamic_frame=True)
        rdr_exchangerate_df = self.transform.prepare_rdr_exchange_rate(rdr_exchangerate_df)
        io_ex_df = self.read_table(vw_aws_c_koers, True, dynamic_frame=True)
        io_exchange_df = self.transform.prepare_io_exchange_rate(io_ex_df)

        claimpayment_df = self.read_table(claimpayment, dynamic_frame=True, partition_predicate=self.part_predicate)

        party_df = self.read_table(party, dynamic_frame=True, partition_predicate=self.part_predicate)
        partyroletype_df = self.read_table(partyroletype, dynamic_frame=True)
        payment_method_code_df = self.read_table(paymentmethodcode, True, dynamic_frame=True)
        currency_df = self.read_table(currencycode, dynamic_frame=True)
        clm_sts_lvl_1_df = self.read_table(claim_payment_status_level1code, dynamic_frame=True)
        clm_sts_lvl_2_df = self.read_table(claim_payment_status_level2code, dynamic_frame=True)
        clm_sts_lvl_3_df = self.read_table(claim_payment_status_level3code, dynamic_frame=True)
        banking_df = self.read_table(banking, True, dynamic_frame=True,
                                     partition_predicate=self.part_predicate).distinct()
        clm_settle_df = self.read_table(claimsettlement, dynamic_frame=True)

        pay_df = self.transform.create_payment_df(claimpayment_df, party_df, partyroletype_df, payment_method_code_df,
                                                  clm_sts_lvl_1_df, clm_sts_lvl_2_df, clm_sts_lvl_3_df, currency_df,
                                                  country_df, banking_df, clm_settle_df)

        claiminv_raw_df = self.read_table(claiminvoice, True, dynamic_frame=True,
                                          partition_predicate=self.part_predicate)
        partner_df = self.read_table(partner, True, dynamic_frame=True, partition_predicate=self.part_predicate)

        invoice_list = parse_filter_id_list_param(self.filter_id_list)

        clm_inv_ext = self.read_table(claim_invoice_status_level1code, True, dynamic_frame=True)

        clm_ext_inv_df = self.read_table(claimexternalinvoice, True, dynamic_frame=True,
                                         partition_predicate=self.part_predicate)

        clm_inv_nw_df = self.read_table(claiminvoicenetworkusagecode, dynamic_frame=True)

        ciesc_df = self.read_table(claim_invoice_status_level2code, dynamic_frame=True)
        ciicsc_df = self.read_table(claim_invoice_status_level3code, dynamic_frame=True)

        claiminv_df = self.transform.create_claiminvoice_df(claiminv_raw_df, partner_df, clm_inv_ext, clm_ext_inv_df,
                                                            clm_inv_nw_df, country_df, currency_df, partyroletype_df,
                                                            ciesc_df, ciicsc_df, invoice_list)

        if self.etl_args.get('DATA_FILTERS', 7):
            rolling_year = dt.now().year - int(self.etl_args.get('DATA_FILTERS', '').split('=')[1])
            CLAIM_START_DATE = date(rolling_year, 12, 31).strftime('%Y-%m-%d')
            logger.info("Filtering out claims data before " + str(CLAIM_START_DATE))
        else:
            CLAIM_START_DATE = '1000-01-01'

        claimdet_df = self.read_table(claimline, True, dynamic_frame=True, partition_predicate=self.part_predicate)

        reason_df = self.read_table(reason, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        denied_reason_code_df = self.read_table(deniedreasoncode, True, dynamic_frame=True,
                                                partition_predicate=self.part_predicate)
        amountcopayment_df = self.read_table(amountcopayment, dynamic_frame=True,
                                             partition_predicate=self.part_predicate)
        reason_nc_us_df = self.read_table(reasonnotcovereduscode, dynamic_frame=True,
                                          partition_predicate=self.part_predicate)

        reimbursement_limit_df = self.read_table(reasonreimbursementlimitreductionreasoncode, dynamic_frame=True,
                                          partition_predicate=self.part_predicate)

        customer_reduction_df = self.read_table(reasonableandcustomaryreductionreasoncode, dynamic_frame=True,
                                                partition_predicate=self.part_predicate)

        admission_df = self.read_table(claimlineadmissiontypecode, dynamic_frame=True)

        modifier_code_df = self.read_table(currentproceduralterminologymodifiercode, dynamic_frame=True)

        reason_df = self.transform.prepare_reason_data(reason_df, denied_reason_code_df, reason_nc_us_df,
                                                       reimbursement_limit_df,customer_reduction_df)
        claimdet_df = self.transform.add_reason_code(claimdet_df, reason_df, amountcopayment_df,
                                                     admission_df,modifier_code_df)

        product_df = self.read_table(product, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        plan_df = self.read_table(plan, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        package_df = self.read_table(package, True, dynamic_frame=True, partition_predicate=self.part_predicate)

        dt_df = self.read_table(detail_transaction, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        pdt_tree_df = self.read_table(product_tree, True, dynamic_frame=True)
        dt_tr_df = self.transform.create_bob_df(dt_df, pdt_tree_df)
        procedure_df = self.read_table(procedure, True, dynamic_frame=True, partition_predicate=self.part_predicate)
        terminology_df = self.read_table(currentproceduralterminologycode, dynamic_frame=True)

        procedure_df = self.transform.prepare_procedure_data(procedure_df, terminology_df)

        claimline_service_df = self.read_table(claimline_service_code, True, dynamic_frame=True,
                                               partition_predicate=self.part_predicate)

        claimline_external_stc_df = self.read_table(claim_line_status_level1code, True, dynamic_frame=True)

        clm_int_clm_sc_df = self.read_table(claim_line_status_level3code, True, dynamic_frame=True)

        claimline_external_sc_df = self.read_table(claim_line_status_level2code, True, dynamic_frame=True)

        loc_df = self.read_table(lineofcovercode, True, dynamic_frame=True)
        product_map_df = self.read_table(product_id_map, dynamic_frame=True)

        pkg_plan_product_df = self.transform.map_product_package_plan(product_df, package_df, plan_df, loc_df,
                                                                      product_map_df)

        claimdet_product_df = self.transform.create_product_df(pkg_plan_product_df, claimdet_df,
                                                               dt_tr_df, procedure_df, claimline_service_df,
                                                               claimline_external_sc_df, claimline_external_stc_df,
                                                               clm_int_clm_sc_df) # , admission_df,modifier_code_df)

        placeofservicecode_df = self.read_table(placeofservice, True, dynamic_frame=True)

        claimsubmission_df = self.read_table(claimsubmission, dynamic_frame=True,
                                             partition_predicate=self.part_predicate)

        benefit_df = self.read_table(benefit, True, dynamic_frame=True, partition_predicate=self.part_predicate)

        clm_sub_channel_df = self.read_table(claimsubmissionclaimchannelcode, dynamic_frame=True)

        claims_view_df = self.read_table_list(claims_view, dynamic_frame=True)

        case_type_df = self.read_table(case_type, True, dynamic_frame=True)

        claims_view_df = self.transform.add_case_data(claims_view_df, case_type_df)

        legalentityDf = self.read_table(legalentity, dynamic_frame=True, partition_predicate=self.part_predicate)

        lineofbusiness_df = self.read_table(lineofbusiness, dynamic_frame=True)

        claim_line_legal_df = self.transform.add_legalentity_data(legalentityDf, lineofbusiness_df,
                                                                  claimdet_product_df)

        claim_result_df = self.transform.create_claim_final_df(member_df, pay_df, claiminv_df, claim_line_legal_df,
                                                               benefit_df, placeofservicecode_df, rdr_exchangerate_df,
                                                               io_exchange_df, claimsubmission_df, clm_sub_channel_df,
                                                               claims_view_df, claim_start_date=CLAIM_START_DATE)

        return claim_result_df

    def claims_query_df(self):

        claim_result_det_df = self.prepare_claims_data()

        invoice_recovery_df = self.read_table(invoicerecovery, True, dynamic_frame=True,
                                              partition_predicate=self.part_predicate)
        claim_recovery_df = self.read_table(claimrecovery, True, dynamic_frame=True,
                                            partition_predicate=self.part_predicate)

        claim_result_rec_df = self.transform.add_recovered_amount(invoice_recovery_df, claim_recovery_df,
                                                                  claim_result_det_df)

        provider_df = self.read_table(provider, dynamic_frame=True, partition_predicate=self.part_predicate)
        provider_type_df = self.read_table(providertypecode, dynamic_frame=True)
        provider_group_df = self.read_table(providergroupcode, dynamic_frame=True)

        claim_result_prvdr_df = self.transform.add_provider_data(provider_df, claim_result_rec_df, provider_type_df,
                                                                 provider_group_df)

        amountdenied_df = self.read_table(amountdenied, dynamic_frame=True, partition_predicate=self.part_predicate)
        claim_result_amntd_df = self.transform.add_amountdenied_data(amountdenied_df, claim_result_prvdr_df)

        amounttax_df = self.read_table(amounttax, dynamic_frame=True, partition_predicate=self.part_predicate)
        claim_result_amttax_df = self.transform.add_amounttax_data(amounttax_df, claim_result_amntd_df)

        amountdiscount_df = self.read_table(amountdiscount, dynamic_frame=True, partition_predicate=self.part_predicate)
        claim_result_amount_final_df = self.transform.add_amountdiscount_data(amountdiscount_df, claim_result_amttax_df)

        sourcesystem_df = self.read_table(source_system_info, dynamic_frame=True)
        claim_result_sn_df = self.transform.add_source_name(sourcesystem_df, claim_result_amount_final_df)

        diagnosis_claim_df = self.read_table(diagnosis_claim_info, True, dynamic_frame=True)

        cci_conf_path = format_s3_path(self.code_bucket, self.code_path, 'cci2015.csv')
        ccs_conf_path = format_s3_path(self.code_bucket, self.code_path, 'ccs_multi_dx_tool_2015.csv')
        cci_df = create_dataframe_from_file(self.spark, cci_conf_path, properties={'header': True})
        ccs_df = create_dataframe_from_file(self.spark, ccs_conf_path, properties={'header': True})

        dx_ccr_conf_path = format_s3_path(self.code_bucket, self.code_path, 'DXCCSR_v2024-1.csv')
        ccir_conf_path = format_s3_path(self.code_bucket, self.code_path, 'CCIR-v2024-1.csv')
        cc_conf_path = format_s3_path(self.code_bucket, self.code_path, 'Code_chapter_2023.csv')

        dx_ccr_df = create_dataframe_from_file(self.spark, dx_ccr_conf_path, properties={'header': True})
        dx_ccir_df = create_dataframe_from_file(self.spark, ccir_conf_path, properties={'header': True})
        cc_df = create_dataframe_from_file(self.spark, cc_conf_path, properties={'header': True})

        icd_10_desc_df, icd_10_merge_df = self.transform.prepare_icd10_data(dx_ccr_df, dx_ccir_df)
        icd9_df = self.read_table(icd9diagnosis, dynamic_frame=True)
        icd10_df = self.read_table(icd10diagnosis, dynamic_frame=True)

        ds_path = config.DS_PATH.format(self.code_bucket.split('-')[-1])
        ds_bucket = ds_path.split('/')[2]
        ds_prefix = f"{ds_path.split(ds_bucket)[1].removeprefix('/')}/"
        tds_prefix = os.path.join(self.output_path, 'parquet', ds_prefix.split('/')[-2], config.DS_PATH.split('/')[-1])

        self.logger.info(f"writing DCX CCR data to {ds_path}")

        self.write_df_to_s3(icd_10_merge_df, ds_path, out_format='csv', num_of_files=1, header=True)

        self.logger.info(f"writing DCX CCR data to TDS {tds_prefix}")

        delete_s3_object(self.bucket_name, tds_prefix)

        move_s3_files(ds_bucket, ds_prefix, self.bucket_name, tds_prefix, delete_flag=False)

        cci_ccs_df = self.transform.prepare_lookup_data(cci_df, ccs_df)

        diag_code_df = self.read_table(internaldiagnosis, dynamic_frame=True, partition_predicate=self.part_predicate)

        claim_result_diagnosis_df = self.transform.add_diagnosis_data(diagnosis_claim_df, claim_result_sn_df,
                                                                      cci_ccs_df, icd_10_desc_df, cc_df, diag_code_df,
                                                                      icd9_df, icd10_df)

        client_df = self.read_table(client, dynamic_frame=True, partition_predicate=self.part_predicate)
        client_sector_code_df = self.read_table(clientinternalindustrysectorcode, force=True, dynamic_frame=True)
        client_df = self.transform.create_ancestor_cols(client_df, client_sector_code_df)

        policy_df = self.read_table(policy, dynamic_frame=True, partition_predicate=self.part_predicate)
        client_risk_df = self.read_table(client_risk_arrange, dynamic_frame=True)
        client_bs_code_df = self.read_table(clientbusinesssegmentcode, dynamic_frame=True)

        claim_result_final_df = (
            self.transform
            .add_client_data(client_df, client_risk_df, policy_df, claim_result_diagnosis_df, client_bs_code_df)
            .withColumn("sysbatchdttm", current_timestamp())
            .withColumnRenamed("externalclaimstatustype", "claimlinestatuslevel1")
        )

        hsrc_df = self.read_table(hospitalrevenuecode, dynamic_frame=True)
        pcdtc_df = self.read_table(proceduretypecode, dynamic_frame=True)
        clctc_df = self.read_table(claimlineclaimtypecode, dynamic_frame=True)

        claim_result_final_df = (self.transform
                                 .add_code_description(claim_result_final_df, hsrc_df, pcdtc_df, clctc_df)
                                 )

        if self.is_debug:
            self.logger.info(f'created the claims data, with record count, {claim_result_final_df.count()}')

        return claim_result_final_df

    def pre_validations(self):
        ClaimsPreValidations().validate()

    def post_validations(self, claims_df, archive_df):
        key_values = ClaimsPostValidations(claims_df, self.spark, bucket_name=self.bucket_name).validate(archive_df)
        if key_values:
            self.logger.error("Claims Post Validation Failed for source_system ->" + str(key_values))
            return key_values
        else:
            return None

    def _execute(self, *args, **params):
        claims_df = self.claims_query_df()
        claim = claims(partition_columns=self.etl_args.get('PARTITION_COLUMNS', ''))
        s3_output_path = format_s3_path(self.bucket_name, self.output_path, self.app_name, output_format='parquet')
        s3_archive_prefix = os.path.join(self.output_path.split('/')[0], 'archive', 'parquet', self.app_name +
                                         '_archive')
        s3_archive_path = os.path.join("s3a://" + self.bucket_name, s3_archive_prefix)

        self.logger.info("Archive path -> " + s3_archive_path)
        s3_source_prefix = os.path.join(self.output_path, 'parquet', self.app_name)

        # Delete older Archival
        delete_s3_object(self.bucket_name, s3_archive_prefix)

        # Old data Archival
        copy_all_s3_object(self.bucket_name, s3_source_prefix + '/', s3_archive_prefix)

        archive_df = create_empty_df(self.spark)  # self.read_s3_file(s3_archive_path)

        claims_df = reformat_df(claims_df, claim.schema)

        # Start the post data validations
        if is_empty(archive_df):
            key_value = None
            logger.info("No back up data is available skipping threshold report")
        else:
            key_value = self.post_validations(claims_df, archive_df)
        try:
            # Claims post load validations
            if key_value:
                claims_error_df = claims_df.where(col("sourcesystemid").isin(key_value))
                s3_error_path = format_s3_path(self.bucket_name, "analytics-dataset/error",
                                               query_type=(self.app_name + '_error'))
                self.write_df_to_s3(claims_error_df, s3_error_path, partition=claim.partition_columns)

                claims_non_error_df = claims_df.where(~col("sourcesystemid").isin(key_value))
                self.write_df_to_s3(claims_non_error_df, s3_output_path, partition=claim.partition_columns)
                raise PostValidationError(self.app_name)

            else:
                self.write_df_to_s3(claims_df, s3_output_path, partition=claim.partition_columns,
                                    redshift_partition_col='sourcesystemid')
                if self.is_debug:
                    s3_csv_path = format_s3_path(self.bucket_name, self.output_path, self.app_name, output_format='csv')
                    self.write_df_to_s3(claims_df, s3_csv_path, out_format='csv', partition=claim.partition_columns)

                self.logger.info("Job executed successfully")

        except PostValidationError:
            self.logger.error("Claims Post Validation Failed for source system id ->" + str(key_value))
            self.logger.error("Using Archive data for source system id ->" + str(key_value))
            self.logger.error("Failing the Glue job ->" + str(key_value))
            raise ThresholdValidationError(self.app_name, "sourcesystemid", key_value)


def main(*params, **kwargs) -> None:
    param = ['CATALOG_DB_NAME', 'OUTPUT_BUCKET', 'OUTPUT_BUCKET_PREFIX', 'PARTITION_COLUMNS', 'FILTER_ID_LIST', 'DEBUG',
             'PARTITION_PREDICATE', 'DATA_FILTERS', 'CONF_BUCKET', 'CONF_FILE_PREFIX']
    Claims(param).execute()
