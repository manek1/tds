from common_data_sets.common.spark_common_functions import *
from pyspark.sql import DataFrame, Window as Win
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, when, datediff, to_date, months_between, current_date, lit, broadcast,
                                   to_timestamp, row_number, sum, rank, date_format, coalesce, rpad, translate, length,
                                   split, substring, concat_ws, collect_list, asc_nulls_last, array_join, max, trim)
from typing import List

DATE_FORMAT_WITH_TS = '%Y-%m-%d %H:%M:%S'


class ClaimsTransformations:

    def __init__(self, spark: SparkSession, logger, is_debug: bool = False):
        self.spark = spark
        self.logger = logger
        self.is_debug = is_debug

    def prepare_exchange_rate(self, exchangerate_df, exchangeratedesired='USD'):
        """
        :param exchangerate_df: Spark DataFrame
        :param exchangeratedesired: String for which exchangerates needs to be extracted
        :return: Spark DataFrame with all exchangerates
        """

        if is_empty(exchangerate_df):
            logger.warning("IDS Exchangerate table is empty")
            return create_empty_df(self.spark)

        exchangerate_df = (exchangerate_df
                           .withColumn("exchange_priority",
                                       when(col('exchangeratesourcecode') == 'OAB', lit(1))
                                       .otherwise(lit(2))
                                       )
                           )

        exc_win = (Win.partitionBy(col('exchangeratedate'), col('exchangeratecurrencytocode'))
                   .orderBy(col('exchange_priority').asc(), col("exchangerate").asc()))

        exchangerate_df = exchangerate_df.withColumn("exchange_rank", rank().over(exc_win))

        final_exchange_df = (exchangerate_df
                             .where((col("exchangeratecurrencyfromcode") == exchangeratedesired)
                                    & (col("exchange_rank") == 1))
                             .selectExpr('exchangeratecurrencyfromcode as exchangeratecurrencytocode',
                                         'exchangeratecurrencytocode as exchangeratecurrencyfromcode',
                                         'exchangeratedate',
                                         'round(1/exchangerate, 6) as exchangerate')
                             )

        return final_exchange_df

    def prepare_rdr_exchange_rate(self, rdr_exchangerate_df, exchangeratedesired='USD'):
        """
        :param rdr_exchangerate_df: Spark DataFrame
        :param exchangeratedesired: String for which exchangerates needs to be extracted
        :return: Spark DataFrame with all exchangerates
        """

        if is_empty(rdr_exchangerate_df):
            logger.warning("RDR Exchangerate table is empty")
            return create_empty_df(self.spark)

        rdr_exchange_df = rdr_exchangerate_df.where(col("to_currency_key") == exchangeratedesired)

        return rdr_exchange_df

    def prepare_io_exchange_rate(self, koers_df: DataFrame, exchangeratedesired: str = 'USD'):
        self.logger.info(f"creating io exchangerate for {exchangeratedesired}")
        if is_empty(koers_df):
            logger.warning("Archmcc Exchangerate table is empty")
            return create_empty_df(self.spark)

        koers_df = (koers_df
                    .withColumn("exchange_month", date_format(col("datumkoers"), "yyyyMM"))
                    .where((col("swiftmuntvan") == 'USD') & (col("codekoers") == 'U')
                           & (col("exchange_month") >= '202001'))
                    .selectExpr('round(1/koers, 6) as io_exchangerate',
                                'swiftmuntnaar as productcurrencycode',
                                'datumkoers as reporteddate')
                    .distinct()
                    )

        return koers_df

    def create_payment_df(self, claimpayment_df: DataFrame, party_df: DataFrame, partyroletype_df: DataFrame,
                          payment_method_code_df: DataFrame, clm_sts_lvl_1_df: DataFrame, clm_sts_lvl_2_df: DataFrame,
                          clm_sts_lvl_3_df: DataFrame, currency_df: DataFrame, country_df: DataFrame,
                          banking_df: DataFrame, clm_settle_df: DataFrame):
        """
        :return: pyspark DataFrame - returns the payment details with columns need in claims analytics
        """
        pay_curr_df = currency_df.selectExpr("currencycode as paymentcurrencycode", "currency as paymentcurrency")

        if is_non_empty(clm_settle_df):
            self.logger.info(f'creating the payment dataframe')

            # Need to check it with SS1
            claimpayment_df = (claimpayment_df
                               .join(clm_settle_df, on=['claimsettlementid', 'sourcesystemid'], how="left")
                               )

            if is_non_empty(party_df) and is_non_empty(partyroletype_df):
                claimpayment_df = (claimpayment_df
                                   .join(party_df, on=['partyid', 'sourcesystemid'], how="left")
                                   .join(broadcast(partyroletype_df), on=["partyroletypecode"], how="left")
                                   .join(broadcast(payment_method_code_df), on=["paymentmethodcode"], how="left")
                                   .join(broadcast(clm_sts_lvl_1_df), on=["claimpaymentstatuslevel1code"], how='left')
                                   .join(broadcast(clm_sts_lvl_2_df), on=["claimpaymentstatuslevel2code"], how='left')
                                   .join(broadcast(clm_sts_lvl_3_df), on=["claimpaymentstatuslevel3code"], how='left')
                                   .join(broadcast(pay_curr_df), on=['paymentcurrencycode'], how='left')
                                   .withColumnRenamed("paymentmethod", "paymentchannel")
                                   )
                self.logger.info(f'Adding party data into claims')
            country_df = (country_df
                          .selectExpr("countrycode as bankcountrycode", "country as bankcountry", "sourcesystemid")
                          .distinct()
                          )
            banking_df = banking_df.join(country_df, on=["bankcountrycode", "sourcesystemid"], how="left")
            claimpayment_df = claimpayment_df.join(broadcast(banking_df),
                                                   on=["bankaccountinternalid", "sourcesystemid"], how="left")
        else:
            claimpayment_df = create_empty_df(self.spark)
            self.logger.info(f'No data present in payment tables')

        return claimpayment_df

    def create_country_of_res_df(self, cor_df: DataFrame, country_df: DataFrame):
        """
        :return: pyspark DataFrame - returns country details of customer
        """
        cor_window = (Win.partitionBy("customerid", "sourcesystemid")
                      .orderBy("countryofresidencetypecode", col("countryofresidencestartdate").desc()))

        cor_df = (cor_df.withColumn("country_rank", row_number().over(cor_window))
                  .drop(*["countryofresidencestartdate", "countryofresidenceenddate"])
                  )
        cor_df = cor_df.where(col('country_rank') == 1)

        if self.is_debug:
            self.logger.info(f"cor_df columns {cor_df.columns}")
            self.logger.info(f"cor_df count {cor_df.count()}")

        country_df = (country_df
                      .selectExpr("countrycode as countryofresidencecode", "country as countryofresidence")
                      .distinct()
                      )

        cor_df = cor_df.join(broadcast(country_df), on=['countryofresidencecode'], how='left')

        return cor_df

    def create_member_ins_telling_df(self, member_df: DataFrame, ins_telling_df: DataFrame):
        if is_non_empty(ins_telling_df):
            self.logger.info(f'Adding INS telling data to member')

            ins_telling_df = ins_telling_df.withColumn('segment', lit('IO')) \
                .withColumn('market', when(col('businessline') == 'C', 'Corporate')
                            .when(col('businessline') == 'I', 'IGO')
                            .when(col('businessline') == 'N', 'NGO')
                            .when(col('businessline') == 'A', 'AFRICA')
                            .when(col('businessline') == 'G', 'GOVT')
                            .when(col('businessline') == 'O', 'O').otherwise(lit('OTHER'))) \
                .withColumnRenamed('segment', 'i_segment') \
                .withColumnRenamed('market', 'i_market')

            member_code_ins_df = (member_df
                                  .where(col('sourcesystemid') == '1')
                                  .withColumn('codeinstelling', substring(col("memberid"), 1, 3))
                                  .select('memberid', 'codeinstelling', 'sourcesystemid').distinct()
                                  )

            member_code_ins_df = (member_code_ins_df
                                  .join(broadcast(ins_telling_df), on=['codeinstelling'], how='inner')
                                  .drop(*["codeinstelling", "businessline"])
                                  )

            member_df = member_df.join(broadcast(member_code_ins_df), on=['memberid', 'sourcesystemid'], how='left')

            return member_df

        return member_df

    def create_member_df(self, cor_df: DataFrame, member_df: DataFrame, clientstaffcategory_df: DataFrame,
                         customer_df: DataFrame, family_type_df: DataFrame, nationality_df: DataFrame,
                         country_df: DataFrame):

        self.logger.info(f'Adding the customer information dataframe into member')

        member_df = (member_df
                     .join(broadcast(clientstaffcategory_df), on=['clientstaffcategoryid', 'sourcesystemid'], how='left')
                     )

        nat_win = Win.partitionBy('customerid', 'sourcesystemid').orderBy(col('nationalitypriority').asc_nulls_last())

        nationality_df = (nationality_df
                          .withColumn('n_rank', row_number().over(nat_win))
                          .where(col('n_rank') == 1)
                          )

        country_df = (country_df
                      .selectExpr("countrycode as nationalitycode", "country as nationality")
                      .distinct()
                      )

        nationality_df = nationality_df.join(broadcast(country_df), on=["nationalitycode"], how='left')

        # Add missing nationalities based on familyunit IDs

        nationality_win = (Win.partitionBy('familyunitid', 'sourcesystemid')
                           .orderBy(col('fam_nationalitycode').asc_nulls_last()))

        nationality_family_df = (member_df
                                 .join(nationality_df, on=["customerid", "sourcesystemid"], how="left")
                                 .where((col('nationalitycode').isNotNull()) & (col('relationshiptypecode') == 'PRI')
                                        & (col('sourcesystemid') == '5'))
                                 .selectExpr('familyunitid', 'nationalitycode as fam_nationalitycode',
                                             'nationality as fam_nationality', 'sourcesystemid')
                                 .withColumn("nationality_rnk", row_number().over(nationality_win))
                                 )

        nationality_family_df = nationality_family_df.where(col('nationality_rnk') == 1)

        member_df = (member_df
                     .join(customer_df, on=["customerid", "sourcesystemid"], how="inner")
                     .join(cor_df, on=["customerid", "sourcesystemid"], how="left")
                     .join(family_type_df, on=["familyunitid", "sourcesystemid"], how="left")
                     .join(nationality_df, on=["customerid", "sourcesystemid"], how="left")
                     .join(nationality_family_df, on=["familyunitid", "sourcesystemid"], how="left")
                     .withColumn('nationality', when(col('nationality').isNull(), col('fam_nationality'))
                                 .otherwise(col('nationality')))
                     .withColumn('nationalitycode', when(col('nationalitycode').isNull(),
                                                         col('fam_nationalitycode')).otherwise(col('nationalitycode')))
                     )

        self.logger.info(f'created the member with customer information dataframe')
        return member_df

    def create_claiminvoice_df(self, claiminv_df: DataFrame, partner_df: DataFrame, clm_inv_ext: DataFrame,
                               clm_ext_inv_df: DataFrame, clm_inv_nw_df: DataFrame, country_df: DataFrame,
                               currency_df: DataFrame, partyroletype_df: DataFrame, ciesc_df, ciicsc_df,
                               invoice_list: list):

        if invoice_list:
            claiminv_df = (claiminv_df
                           .where(col('originalclaiminvoiceid').isin(invoice_list))
                           )
            self.logger.info(f'filtered the claim invoice data for invoice id {invoice_list}')

        claiminv_df = (claiminv_df
                       .join(broadcast(ciicsc_df), on=['claiminvoicestatuslevel3code'], how='left')
                       .join(broadcast(ciesc_df), on=['claiminvoicestatuslevel2code'], how='left')
                       )

        country_inc_df = (country_df
                          .selectExpr("countrycode as incurredcountrycode", "country as incurredcountry")
                          .distinct())

        incr_curr_df = currency_df.selectExpr("currencycode as incurredcurrencycode", "currency as incurredcurrency")
        prd_curr_df = currency_df.selectExpr("currencycode as productcurrencycode", "currency as productcurrency")

        claiminv_df = (claiminv_df
                       .join(broadcast(country_inc_df), on=['incurredcountrycode'], how='left')
                       .join(broadcast(incr_curr_df), on=['incurredcurrencycode'], how='left')
                       .join(broadcast(prd_curr_df), on=['productcurrencycode'], how='left')
                       .join(broadcast(clm_inv_nw_df), on=['networkusagecode'], how='left')
                       )

        self.logger.info(f'Added incurredcountry, networkusage, incurredcurrency and  productcurrency in claim invoice')

        claiminv_df = claiminv_df.join(clm_ext_inv_df, on=['claimexternalinvoiceid', 'sourcesystemid'], how='left')

        claiminv_df = (claiminv_df
                       .join(broadcast(clm_inv_ext), on=['claiminvoicestatuslevel1code'], how='left')
                       )

        self.logger.info(f'Added partner info in the claim invoice data')

        partner_df = (partner_df
                      .withColumnRenamed("cignalinksindicator", "cignalinkspartnerindicator")
                      )
        self.logger.info(f'Added partner info in the claim invoice data')

        claiminv_df = claiminv_df.join(broadcast(partner_df), on=["partnerid", "sourcesystemid"], how="left")
        claiminv_df = claiminv_df.join(broadcast(partyroletype_df), on=[
            claiminv_df['paymentrequestorroletypecode'] == partyroletype_df['partyroletypecode']], how='left')
        claiminv_df = claiminv_df.withColumnRenamed('partyroletype', 'paymentrequestor')
        self.logger.info(f'Added partyroletype info in the claim invoice data')


        return claiminv_df

    def add_recovered_amount(self, invoice_recovery_df: DataFrame, claim_recovery_df: DataFrame,
                             claim_result_det_df: DataFrame):
        """
        :return: pyspark DataFrame - returns claim result df with recoveredamount column added
        """

        inv_rec_temp_df = invoice_recovery_df.join(claim_recovery_df, on=["recoveryid", "sourcesystemid"], how="inner")

        inv_rec_temp_df = inv_rec_temp_df.groupBy("claiminvoiceid", "sourcesystemid").agg(sum("recoveredamount").
                                                                                          alias("recoveredamount"))

        if is_empty(inv_rec_temp_df):
            self.logger.info(f'no recovered-amount found and returned claim_result_det_df')
            return claim_result_det_df

        inv_rec_claim = (claim_result_det_df.join(inv_rec_temp_df, on=["claiminvoiceid", "sourcesystemid"], how="left")
                         .select(claim_result_det_df["*"], inv_rec_temp_df["recoveredamount"]))

        self.logger.info(f'Added the column recovered-amount from claim recovery table')

        return inv_rec_claim

    def create_claim_final_df(self, member_df: DataFrame, pay_df: DataFrame, claiminv_df: DataFrame,
                              claimdet_df: DataFrame,
                              benefit_df: DataFrame, placeofservicecode_df: DataFrame,
                              rdr_exchangerate_df: DataFrame, koers_df: DataFrame, claimsubmission_df: DataFrame,
                              clm_sub_channel_df: DataFrame, claims_view_df: DataFrame, claim_start_date='1000-01-01',
                              col_date_format='yyyy-MM-dd'):

        claiminv_final_df = claiminv_df.join(member_df, on=["memberid", "sourcesystemid"], how="left")
        self.logger.info('Added the member information to claim invoice data & claculating segment market')

        claiminv_final_df = claiminv_final_df.join(claimdet_df, on=["claiminvoiceid", "sourcesystemid"], how="inner")

        claiminv_final_df = (claiminv_final_df
                             .withColumn("segment",
                                         when(col("sourcesystemid").isin("8", "5"), col('legalsegment')).
                                         when(col("sourcesystemid") == "1", col('i_segment')).
                                         when(col("sourcesystemid").isin('2', '3', '4'), col('p_segment')).
                                         otherwise(None))
                             .withColumn("market",
                                         when(col("sourcesystemid").isin("8", "5"), col('legalmarket')).
                                         when(col("sourcesystemid") == "1", col('i_market')).
                                         when(col("sourcesystemid").isin('2', '3', '4'), col('p_market')).
                                         otherwise(None))
                             )

        claiminv_final_df = (claiminv_final_df
                             .join(broadcast(benefit_df), on=["benefitid", "sourcesystemid"], how="left")
                             .join(broadcast(placeofservicecode_df), on=["placeofservicecode"], how="left"))

        claiminv_final_df = (claiminv_final_df
                             .withColumn('reporteddate',
                                         when(col('reporteddate').isNotNull(), col('reporteddate'))
                                         .when((col('paymentid').isNull())
                                               & (col('claiminvoicestatuslevel2') != 'Paid'), col('servicedate'))
                                         .otherwise(lit(None))
                                         )
                             )

        claiminv_final_df = (claiminv_final_df
                             .join(claims_view_df, on=['internalprocedurecode', 'originalclaiminvoiceid'],
                                   how='left')
                             )

        self.logger.info("Added claims view data")

        if is_non_empty(claimsubmission_df):
            claimsubmission_df = claimsubmission_df.join(broadcast(clm_sub_channel_df), on=['claimchannelcode'],
                                                         how='left')

            claiminv_final_df = (claiminv_final_df
                                 .join(claimsubmission_df, on=["claimsubmissionid", "sourcesystemid"], how="left")
                                 )

            logger.info("Added claimsubmission information into claims dataset")

        if is_non_empty(pay_df):

            pay_df = (pay_df
                      .withColumn("pay_jc", when(col("sourcesystemid") == '1', col('claimsettlementid'))
                                  .otherwise(col('paymentid')))
                      .drop('claimsettlementid')
                      )

            # logger.info(f"io party record count {io_pay_df.count()}")

            claiminv_final_df = (claiminv_final_df
                                 .withColumn("pay_jc", when(col("sourcesystemid") == '1', col('claimsettlementid'))
                                             .otherwise(col('paymentid')))
                                 )

            claiminv_final_df = (claiminv_final_df
                                 .join(pay_df, on=["pay_jc", "sourcesystemid"], how="left")
                                 .where((col('paymentdate') >= to_timestamp(lit(claim_start_date), col_date_format))
                                        | ((col('claiminvoicereceiveddate') >= to_timestamp(lit(claim_start_date),
                                                                                            col_date_format))
                                           & ((col('paymentdate').isNull()) | (col('paymentdate') == '1900-01-01'))
                                           )
                                        )
                                 .withColumnRenamed("partyroletype", "tds_paidto")
                                 .drop('pay_jc')
                                 .drop(claiminv_final_df['paymentid'])
                                 )

            self.logger.info('Added the payment information to claim invoice data')

            # RDR Exchange rate on product currency code for GC & ACT

            if is_non_empty(rdr_exchangerate_df):
                rdr_exchangerate_df = (rdr_exchangerate_df
                                       .withColumnRenamed("exchange_rate_current", "rdr_prd_exchangerate")
                                       .withColumnRenamed("from_currency_key", "productcurrencycode")
                                       .withColumnRenamed("effective_date", "reporteddate")
                                       )

                rdr_clm_jc = [(rdr_exchangerate_df['productcurrencycode'] == claiminv_final_df['productcurrencycode'])
                              & (rdr_exchangerate_df['reporteddate'] == claiminv_final_df['reporteddate'])
                              & (~claiminv_final_df["sourcesystemid"].isin('5', '1'))]

                claiminv_final_df = (claiminv_final_df
                                     .join(broadcast(rdr_exchangerate_df), on=rdr_clm_jc, how='left')
                                     .drop(rdr_exchangerate_df['productcurrencycode'])
                                     .drop(rdr_exchangerate_df['reporteddate'])
                                     )
            if is_non_empty(koers_df):
                io_clm_jc = [(koers_df['productcurrencycode'] == claiminv_final_df['productcurrencycode'])
                             & (koers_df['reporteddate'] == claiminv_final_df['reporteddate'])
                             & (claiminv_final_df["sourcesystemid"] == '1')]

                claiminv_final_df = (claiminv_final_df
                                     .join(broadcast(koers_df), on=io_clm_jc, how='left')
                                     .drop(koers_df['productcurrencycode'])
                                     .drop(koers_df['reporteddate'])
                                     )

                claiminv_final_df = (claiminv_final_df
                                     .withColumn("exchangerate",
                                                 when((col("sourcesystemid") == '5')
                                                      | (col("productcurrencycode") == 'USD'), 1)
                                                 .when(col("sourcesystemid") == '1', col("io_exchangerate"))
                                                 .otherwise(col("rdr_prd_exchangerate"))
                                                 )
                                     )

            claiminv_final_df = (claiminv_final_df
                                 .withColumn("tds_paymentamountusd",
                                             col("productcurrencypaymentamount") * col("exchangerate"))
                                 .withColumn("tds_allowedamountusd", col("allowedamount") * col("exchangerate"))
                                 .withColumn("tds_networkfeeamountusd",
                                             col("networkfeeamount") * col("exchangerate"))
                                 .withColumn("tds_networkclientfeeamountusd", col("networkclientfeeamount")
                                             * col("exchangerate"))
                                 .withColumn("tds_billedamountusd",
                                             col("productcurrencybilledamount") * col("exchangerate"))
                                 .withColumn("tds_customershareamountusd",
                                             col("customershareamount") * col("exchangerate"))
                                 .withColumn("tds_notpayableamountusd", col("notpayableamount") * col("exchangerate"))
                                 .withColumn("tds_copaymentamountusd", col("copaymentamount") * col("exchangerate"))
                                 .withColumnRenamed("healthcarecommonprocedurecodingsystemcode", "hcpcscode")
                                 .withColumnRenamed("codeondentalproceduresandnomenclaturecode", "cdtcode")
                                 .withColumnRenamed("currentproceduralterminologycode", "cptcode")
                                 .withColumn("tds_grossamountusd", col("tds_paymentamountusd")
                                             + col("tds_networkclientfeeamountusd"))
                                 )

            self.logger.info("Calculated USD amounts")

            claiminv_final_df = (claiminv_final_df
                                 .withColumn("dateofbirth", to_date(col("dateofbirth"), 'yyyy-MM-dd'))
                                 .withColumn("servicedate", to_date(col("servicedate"), 'yyyy-MM-dd'))
                                 .withColumn('tds_claimage',
                                             (months_between(col('servicedate'), col('dateofbirth')) / 12).cast('int'))
                                 )

            self.logger.info("Added tds_claimage")

        return claiminv_final_df

    def add_provider_data(self, provider_df: DataFrame, claim_result_det_df: DataFrame, provider_type_df: DataFrame,
                          provider_group_df: DataFrame):

        if is_non_empty(provider_df):
            provider_df = (provider_df
                           .join(broadcast(provider_type_df), on=["providertypecode"], how="left")
                           .join(broadcast(provider_group_df), on=['providergroupcode'], how='left')
                           .withColumnRenamed('providerid', 'billingproviderid')
                           .withColumnRenamed('providertype', 'billingprovidertype')
                           .withColumnRenamed('providertypecode', 'billingprovidertypecode')
                           .withColumnRenamed('masterproviderid', 'billingmasterproviderid')
                           .withColumnRenamed('providergroup', 'billingprovidergroup')
                           .withColumnRenamed('providercompanyname', 'billingprovidercompanyname')
                           .withColumnRenamed('providergroupcode', 'billingprovidergroupcode')
                           )

            claim_result_det_df = (claim_result_det_df
                                   .join(provider_df, ['billingproviderid', 'sourcesystemid'], how="left")
                                   )
            if self.is_debug:
                self.logger.info(f"count of result_det_df after provider join {claim_result_det_df.count()}")
                self.logger.info(f"columns of result_det_df after provider join {claim_result_det_df.columns}")
        return claim_result_det_df

    def add_amountdenied_data(self, amountdenied_df: DataFrame, claim_result_det_df: DataFrame):
        claim_result_det_df = (claim_result_det_df
                               .join(amountdenied_df, on=["claimlineid", "sourcesystemid"], how="left")
                               .withColumn("tds_deniedamountusd", col("deniedamount") * col("exchangerate"))
                               )

        self.logger.info("Added Amount Denied data")

        return claim_result_det_df

    def add_amounttax_data(self, amounttax_df: DataFrame, claim_result_det_df: DataFrame):
        if is_non_empty(amounttax_df):
            amounttax_df = (amounttax_df
                            .groupBy("claimlineid", "sourcesystemid")
                            .agg(sum("taxamount").alias("taxamount"))
                            )

            claim_result_det_df = claim_result_det_df.join(amounttax_df, on=["claimlineid", "sourcesystemid"],
                                                           how="left")
            self.logger.info("Added Amount Tax data")
        return claim_result_det_df

    def add_legalentity_data(self, legalentity_df: DataFrame, lineofbusiness_df: DataFrame,
                             claim_result_det_df: DataFrame):

        legalentity_df = (legalentity_df
        .withColumn("legalsegment",
                    when((col("sourcesystemid") == "8") & (col("legalentityid").isin('42014', '42106')),
                         "Middle East & Africa").
                    when((col("sourcesystemid") == "8") & (col("legalentityid").isin('82904', '83006', '82441')),
                         "Domestic Healthcare")
                    .when(col("sourcesystemid") == "8", "Individual")
                    .when(col("sourcesystemid") == "5", "GHB")
                    .otherwise(None))
        .withColumn(
            "legalmarket",
            when((col("sourcesystemid") == "8") & (col("legalentityid").isin('42014', '42106')),
                 "Middle East").
            when((col("sourcesystemid") == "8") & (col("legalentityid").isin('82904', '83006')),
                 "Hong Kong").
            when((col("sourcesystemid") == "8") & (col("legalentityid").isin('82441')),
                 "Singapore").
            when((col("sourcesystemid") == "8") & (col("legalentityid").isin('86613')),
                 "Chubb").
            when(col("sourcesystemid") == "8", "GIH").
            when((col("sourcesystemid") == "5") & (col("legalentityid").isin('03001', '03002', '03044', '03054'
                                                                             , '08001', '08003', '08005', '08008',
                                                                             '09002'
                                                                             , '14421', '09007', '14422', '22002',
                                                                             '82802'
                                                                             , '53134', '82813', '84804', '84817',
                                                                             '84818'
                                                                             , '84834', '84848', '84856', '84876',
                                                                             '84890'
                                                                             , '84893', '84898', '53154', '84874',
                                                                             '42080'
                                                                             , '42081', '42082', '42083', '42084')),
                 "Americas").
            when((col("sourcesystemid") == "5") & (col("legalentityid").like('DE%')),
                 "Americas").
            when((col("sourcesystemid") == "5") & (col("legalentityid").isin('08002', '82807', '84880', '84897',
                                                                             '87101')),
                 "Australias").
            when(
                (col("sourcesystemid") == "5") & (col("legalentityid").isin('03045', '84805', '03046', '84806')),
                "Thailand").
            when(
                (col("sourcesystemid") == "5") | (col("legalentityid").isNull()), "OTHER")
            .otherwise(None))
        )

        legalentity_df = legalentity_df.join(lineofbusiness_df, on=['lineofbusinessid'], how='left')

        claim_legal_join_cond = ["financialbooktypecode", "legalentityid", "sourcesystemid"]
        if is_non_empty(legalentity_df):
            claim_result_det_df = (claim_result_det_df
                                   .join(broadcast(legalentity_df), on=claim_legal_join_cond, how='left')
                                   )
            self.logger.info("Added legalentity data")
        return claim_result_det_df

    def add_amountdiscount_data(self, amountdiscount_df: DataFrame, claim_result_det_df: DataFrame):
        if is_non_empty(amountdiscount_df):
            amountdiscount_df = (amountdiscount_df
                                 .groupBy("claimlineid", "sourcesystemid")
                                 .agg(sum("discountamount").alias("discountamount"))
                                 )
            claim_result_det_df = (claim_result_det_df
                                   .join(amountdiscount_df, on=["claimlineid", "sourcesystemid"], how="left")
                                   .withColumn("tds_discountamountusd", col("discountamount") * col("exchangerate"))
                                   .withColumn("tds_discountamountincurredcurrency",
                                               col("discountamount") * (col("incurredcurrencybilledamount")
                                                                        / col("productcurrencybilledamount")))
                                   )
            self.logger.info("Added amountdiscount data")
        return claim_result_det_df

    def add_source_name(self, sourcesystem_df: DataFrame, claims_final_df: DataFrame):
        if is_empty(sourcesystem_df):
            return claims_final_df
        claims_src_name_df = (claims_final_df
                              .join(broadcast(sourcesystem_df), on=["sourcesystemid"], how="left")
                              .withColumnRenamed("name", "sourcesystemname")
                              )
        self.logger.info("Added source system name")
        return claims_src_name_df

    def prepare_lookup_data(self, cci_df: DataFrame, ccs_df: DataFrame):
        cci_ccs_column_mapping = {"icd_9_code": "icd9jc",
                                  "CCS LVL 1 LABEL": "ccs_lv1_desc",
                                  "adjusted_level_2_label": "ccs_lv2_desc",
                                  "adjusted_level_3_label": "ccs_lv3_desc",
                                  "CCS LVL 1": "ccs_lv1_code",
                                  "CCS LVL 2": "ccs_lv2_code",
                                  "CCS LVL 3": "ccs_lv3_code",
                                  "CCI_Chronic": "cci_chronic"}
        self.logger.info(f"cci_df counts before dropping duplicates {str(cci_df.count())}")
        self.logger.info(f"ccs_df counts before dropping duplicates {str(ccs_df.count())}")

        cci_df = (cci_df
                  .drop_duplicates(subset=['ICD-9 CODE'])
                  .withColumnRenamed('ICD-9 CODE', 'icd_9_code')
                  )
        ccs_df = ccs_df.drop_duplicates(subset=['icd_9_code'])

        self.logger.info(f"cci_df counts after dropping duplicates {str(cci_df.count())}")
        self.logger.info(f"ccs_df counts after dropping duplicates {str(ccs_df.count())}")

        cci_ccs_df = ccs_df.join(cci_df, on=['icd_9_code'], how='left')
        cci_ccs_df = rename_columns(cci_ccs_df, cci_ccs_column_mapping)
        self.logger.info(f"cci_ccs_df counts {str(cci_ccs_df.count())}")
        return cci_ccs_df.select(list(cci_ccs_column_mapping.values()))

    def prepare_icd10_data(self, dx_ccr_df: DataFrame, dx_ccir_df: DataFrame) -> tuple:
        ccr_ccs_column_mapping = {'icd_10_code': 'icd10jc',
                                  'ICD-10-CM CODE DESCRIPTION': 'icd10_code_description',
                                  'Default CCSR CATEGORY IP': 'ccsr_category_ip',
                                  'Default CCSR CATEGORY DESCRIPTION IP': 'ccsr_category_description_ip',
                                  'Default CCSR CATEGORY OP': 'ccsr_category_op',
                                  'Default CCSR CATEGORY DESCRIPTION OP': 'ccsr_category_description_op',
                                  'CCSR CATEGORY 1': 'ccsr_category_1',
                                  'CCSR CATEGORY 1 DESCRIPTION': 'ccsr_category_1_description',
                                  'CCSR CATEGORY 2': 'ccsr_category_2',
                                  'CCSR CATEGORY 2 DESCRIPTION': 'ccsr_category_2_description',
                                  'CCSR CATEGORY 3': 'ccsr_category_3',
                                  'CCSR CATEGORY 3 DESCRIPTION': 'ccsr_category_3_description',
                                  'CHRONIC INDICATOR': 'icd10_chronic_indicator',
                                  'icd10codes_2024': 'icd10codes_2024'
                                  }

        dx_ccr_df = (dx_ccr_df.withColumnRenamed('ICD-10-CM CODE', 'icd_10_code')

                     )

        dx_ccir_df = (dx_ccir_df.drop('ICD-10-CM CODE DESCRIPTION')
                      .withColumnRenamed('ICD-10-CM CODE', 'icd_10_code')
                      )

        icd_10_description_df = (dx_ccr_df
                                 .join(dx_ccir_df, on=["icd_10_code"], how='inner')
                                 .withColumn("icd10codes_2024", col("icd_10_code"))
                                 )

        icd_10_merge_df = (replace_column_name_chars(icd_10_description_df, {' ': '_', '-': '_'})
                           .drop('icd10codes_2024'))

        rename_icd_10_description_df = rename_columns(icd_10_description_df, ccr_ccs_column_mapping)
        self.logger.info(f"cci_ccs_df counts {str(rename_icd_10_description_df.count())}")
        return rename_icd_10_description_df.select(list(ccr_ccs_column_mapping.values())), icd_10_merge_df

    def add_diagnosis_data(self, diagnosis_claim_df: DataFrame, claim_line_df: DataFrame, cci_ccs_df: DataFrame,
                           icd_10_description_df: DataFrame, cc_df: DataFrame, diag_code_df: DataFrame,
                           icd9_df: DataFrame, icd10_df: DataFrame):

        diagnosis_claim_df = (diagnosis_claim_df
                              .join(icd9_df, on=['internationalstatisticalclassificationofdiseasesicd9code'],
                                    how='left')
                              .join(icd10_df, on=['internationalstatisticalclassificationofdiseasesicd10code'],
                                    how='left')
                              .withColumnRenamed('internationalstatisticalclassificationofdiseasesicd9', 'icd9')
                              .withColumnRenamed('internationalstatisticalclassificationofdiseasesicd10', 'icd10')
                              )

        diagnosis_claim_df.select('icd9', 'icd10').show(5)

        cc_df = (cc_df
                 .withColumnRenamed('CODE_range_first_three_characters', 'icd_10_prefix')
                 .withColumnRenamed('CCSR_Body_System_Abbreviation_2023', 'ccsr_body_system_abbreviation_2024')
                 .withColumnRenamed('ICD_10_code_book_chapter_2024', 'icd_10_code_book_chapter_2024')
                 )

        if is_empty(diagnosis_claim_df):
            self.logger.error("Diagnosis data is not present")
            return claim_line_df

        diagnosis_claim_df = (diagnosis_claim_df
                              .withColumn("icd9jc",
                                          translate('internationalstatisticalclassificationofdiseasesicd9code', '.', '')
                                          )
                              .withColumn("icd10jc",
                                          translate('internationalstatisticalclassificationofdiseasesicd10code', '.', ''
                                                    )
                                          )
                              .withColumn('icd_10_prefix', substring(col("icd10jc"), 1, 3))
                              )

        diagnosis_claim_df = (diagnosis_claim_df
                              .withColumn('icd9jc',
                                          when(length(col("icd9jc")) < 5, rpad("icd9jc", 5, '0'))
                                          .otherwise(col("icd9jc"))
                                          )
                              )

        diagnosis_claim_df.select('icd9jc').show(10)
        cci_ccs_df.show(5)

        self.logger.info("Added Diag description into diag data")

        diagnosis_claim_df = (diagnosis_claim_df
                              .join(broadcast(cci_ccs_df), on=['icd9jc'], how='left')
                              .join(broadcast(icd_10_description_df), on=['icd10jc'], how='left')
                              .join(broadcast(cc_df), on=['icd_10_prefix'], how='left')
                              .join(broadcast(diag_code_df), on=['internaldiagnosiscode', 'sourcesystemid'], how='left')
                              )

        diag_win = Win.partitionBy("claimlineid", "sourcesystemid").orderBy(col("diag_priority").asc())

        diag_df = (diagnosis_claim_df
                   .withColumn("diag_priority",
                               when(col("primarydiagnosisindicator"), lit(1))
                               .when(~col("primarydiagnosisindicator"), lit(2))
                               .otherwise(lit(3)))
                   )


        diag_df = (diag_df.withColumn("diag_rank", row_number().over(diag_win))
                   .where(col("diag_rank") == 1)
                   .withColumnRenamed("internationalstatisticalclassificationofdiseasesicd10code", "icd10code")
                   .withColumnRenamed("internationalstatisticalclassificationofdiseasesicd9code", "icd9code")
                   .drop(*["diag_priority", "primarydiagnosisindicator", "diag_rank"])
                   )

        if is_empty(diag_df):
            self.logger.error("Diagnosis data is not present")
            return claim_line_df

        diagnosis_final_df = claim_line_df.join(diag_df, on=["claimlineid", "sourcesystemid"], how="left")
        self.logger.info("Added Diagnosis data into claims")
        return diagnosis_final_df

    def add_client_data(self, client_df: DataFrame, client_risk_df: DataFrame, policy_df: DataFrame,
                        claiminv_df: DataFrame, client_bs_code_df: DataFrame) -> DataFrame:

        client_df = (client_df.join(broadcast(client_bs_code_df), on=['clientbusinesssegmentcode'], how='left'))

        if is_empty(client_df) or is_empty(policy_df):
            return claiminv_df
        client_policy_join_df = (client_df
                                 .join(policy_df, on=["clientid", "sourcesystemid"], how="inner")
                                 .join(broadcast(client_risk_df), on=["claimsriskarrangementcode"], how="left")
                                 )
        if is_empty(client_policy_join_df):
            return claiminv_df

        claiminv_df = claiminv_df.join(client_policy_join_df, on=["policyid", "sourcesystemid"], how="left")

        self.logger.info(f'Added client Data into claims')

        return claiminv_df

    def create_family_type_df(self, family_unit_df: DataFrame, family_unit_type_df: DataFrame):
        family_type_df = family_unit_df.join(broadcast(family_unit_type_df), on=["familyunittypecode"])
        self.logger.info(f'created the family type information dataframe')
        return family_type_df

    def create_bob_df(self, dt_df: DataFrame, pdt_tree_df: DataFrame):

        self.logger.info("Formatting detailtransaction Data")

        dt_df = (dt_df
                 .where(~split(dt_df['financialdistributionid'], '-').getItem(1).isin(['SIMX', '0000']))
                 .withColumn('product_tree_key', split(dt_df['financialdistributionid'], '-').getItem(1))
                 )
        pdt_tree_df = pdt_tree_df.where(col('environment_key').isin('DL', 'EU', 'GB', 'IN', 'CY')) \
            .withColumn("sourcesystemid",
                        when(pdt_tree_df.environment_key == 'DL', '2')
                        .when(pdt_tree_df.environment_key == 'EU', '3')
                        .when(pdt_tree_df.environment_key == 'GB', '4')
                        .when(pdt_tree_df.environment_key == 'IN', '6')
                        .when(pdt_tree_df.environment_key == 'CY', '7')
                        .otherwise(pdt_tree_df.environment_key)
                        ).dropDuplicates()
        dt_tr_df = (dt_df
                    .join(broadcast(pdt_tree_df), on=['product_tree_key', 'sourcesystemid'], how='left')
                    .drop(dt_df["financialdistributionid"])
                    )
        cols_to_rename = {'business_line': 'edw_businessline',
                          'business_line_desc': 'edw_businesslinedesc',
                          'product_tree_key': 'edw_producttreekey',
                          'product_tree_desc': 'edw_producttreedesc',
                          'business_book': 'edw_businessbook',
                          'business_book_desc': 'edw_businessbookdesc'}
        dt_tr_df = rename_columns(dt_tr_df, cols_to_rename)
        return dt_tr_df.distinct()

    def add_case_data(self, claims_view_df: DataFrame, case_type_df: DataFrame):
        self.logger.info("preparing claims view data to get case information")
        # logic to handle union of df with non matching number of columns
        # claims_view_df = align_dataframes_to_max_schema(claims_view_df)

        claims_view_df = (claims_view_df
                          .join(case_type_df, on=['case_type_key', 'environment_key'], how='inner')
                          .withColumnRenamed("case_type_key", "tds_casetype")
                          .withColumnRenamed("case_type_desc", "tds_casetypename")
                          .withColumnRenamed('form_number', 'originalclaiminvoiceid')
                          .withColumn('internalprocedurecode', trim(claims_view_df['procedure_codes_key']))
                          )

        cv_win = (Win.partitionBy("originalclaiminvoiceid", "internalprocedurecode", "environment_key")
                  .orderBy(col("claim_detail_entered_date").desc()))

        claims_view_df = (claims_view_df
                          .withColumn("entry_rank", row_number().over(cv_win))
                          .where(col("entry_rank") == 1)
                          .drop("environment_key"))

        return claims_view_df

    def map_product_package_plan(self, product_df: DataFrame, package_df: DataFrame, plan_df: DataFrame,
                                 loc_df: DataFrame, productidmap_df: DataFrame):

        self.logger.info("preparing product package plan data")

        claims_prod_map_join_cond = [
            (product_df['productid'] == productidmap_df['product_id']) & (
                product_df['sourcesystemid'].isin('2', '3', '4'))]


        pkg_plan_product_df = (broadcast(product_df)
                               .join(broadcast(package_df), on=['productid', 'sourcesystemid'], how='inner')
                               .join(plan_df, on=['packageid', 'sourcesystemid'], how='inner')
                               .join(broadcast(loc_df), on=['lineofcovercode'], how='left')
                               .join(broadcast(productidmap_df), on=claims_prod_map_join_cond, how='left')
                               .withColumnRenamed("segment", "p_segment")
                               .withColumnRenamed("market", "p_market")
                               .drop('product_id')
                               )

        self.logger.info("Added market & segmentation for product package plan")

        return pkg_plan_product_df

    def prepare_procedure_data(self, procedure_df: DataFrame, terminology_df: DataFrame):
        if is_empty(terminology_df):
            self.logger.info("terminology data is not present")
            return procedure_df
        self.logger.info("Adding currentproceduralterminologycode data")
        procedure_df = (procedure_df
                        .join(broadcast(terminology_df), on=["currentproceduralterminologycode"], how='left')
                        .withColumnRenamed("currentproceduralterminology", "cpt")
                        .withColumnRenamed("currentproceduralterminologycode",
                                           "procedure_currentproceduralterminologycode")
                        )
        return procedure_df

    def create_product_df(self, pkg_plan_product_df, claimline_df: DataFrame, dt_tr_df: DataFrame,
                          procedure_df: DataFrame, claimline_service_df: DataFrame, claimline_external_sc_df: DataFrame,
                          claimline_external_stc_df: DataFrame, clm_int_clm_sc_df: DataFrame, admission_df: DataFrame,
                          modifier_code_df: DataFrame):

        if is_non_empty(claimline_external_stc_df):
            self.logger.info("Adding externalclaimstatustype into claims")
            claimline_df = (claimline_df
                            .join(broadcast(claimline_external_stc_df), on=['claimlinestatuslevel1code'], how='left')
                            )

        if is_non_empty(clm_int_clm_sc_df):
            self.logger.info("Adding internalclaimstatus & externalclaimstatus into claims")
            claimline_df = (claimline_df
                            .join(broadcast(claimline_external_sc_df), on=['claimlinestatuslevel2code'], how='left')
                            .join(broadcast(clm_int_clm_sc_df), on=['claimlinestatuslevel3code'], how='left')
                            )

        if is_non_empty(admission_df):
            self.logger.info(f"Adding claimline admission type details into claims {admission_df.show()}")
            claimline_df = claimline_df.join(broadcast(admission_df), on=['admissiontypecode'], how='left')

        if is_non_empty(modifier_code_df):
            self.logger.info("Adding claimline modifier type details into claims")
            claimline_df = claimline_df.join(broadcast(modifier_code_df), on=['currentproceduralterminologymodifiercode'], how='left')

        if is_non_empty(claimline_service_df):
            self.logger.info("Adding service info into claims")
            claimline_df = (claimline_df
                            .join(broadcast(claimline_service_df), on=['servicecode', 'sourcesystemid'], how='left')
                            )

        if is_non_empty(procedure_df):
            self.logger.info("Adding internalprocedure into claims")
            claimline_df = (claimline_df
                            .join(broadcast(procedure_df), on=["internalprocedurecode", "sourcesystemid"], how='left')
                            )

        self.logger.info("Added product information to claims")

        claim_product_df = claimline_df.join(broadcast(pkg_plan_product_df), on=['planid', 'sourcesystemid'],
                                             how='left')

        self.logger.info("Added book of business into claims dataset")
        claim_product_df = claim_product_df.join(dt_tr_df, on=['financetransactionid', 'sourcesystemid'], how='left')

        self.logger.info(f"Added product information to claims")

        if self.is_debug:
            self.logger.info(f"Record count after adding product information to claims {str(claim_product_df.count())}")

        return claim_product_df


    def prepare_reason_data(self, reason_df: DataFrame, denied_reason_code_df: DataFrame, reason_nc_us_df: DataFrame,
                            reimbursement_limit_df: DataFrame, customer_reduction_df: DataFrame):
        self.logger.info("Preparing reason data for claims")

        reason_df = (reason_df
                     .where(((col('deniedreasoncode').isNotNull())
                             | (col('deniedreasonuscode').isNotNull())
                             |(col('reasonnotcovereduscode').isNotNull())
                             | (col('reimbursementlimitreductionreasoncode').isNotNull())
                             | (col('reasonableandcustomaryreductionreasoncode').isNotNull())
                             ))
                     )

        reason_df = reason_df.groupBy("claimlineid", "sourcesystemid").agg(
            concat_ws(", ", collect_list("deniedreasoncode")).alias("deniedreasoncode"),
            concat_ws(", ", collect_list("reasonnotcovereduscode")).alias("reasonnotcovereduscode"),
            concat_ws(", ", collect_list("deniedreasonuscode")).alias("deniedreasonuscode"),
            concat_ws(", ", collect_list("reimbursementlimitreductionreasoncode"))
            .alias("reimbursementlimitreductionreasoncode"),
            concat_ws(", ", collect_list("reasonableandcustomaryreductionreasoncode"))
            .alias("reasonableandcustomaryreductionreasoncode")
        )

        rsn_us_rnk = Win.partitionBy(col("reasonnotcovereduscode")).orderBy(col("reasonnotcoveredus"))
        rsn_limit_rnk = (Win.partitionBy(col("reimbursementlimitreductionreasoncode"))
                         .orderBy(col("reimbursementlimitreductionreason")))

        reason_nc_us_df = (reason_nc_us_df
                           .withColumn("rsn_us_rnk", row_number().over(rsn_us_rnk))
                           .where(col("rsn_us_rnk")==1)
                           .drop("rsn_us_rnk"))

        reimbursement_limit_df = (reimbursement_limit_df
                                  .withColumn("rsn_limit_rnk", row_number().over(rsn_limit_rnk))
                                  .where(col("rsn_limit_rnk") == 1)
                                  .drop("rsn_limit_rnk"))

        reason_df = (reason_df
                     .join(broadcast(denied_reason_code_df), on=['deniedreasoncode', 'sourcesystemid'], how='left')
                     .join(broadcast(reason_nc_us_df), on=["reasonnotcovereduscode", 'sourcesystemid'], how='left')
                     .join(broadcast(reimbursement_limit_df),
                           on=["reimbursementlimitreductionreasoncode", 'sourcesystemid'], how='left')
                     .join(broadcast(customer_reduction_df),
                           on=["reasonableandcustomaryreductionreasoncode", 'sourcesystemid'],how='left')
                     )

        return reason_df

    def add_reason_code(self, claimline_df: DataFrame, reason_df: DataFrame, amountcopayment_df: DataFrame) \
            -> DataFrame:
        """
        claimline_df: DataFrame
        reason_df: DataFrame
        reason_code_df: DataFrame
        return: DataFrame
        """
        self.logger.info("Adding reason codes into claims data")

        amountcopayment_df = (amountcopayment_df
                              .groupBy("claimlineid", "sourcesystemid", "coinsuranceamount")
                              .agg(sum("copaymentamount").alias("copaymentamount"))
                              )

        if is_non_empty(amountcopayment_df):
            claimline_df = (claimline_df
                            .join(amountcopayment_df, on=["claimlineid", "sourcesystemid"], how="left")
                            .withColumn("tds_copayamountincurredcurrency", col("copaymentamount")
                                        * (col("incurredcurrencybilledamount")
                                           / col("productcurrencybilledamount")))
                            .withColumn("tds_deductibleamountincurredcurrency", col("deductibleamount")
                                        * (col("incurredcurrencybilledamount")
                                           / col("productcurrencybilledamount")))
                            .withColumn("tds_notcoveredamountincurredcurrency", col("notpayableamount")
                                        * (col("incurredcurrencybilledamount")
                                           / col("productcurrencybilledamount")))
                            )

            self.logger.info("Added amountcopayment data")

        claimline_df = (claimline_df
                        .join(reason_df, on=['claimlineid', 'sourcesystemid'], how='left')
                        .withColumn('bednight',
                                    when(col('admissiontypecode') == 'INP',
                                         datediff(col('dischargedate'), col('admissiondate')))
                                    .otherwise(None))
                        .withColumn("tds_allowedamountincurredcurrency", col("allowedamount")
                                    * (col("incurredcurrencybilledamount") / col("productcurrencybilledamount")))
                        )

        self.logger.info("Added reason code into claims data")

        return claimline_df

    def add_code_description(self, claims_final_dataframe: DataFrame, hsrc_df: DataFrame, pcdtc_df: DataFrame,
                             clctc_df: DataFrame):

        self.logger.info("Adding code column descriptions")

        claims_final_dataframe = (claims_final_dataframe
                                  .join(broadcast(hsrc_df), on=['hospitalrevenuecode'], how='left')
                                  .join(broadcast(pcdtc_df), on=['proceduretypecode'], how='left')
                                  .join(broadcast(clctc_df), on=['claimtypecode'], how='left')
                                  )

        self.logger.info("Added code column descriptions")

        return claims_final_dataframe

    def create_ancestor_cols(self, client_df: DataFrame, client_sector_code_df: DataFrame) -> DataFrame:
        self.logger.info("Adding internalindustrysector columns")
        client_df = (client_df
                     .join(broadcast(client_sector_code_df),
                           on=['internalindustrysectorcode', 'sourcesystemid'], how='left')
                     )

        self.logger.info("Adding ancestor columns")
        df = client_df.select('clientid', 'clientlevel', 'parentclientid', 'sourcesystemid')
        initial_df = df.filter(col("clientlevel") == 1) \
            .withColumn("ancestorid", col("clientid")) \
            .withColumn("lowestlevel", when(col("clientlevel") == 3, "Yes").otherwise("")).select('clientid',
                                                                                                  'clientlevel',
                                                                                                  'parentclientid',
                                                                                                  'ancestorid',
                                                                                                  'lowestlevel',
                                                                                                  'sourcesystemid')
        df_level_2 = df.alias('t') \
            .join(initial_df.alias('cte'), [col('t.parentclientid') == col('cte.clientid'),
                                            col('t.sourcesystemid') == col('cte.sourcesystemid')], "inner") \
            .where(col("t.clientlevel") == 2) \
            .select(
            col('t.clientid'),
            col('t.clientlevel'),
            col('t.parentclientid'),
            col('cte.ancestorid'),
            when(col("t.clientlevel") == 3, "Yes").otherwise("").alias("lowestlevel"),
            col('t.sourcesystemid')
        )
        df_level_3 = df.alias('t') \
            .join(df_level_2.alias('cte'), [col('t.parentclientid') == col('cte.clientid'),
                                            col('t.sourcesystemid') == col('cte.sourcesystemid')], "inner") \
            .where(col("t.clientlevel") == 3) \
            .select(
            col('t.clientid'),
            col('t.clientlevel'),
            col('t.parentclientid'),
            col('cte.ancestorid'),
            when(col("t.clientlevel") == 3, "Yes").otherwise("").alias("lowestlevel"),
            col('t.sourcesystemid')
        )
        client_ance_df = initial_df.union(df_level_2).union(df_level_3).dropDuplicates().select('clientid',
                                                                                                'sourcesystemid',
                                                                                                'ancestorid')

        final_df = client_df.join(client_ance_df, on=['clientid', 'sourcesystemid'], how='left')
        self.logger.info("Added ancestor columns")
        return final_df
