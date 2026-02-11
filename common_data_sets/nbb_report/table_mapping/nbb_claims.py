from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import (StringType, StructField, StructType, TimestampType, IntegerType, StringType, DateType,
                               BooleanType, DecimalType)
from common_data_sets.common.configs import config


class NbbClaims(AbstractGlueTableMapping):
    schema_name = config.TDS_EXTRACTS_SCHEMA
    table_name = 'nbb_claims'

    schema = StructType([StructField("claimlineid", StringType(), True),
                         StructField("claiminvoiceid", StringType(), True),
                         StructField("memberid", StringType(), True),
                         StructField("originalclaiminvoiceid", StringType(), True),
                         StructField("partnerid", StringType(), True),
                         StructField("productid", StringType(), True),
                         StructField("policyid", StringType(), True),
                         StructField("legalentityid", StringType(), True),
                         StructField("claimchannel", StringType(), True),
                         StructField("internaldiagnosis", StringType(), True),
                         StructField("internaldiagnosiscode", StringType(), True),
                         StructField("internalprocedurecode", StringType(), True),
                         StructField("admissiontypecode", StringType(), True),
                         StructField("deniedreason", StringType(), True),
                         StructField("paymentcurrencycode", StringType(), True),
                         StructField("productcurrencycode", StringType(), True),
                         StructField("edw_businesslinedesc", StringType(), True),
                         StructField("edw_producttreedesc", StringType(), True),
                         StructField("edw_producttreekey", StringType(), True),
                         StructField("edw_businessbook", StringType(), True),
                         StructField("edw_businessline", StringType(), True),
                         StructField("lineofcover", StringType(), True),
                         StructField("market", StringType(), True),
                         StructField("countryofassignment", StringType(), True),
                         StructField("incurredcountry", StringType(), True),
                         StructField("legalentityname", StringType(), True),
                         StructField("partnername", StringType(), True),
                         StructField("policyname", StringType(), True),
                         StructField("bednight", IntegerType(), True),
                         StructField("benefitname", StringType(), True),
                         StructField("billingprovidercompanyname", StringType(), True),
                         StructField("ccs_lv2_desc", StringType(), True),
                         StructField("claimexternalinvoicereference", StringType(), True),
                         StructField("planname", StringType(), True),
                         StructField("tds_claimage", IntegerType(), True),
                         StructField("tds_casetype", StringType(), True),
                         StructField("claiminvoicestatuslevel1", StringType(), True),
                         StructField("claiminvoicestatuslevel2", StringType(), True),
                         StructField("claiminvoicestatuslevel3", StringType(), True),
                         StructField("claimlinestatuslevel1", StringType(), True),
                         StructField("claimlinestatuslevel2", StringType(), True),
                         StructField("claimlinestatuslevel3", StringType(), True),
                         StructField("claimpaymentstatuslevel1", StringType(), True),
                         StructField("claimpaymentstatuslevel2", StringType(), True),
                         StructField("claimpaymentstatuslevel3", StringType(), True),
                         StructField("claimsriskarrangement", StringType(), True),
                         StructField("cignalinkspartnerindicator", BooleanType(), True),
                         StructField("claiminvoicereceiveddate", DateType(), True),
                         StructField("paymentdate", DateType(), True),
                         StructField("reporteddate", DateType(), True),
                         StructField("servicedate", DateType(), True),
                         StructField("servicefromdate", DateType(), True),
                         StructField("policystartdate", DateType(), True),
                         StructField("policyenddate", DateType(), True),
                         StructField("productcurrencybilledamount",DecimalType(17, 2), True),
                         StructField("paymentcurrencypaymentamount", DecimalType(17, 2), True),
                         StructField("productcurrencypaymentamount", DecimalType(17, 2), True),
                         StructField("subscriberindicator", BooleanType(), True),
                         StructField("sourcesystemid", StringType(), True),
                         StructField("sourcesystemname", StringType(), True)
                         ])


def nbb_claims(*args, **kwargs):
    nbb_clm = NbbClaims()
    nbb_clm.initialize_table(**kwargs)
    return nbb_clm
