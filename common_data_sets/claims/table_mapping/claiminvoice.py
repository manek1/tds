from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ClaimInvoice(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claiminvoice"

    @property
    def schema(self):
        schema = StructType(
            [StructField('claimsubmissionid', StringType(), True), StructField('claiminvoiceid', StringType(), True),
             StructField('memberid', StringType(), True), StructField('partnerclaimid', StringType(), True),
             # StructField('claimexternalinvoicereference', StringType(), True),
             StructField('sourcesystemid', StringType(), True),
             StructField('originalclaiminvoiceid', StringType(), True),
             # StructField('admissiontypecode', StringType(), True),
             StructField('networkusagecode', StringType(), True),
             StructField('billingproviderid', StringType(), True), StructField('partnerid', StringType(), True),
             StructField('incurredcurrencycode', StringType(), True),
             StructField('incurredcountrycode', StringType(), True),
             # StructField('claimtypecode', StringType(), True),
             StructField('billingproviderspecialty', StringType(), True),
             StructField('claimlanguagecode', StringType(), True),
             StructField('claiminvoicereceiveddate', TimestampType(), True),
             StructField('claiminvoicestatuslevel1code', StringType(), True),
             StructField('claiminvoicestatuslevel2code', StringType(), True),
             StructField('claiminvoicestatuslevel3code', StringType(), True),
             StructField('reporteddate', TimestampType(), True),
             StructField('productcurrencycode', StringType(), True),
             StructField('claimexternalinvoiceid', StringType(), True),
             StructField('paymentrequestorroletypecode', StringType(), True)
             ])

        return schema


def claiminvoice(*args, **kwargs):
    clm_inv = ClaimInvoice()
    clm_inv.initialize_table(**kwargs)
    return clm_inv
