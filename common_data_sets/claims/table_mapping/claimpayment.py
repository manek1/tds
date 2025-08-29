from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ClaimPayment(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimpayment"

    @property
    def schema(self):
        schema = StructType([StructField('paymentid', StringType(), True),
                             StructField('paymentdate', TimestampType(), True),
                             StructField('paymentcurrencycode', StringType(), True),
                             StructField('claimpaymentstatuslevel1code', StringType(), True),
                             StructField('claimpaymentstatuslevel2code', StringType(), True),
                             StructField('claimpaymentstatuslevel3code', StringType(), True),
                             StructField('partyid', StringType(), True),
                             StructField('claimsettlementid', StringType(), True),
                             StructField('sourcesystemid', StringType(), True),
                             StructField('originalpaymentid', StringType(), True),
                             StructField('checkid', StringType(), True),
                             StructField('bankaccountinternalid', StringType(), True),
                             StructField('paymentmethodcode', StringType(), True),
                             StructField('externalpaymentreference', StringType(), True),
                             StructField('paymentcurrencytotalpaymentamount', DecimalType(17, 2), True)
                             ])

        return schema


def claimpayment(*args, **kwargs):
    clm = ClaimPayment()
    clm.initialize_table(**kwargs)
    return clm
