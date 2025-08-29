from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType
from common_data_sets.common.configs import config


class InvoicePayment(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "invoicepayment"
    schema = StructType([StructField('claiminvoiceid', StringType(), True),
                         StructField('paymentid', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def invoicepayment(*args, **kwargs):
    inv_pay = InvoicePayment()
    inv_pay.initialize_table(**kwargs)
    return inv_pay
