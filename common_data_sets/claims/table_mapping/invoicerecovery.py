from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType, BinaryType
from common_data_sets.common.configs import config


class InvoiceRecovery(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "invoicerecovery"

    @property
    def schema(self):
        schema = StructType([StructField('recoveryid', StringType(), True),
                             StructField('claiminvoiceid', StringType(), True),
                             StructField('sourcesystemid', StringType(), True)])

        return schema


def invoicerecovery(*args, **kwargs):
    clm = InvoiceRecovery()
    clm.initialize_table(**kwargs)
    return clm
