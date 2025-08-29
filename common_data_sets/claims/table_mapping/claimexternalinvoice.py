from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ClaimExternalInvoice(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = 'claimexternalinvoice'
    schema = StructType([StructField('claimexternalinvoiceid', StringType(), True),
                         StructField('claimexternalinvoicereference', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)]
                        )


def claimexternalinvoice(*args, **kwargs):
    cs = ClaimExternalInvoice()
    cs.initialize_table(**kwargs)
    return cs
