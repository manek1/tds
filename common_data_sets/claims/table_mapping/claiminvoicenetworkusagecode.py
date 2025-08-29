from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ClaimInvoiceNetworkUsageCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claiminvoicenetworkusagecode"

    @property
    def schema(self):
        schema = StructType([
            StructField('networkusagecode', StringType(), True),
            StructField('networkusage', StringType(), True)])

        return schema


def claiminvoicenetworkusagecode(*args, **kwargs):
    clm_inv = ClaimInvoiceNetworkUsageCode()
    clm_inv.initialize_table(**kwargs)
    return clm_inv
