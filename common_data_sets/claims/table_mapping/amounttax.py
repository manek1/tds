from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class AmountTax(AbstractGlueTableMapping):
    table_name = "amounttax"
    schema_name = config.IDS_SCHEMA
    schema = StructType([StructField('claimlineid', StringType(), True),
                         StructField('taxamount', DecimalType(17, 2), True),
                         StructField('sourcesystemid', StringType(), True)])


def amounttax(*args, **kwargs):
    amt_tx = AmountTax()
    amt_tx.initialize_table(**kwargs)
    return amt_tx
