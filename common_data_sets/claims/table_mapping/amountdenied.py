from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType
from common_data_sets.common.configs import config


class AmountDenied(AbstractGlueTableMapping):
    table_name = "amountdenied"
    schema_name = config.IDS_SCHEMA
    schema = StructType([StructField('claimlineid', StringType(), True),
                         StructField('deniedamount', DecimalType(17, 2), True),
                         StructField('sourcesystemid', StringType(), True)])


def amountdenied(*args, **kwargs):
    amt_dny = AmountDenied()
    amt_dny.initialize_table(**kwargs)
    return amt_dny
