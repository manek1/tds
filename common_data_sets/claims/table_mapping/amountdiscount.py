from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class AmountDiscount(AbstractGlueTableMapping):
    table_name = "amountdiscount"
    schema_name = config.IDS_SCHEMA
    distinct_flag = True
    schema = StructType([StructField('claimlineid', StringType(), True),
                         StructField('discountamount', DecimalType(17, 2), True),
                         StructField('discounttypecode', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def amountdiscount(*args, **kwargs):
    amt_dis = AmountDiscount()
    amt_dis.initialize_table(**kwargs)
    return amt_dis
