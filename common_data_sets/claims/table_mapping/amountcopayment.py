from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql.types import StringType, StructField, StructType, DecimalType


class AmountCoPayment(AbstractGlueTableMapping):
    table_name = "amountcopayment"
    schema_name = config.IDS_SCHEMA
    distinct_flag = True

    schema = StructType([StructField('claimlineid', StringType(), True),
                         StructField('copaymentamount', DecimalType(17, 2), True),
                         StructField('coinsuranceamount', DecimalType(17, 2), True),
                         StructField('sourcesystemid', StringType(), True)])


def amountcopayment(*args, **kwargs):
    amt_cpy = AmountCoPayment()
    amt_cpy.initialize_table(**kwargs)
    return amt_cpy
