from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType
from common_data_sets.common.configs import config


class DetailTransaction(AbstractGlueTableMapping):
    table_name = "detailtransaction"
    schema_name = config.IDS_SCHEMA
    distinct_flag = True
    schema = StructType([StructField('financialdistributionid', StringType(), True),
                         StructField('financetransactionid', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def detail_transaction(*args, **kwargs):
    dt_table = DetailTransaction()
    dt_table.initialize_table(**kwargs)
    return dt_table
