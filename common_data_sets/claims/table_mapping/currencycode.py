from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class CurrencyCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "currencycode"
    schema = StructType([
                         StructField('currencycode', StringType(), True),
                         StructField('currency', StringType(), True)]
                        )


def currencycode(*args, **kwargs):
    cur_co = CurrencyCode()
    cur_co.initialize_table(**kwargs)
    return cur_co
