from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class RdrExchangeRate(AbstractGlueTableMapping):
    schema_name = config.RDR_SCHEMA
    table_name = "exchange_rate_history_cross_currency"

    @property
    def schema(self):
        schema = StructType([StructField('to_currency_key', StringType(), True),
                             StructField('from_currency_key', StringType(), True),
                             StructField('effective_date', TimestampType(), True),
                             StructField('exchange_rate_current', DecimalType(13, 6), True)
                             ])

        return schema


def rdr_exchangerate(*args, **kwargs):
    rdr_exr_obj = RdrExchangeRate()
    rdr_exr_obj.initialize_table(**kwargs)
    return rdr_exr_obj
