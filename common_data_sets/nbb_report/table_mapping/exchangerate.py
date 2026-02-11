from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType, IntegerType, DoubleType, DateType
from common_data_sets.common.configs import config


class Exchangerate(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = 'exchangerate'

    schema = StructType([StructField("exchangeratecurrencyfromcode", StringType(), True),
                             StructField("exchangeratecurrencytocode", StringType(), True),
                             StructField("exchangeratedate", DateType(), True),
                             StructField("exchangeratesourcecode", StringType(), True),
                             StructField("exchangerate", DoubleType(), True)
                             ])


def exchangerate(*args, **kwargs):
    exc = Exchangerate()
    exc.initialize_table(**kwargs)
    return exc
