from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class Provider(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "provider"
    schema = StructType([StructField("providergroupcode", StringType(), True),
                         StructField("providercompanyname", StringType(), True),
                         StructField("masterproviderid", StringType(), True),
                         StructField("providertypecode", StringType(), True),
                         StructField("providerid", StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def provider(*args, **kwargs):
    prvider = Provider()
    prvider.initialize_table(**kwargs)
    return prvider
