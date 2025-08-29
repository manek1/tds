from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ProviderGroupCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "providergroupcode"
    schema = StructType([StructField("providergroup", StringType(), True),
                         StructField("providergroupcode", StringType(), True)
                         ])


def providergroupcode(*args, **kwargs):
    prvider_grp_co = ProviderGroupCode()
    prvider_grp_co.initialize_table(**kwargs)
    return prvider_grp_co
