from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ProviderTypeCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "providertypecode"
    schema = StructType([StructField("providertype", StringType(), True),
                         StructField("providertypecode", StringType(), True)])


def providertypecode(*args, **kwargs):
    prvider_typ_co = ProviderTypeCode()
    prvider_typ_co.initialize_table(**kwargs)
    return prvider_typ_co
