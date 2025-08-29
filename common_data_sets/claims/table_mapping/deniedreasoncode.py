from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class DeniedReasonCode(AbstractGlueTableMapping):
    table_name = "reasondeniedreasoncode"
    schema_name = config.IDS_SCHEMA
    schema = StructType([StructField('deniedreason', StringType(), True),
                         StructField('deniedreasoncode', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def deniedreasoncode(*args, **kwargs):
    rsn = DeniedReasonCode()
    rsn.initialize_table(**kwargs)
    return rsn
