from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType
from common_data_sets.common.configs import config


class ReasonableAndCustomaryReductionReasonCode(AbstractGlueTableMapping):
    table_name = "reasonableandcustomaryreductionreasoncode"
    schema_name = config.IDS_SCHEMA
    distinct_flag = True
    schema = StructType([StructField('reasonableandcustomaryreductionreasoncode', StringType(), True),
                         StructField('reasonableandcustomaryreductionreason', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)
                         ])


def reasonableandcustomaryreductionreasoncode(*args, **kwargs):
    rsn = ReasonableAndCustomaryReductionReasonCode()
    rsn.initialize_table(**kwargs)
    return rsn
