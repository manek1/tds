from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql.types import *


class ProcedureTypeCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "proceduretypecode"
    schema = StructType([StructField('proceduretypecode', StringType(), True),
                         StructField("proceduretype", StringType(), True)])


def proceduretypecode(*args, **kwargs):
    proceduretypecode_info = ProcedureTypeCode()
    proceduretypecode_info.initialize_table(**kwargs)
    return proceduretypecode_info
