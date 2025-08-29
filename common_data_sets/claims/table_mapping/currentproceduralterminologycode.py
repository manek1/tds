from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql.types import *


class CurrentProceduralTerminologyCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "currentproceduralterminologycode"
    distinct_flag = True
    schema = StructType([StructField('currentproceduralterminology', StringType(), True),
                         StructField("currentproceduralterminologycode", StringType(), True)
                         ])


def currentproceduralterminologycode(*args, **kwargs):
    procedure_info = CurrentProceduralTerminologyCode()
    procedure_info.initialize_table(**kwargs)
    return procedure_info
