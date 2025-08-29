from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql.types import *


class Procedure(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "procedure"
    distinct_flag = True
    schema = StructType([StructField('internalprocedurecode', StringType(), True),
                         StructField("internalprocedure", StringType(), True),
                         StructField("currentproceduralterminologycode", StringType(), True),
                         StructField('sourcesystemid', StringType(), True)
                         ])


def procedure(*args, **kwargs):
    procedure_info = Procedure()
    procedure_info.initialize_table(**kwargs)
    return procedure_info
