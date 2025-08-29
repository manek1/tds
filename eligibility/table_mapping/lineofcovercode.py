from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, BooleanType
from common_data_sets.common.configs import config


class LineOfCoverCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "lineofcovercode"
    schema = StructType([StructField("lineofcovercode", StringType(), True),
                         StructField("lineofcover", StringType(), True)
                         ])


def lineofcovercode(*args, **kwargs):
    loc = LineOfCoverCode()
    loc.initialize_table(**kwargs)
    return loc
