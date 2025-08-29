from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType
from common_data_sets.common.configs import config


class ReasonNotCoveredUsCode(AbstractGlueTableMapping):
    table_name = "reasonnotcovereduscode"
    schema_name = config.IDS_SCHEMA
    distinct_flag = True
    schema = StructType([StructField('reasonnotcovereduscode', StringType(), True),
                         StructField('reasonnotcoveredus', StringType(), True)
                         ])


def reasonnotcovereduscode(*args, **kwargs):
    rsn = ReasonNotCoveredUsCode()
    rsn.initialize_table(**kwargs)
    return rsn
