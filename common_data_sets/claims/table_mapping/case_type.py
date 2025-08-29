from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql.types import StringType, StructField, StructType, DecimalType


class CaseType(AbstractGlueTableMapping):
    table_name = "case_type"
    schema_name = config.RDR_SCHEMA

    schema = StructType([StructField('case_type_key', StringType(), True),
                         StructField('environment_key', StringType(), True),
                         StructField('case_type_desc', StringType(), True)
                         ])


def case_type(*args, **kwargs):
    case_tp = CaseType()
    case_tp.initialize_table(**kwargs)
    return case_tp
