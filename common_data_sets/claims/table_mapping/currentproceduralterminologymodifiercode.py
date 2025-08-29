from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType
from common_data_sets.common.configs import config


class CurrentProceduralTerminologyModifierCode(AbstractGlueTableMapping):
    table_name = "currentproceduralterminologymodifiercode"
    schema_name = config.IDS_SCHEMA
    schema = StructType([StructField('currentproceduralterminologymodifier', StringType(), True),
                         StructField('currentproceduralterminologymodifiercode', StringType(), True)
                         ])


def currentproceduralterminologymodifiercode(*args, **kwargs):
    admission = CurrentProceduralTerminologyModifierCode()
    admission.initialize_table(**kwargs)
    return admission
