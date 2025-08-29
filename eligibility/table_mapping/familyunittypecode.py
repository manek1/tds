from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType
from common_data_sets.common.configs import config


class FamilyUnitTypeCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "familyunittypecode"
    schema = StructType([StructField("familyunittype", StringType(), True),
                         StructField("familyunittypecode", StringType(), True)
                         ])


def familyunittypecode(*args, **kwargs):
    fu_tc = FamilyUnitTypeCode()
    fu_tc.initialize_table(**kwargs)
    return fu_tc
