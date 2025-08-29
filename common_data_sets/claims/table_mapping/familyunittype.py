from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType
from common_data_sets.common.configs import config


class FamilyUnitType(AbstractGlueTableMapping):
    table_name = "familyunittypecode"
    schema_name = config.IDS_SCHEMA
    schema = StructType([StructField('familyunittype', StringType(), True),
                         StructField('familyunittypecode', StringType(), True)])


def family_unit_type(*args, **kwargs):
    family_unit_type_info = FamilyUnitType()
    family_unit_type_info.initialize_table(**kwargs)
    return family_unit_type_info
