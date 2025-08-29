from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType
from common_data_sets.common.configs import config


class FamilyUnit(AbstractGlueTableMapping):
    table_name = "familyunit"
    schema_name = config.IDS_SCHEMA
    schema = StructType([StructField('familyunitid', StringType(), True),
                         StructField('familyunittypecode', StringType(), True),
                         StructField('clientpersonnelreferenceid', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def family_unit(*args, **kwargs):
    family_unit_info = FamilyUnit()
    family_unit_info.initialize_table(**kwargs)
    return family_unit_info
