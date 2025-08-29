from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType
from common_data_sets.common.configs import config


class FamilyUnit(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "familyunit"
    schema = StructType([StructField("familyunitid", StringType(), True),
                         StructField('sourcesystemid', StringType(), True),
                         StructField('countryofassignmentcode', StringType(), True)
                         ])


def familyunit(*args, **kwargs):
    fu = FamilyUnit()
    fu.initialize_table(**kwargs)
    return fu
