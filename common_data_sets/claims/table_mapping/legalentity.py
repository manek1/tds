from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class LegalEntity(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "legalentity"
    schema = StructType([StructField('legalentityid', StringType(), True),
                         StructField('legalentityname', StringType(), True),
                         StructField('lineofbusinessid', StringType(), True),
                         StructField('sourcesystemid', StringType(), True),
                         StructField('financialbooktypecode', StringType(), True)
                         ])


def legalentity(*args, **kwargs):
    leg_enty = LegalEntity()
    leg_enty.initialize_table(**kwargs)
    return leg_enty
