from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class PartyRoleType(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "partyroletype"

    @property
    def schema(self):
        schema = StructType([StructField('partyroletypecode', StringType(), True),
                             StructField('partyroletype', StringType(), True)
                             ])

        return schema


def partyroletype(*args, **kwargs):
    partyrole = PartyRoleType()
    partyrole.initialize_table(**kwargs)
    return partyrole
