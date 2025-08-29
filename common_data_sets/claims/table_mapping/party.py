from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class Party(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "party"

    @property
    def schema(self):
        schema = StructType([StructField('partyid', StringType(), True),
                             StructField('partyroletypecode', StringType(), True),
                             StructField('sourcesystemid', StringType(), True)
                             ])

        return schema


def party(*args, **kwargs):
    party_obj = Party()
    party_obj.initialize_table(**kwargs)
    return party_obj
