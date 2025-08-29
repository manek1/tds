from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ClaimSettlement(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimsettlement"

    @property
    def schema(self):
        schema = StructType([StructField('claimsettlementid', StringType(), True),
                             StructField('claimsettlementdate', TimestampType(), True),
                             StructField('sourcesystemid', StringType(), True),
                             ])

        return schema


def claimsettlement(*args, **kwargs):
    clm = ClaimSettlement()
    clm.initialize_table(**kwargs)
    return clm
