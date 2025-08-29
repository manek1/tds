from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType, BinaryType
from common_data_sets.common.configs import config


class ClaimRecovery(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimrecovery"

    @property
    def schema(self):
        schema = StructType([StructField('recoveredamount', DecimalType(17, 2), True),
                             StructField('recoveryid', StringType(), True),
                             StructField('sourcesystemid', StringType(), True)])

        return schema


def claimrecovery(*args, **kwargs):
    clm = ClaimRecovery()
    clm.initialize_table(**kwargs)
    return clm
