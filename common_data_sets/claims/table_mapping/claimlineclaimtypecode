from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, BooleanType
from common_data_sets.common.configs import config


class ClaimlineClaimTypeCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimlineclaimtypecode"

    @property
    def schema(self):
        schema = StructType([StructField('claimtype', StringType(), True),
                             StructField('claimtypecode', StringType(), True)
                             ])

        return schema


def claimlineclaimtypecode(*args, **kwargs):
    claim_type_code = ClaimlineClaimTypeCode()
    claim_type_code.initialize_table(**kwargs)
    return claim_type_code
