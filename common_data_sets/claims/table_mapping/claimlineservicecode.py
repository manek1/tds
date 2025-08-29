from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, BooleanType
from common_data_sets.common.configs import config


class ClaimlineServiceCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimlineservicecode"

    @property
    def schema(self):
        schema = StructType([StructField('service', StringType(), True),
                             StructField('servicecode', StringType(), True),
                             StructField('sourcesystemid', StringType(), True)
                             ])
        return schema


def claimline_service_code(*args, **kwargs):
    claim_serv_code = ClaimlineServiceCode()
    claim_serv_code.initialize_table(**kwargs)
    return claim_serv_code
