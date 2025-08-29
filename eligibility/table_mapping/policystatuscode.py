from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class PolicyStatusCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "policystatuscode"

    @property
    def schema(self):
        schema = StructType([StructField("policystatus", StringType(), True),
                             StructField("policystatuscode", StringType(), True)])
        return schema


def policystatus(*args, **kwargs):
    policy_status_table = PolicyStatusCode()
    policy_status_table.initialize_table(**kwargs)
    return policy_status_table
