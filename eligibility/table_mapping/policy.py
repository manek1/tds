from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class Policy(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "policy"

    @property
    def schema(self):
        schema = StructType([StructField("policyid", StringType(), True),
                             StructField("policyname", StringType(), True),
                             StructField("policystatuscode", StringType(), True),
                             StructField("policystartdate", TimestampType(), True),
                             StructField("policyenddate", TimestampType(), True),
                             StructField("premiumcurrencycode", StringType(), True),
                             StructField("clientid", StringType(), True),
                             StructField('sourcesystemid', StringType(), True)])
        return schema


def policy(*args, **kwargs):
    policy_table = Policy()
    policy_table.initialize_table(**kwargs)
    return policy_table
