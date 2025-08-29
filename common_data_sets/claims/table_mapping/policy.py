from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class Policy(AbstractGlueTableMapping):
    table_name = "policy"
    schema_name = config.IDS_SCHEMA

    schema = T.StructType([T.StructField("clientid", T.StringType(), True),
                           T.StructField("policyid", T.StringType(), True),
                           T.StructField("policyname", T.StringType(), True),
                           T.StructField("sourcesystemid", T.StringType(), True)
                           ])


def policy(*args, **kwargs):
    plcy = Policy()
    plcy.initialize_table(**kwargs)
    return plcy
