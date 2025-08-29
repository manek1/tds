from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class ClientBusinessSegmentCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientbusinesssegmentcode"
    schema = T.StructType([T.StructField("clientbusinesssegment", T.StringType(), True),
                           T.StructField("clientbusinesssegmentcode", T.StringType(), True)
                           ])


def clientbusinesssegmentcode(*args, **kwargs):
    clnt = ClientBusinessSegmentCode()
    clnt.initialize_table(**kwargs)
    return clnt
