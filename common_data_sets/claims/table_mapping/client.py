from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class Client(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "client"
    schema = T.StructType([T.StructField("clientid", T.StringType(), True),
                           T.StructField("clientlevel", T.IntegerType(), True),
                           T.StructField('parentclientid', T.StringType(), True),
                           T.StructField("clientname", T.StringType(), True),
                           T.StructField("clientbusinesssegmentcode", T.StringType(), True),
                           T.StructField("claimsriskarrangementcode", T.StringType(), True),
                           T.StructField("internalindustrysectorcode", T.StringType(), True),
                           T.StructField('sourcesystemid', T.StringType(), True)])


def client(*args, **kwargs):
    clnt = Client()
    clnt.initialize_table(**kwargs)
    return clnt
