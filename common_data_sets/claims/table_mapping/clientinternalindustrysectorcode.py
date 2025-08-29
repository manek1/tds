from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class ClientInternalIndustrySectorCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientinternalindustrysectorcode"
    schema = T.StructType([T.StructField("internalindustrysector", T.StringType(), True),
                           T.StructField("internalindustrysectorcode", T.StringType(), True),
                           T.StructField('sourcesystemid', T.StringType(), True)])


def clientinternalindustrysectorcode(*args, **kwargs):
    clnt = ClientInternalIndustrySectorCode()
    clnt.initialize_table(**kwargs)
    return clnt
