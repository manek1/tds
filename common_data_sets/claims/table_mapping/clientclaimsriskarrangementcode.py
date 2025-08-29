from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class ClientRiskArrangement(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientclaimsriskarrangementcode"
    schema = T.StructType([T.StructField("claimsriskarrangementcode", T.StringType(), True),
                           T.StructField("claimsriskarrangement", T.StringType(), True)]
                          )


def client_risk_arrange(*args, **kwargs):
    clntra = ClientRiskArrangement()
    clntra.initialize_table(**kwargs)
    return clntra
