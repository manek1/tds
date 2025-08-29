from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class ClientClaimsRiskArrangementCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientclaimsriskarrangementcode"

    @property
    def schema(self):
        schema = StructType([StructField("claimsriskarrangement", StringType(), True),
                             StructField("claimsriskarrangementcode", StringType(), True)])
        return schema


def clientclaimsriskarrangementcode_info(*args, **kwargs):
    clientclaimsriskarrangementcode_table = ClientClaimsRiskArrangementCode()
    clientclaimsriskarrangementcode_table.initialize_table(**kwargs)
    return clientclaimsriskarrangementcode_table
