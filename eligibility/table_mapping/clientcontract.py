from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class ClientContract(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientcontract"

    @property
    def schema(self):
        schema = StructType([StructField("clientcontractstartdate", TimestampType(), True),
                             StructField("clientcontractenddate", TimestampType(), True),
                             StructField("clientcontractid", StringType(), True),
                             StructField('sourcesystemid', StringType(), True)])
        return schema


def clientcontract(*args, **kwargs):
    client_contract = ClientContract()
    client_contract.initialize_table(**kwargs)
    return client_contract
