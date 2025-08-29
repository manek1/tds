from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class ClientPremiumFundingArrangementCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientpremiumfundingarrangementcode"

    @property
    def schema(self):
        schema = StructType([StructField("premiumfundingarrangement", StringType(), True),
                             StructField("premiumfundingarrangementcode", StringType(), True)])
        return schema


def client_premium_funding_arrangement_code_info(*args, **kwargs):
    client_premium_funding_arrangement_code_table = ClientPremiumFundingArrangementCode()
    client_premium_funding_arrangement_code_table.initialize_table(**kwargs)
    return client_premium_funding_arrangement_code_table
