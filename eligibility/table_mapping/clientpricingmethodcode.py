from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class ClientPricingMethodCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientpricingmethodcode"

    @property
    def schema(self):
        schema = StructType([StructField("pricingmethod", StringType(), True),
                             StructField("pricingmethodcode", StringType(), True)])
        return schema


def client_pricing_method_code_info(*args, **kwargs):
    client_pricing_method_code_table = ClientPricingMethodCode()
    client_pricing_method_code_table.initialize_table(**kwargs)
    return client_pricing_method_code_table
