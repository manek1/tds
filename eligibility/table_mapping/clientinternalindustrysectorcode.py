from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class ClientInternalIndustrySectorCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientinternalindustrysectorcode"

    @property
    def schema(self):
        schema = StructType([StructField("internalindustrysector", StringType(), True),
                             StructField("internalindustrysectorcode", StringType(), True),
                             StructField('sourcesystemid', StringType(), True)])
        return schema


def client_internal_industry_sector_code_info(*args, **kwargs):
    client_internal_industry_sector_code_table = ClientInternalIndustrySectorCode()
    client_internal_industry_sector_code_table.initialize_table(**kwargs)
    return client_internal_industry_sector_code_table
