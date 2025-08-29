from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class PartyAddress(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "partyaddress"
    distinct_flag = True
    @property
    def schema(self):
        schema = StructType([StructField("partyid", StringType(), True),
                             StructField("townorcity", StringType(), True),
                             StructField("addressstartdate", TimestampType(), True),
                             StructField("addressenddate", TimestampType(), True),
                             StructField('sourcesystemid', StringType(), True)])
        return schema


def party_address_info(*args, **kwargs):
    party_address = PartyAddress()
    party_address.initialize_table(**kwargs)
    return party_address
