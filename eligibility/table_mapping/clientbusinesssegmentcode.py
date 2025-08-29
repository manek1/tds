from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class ClientBusinessSegmentCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientbusinesssegmentcode"

    @property
    def schema(self):
        schema = StructType([StructField("clientbusinesssegment", StringType(), True),
                             StructField("clientbusinesssegmentcode", StringType(), True)])
        return schema


def client_business_segment_code_info(*args, **kwargs):
    client_business_segment_code_table = ClientBusinessSegmentCode()
    client_business_segment_code_table.initialize_table(**kwargs)
    return client_business_segment_code_table
