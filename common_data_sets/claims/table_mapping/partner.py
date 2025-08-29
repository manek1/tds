from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, BooleanType
from common_data_sets.common.configs import config


class Partner(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "partner"

    @property
    def schema(self):
        schema = StructType([StructField('partnerid', StringType(), True),
                             StructField('partnername', StringType(), True),
                             StructField('cignalinksindicator', BooleanType(), True),
                             StructField('masterpartnerid', StringType(), True),
                             StructField('masterpartnername', StringType(), True),
                             StructField('sourcesystemid', StringType(), True)
                             ])

        return schema


def partner(*args, **kwargs):
    partner_info = Partner()
    partner_info.initialize_table(**kwargs)
    return partner_info
