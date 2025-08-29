from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class Banking(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "banking"

    @property
    def schema(self):
        schema = StructType([StructField('bankaccountinternalid', StringType(), True),
                             StructField('bankaccountnumber', StringType(), True),
                             StructField('ibannumber', StringType(), True),
                             StructField('bankcountrycode', StringType(), True),
                             StructField('sourcesystemid', StringType(), True)
                             ])

        return schema


def banking(*args, **kwargs):
    exr_obj = Banking()
    exr_obj.initialize_table(**kwargs)
    return exr_obj
