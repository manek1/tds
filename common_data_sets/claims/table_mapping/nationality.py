from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql.types import *


class Nationality(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "nationality"
    schema = StructType([StructField('customerid', StringType(), True),
                         StructField("nationalitycode", StringType(), True),
                         StructField("nationalitypriority", DecimalType(10,0), True),
                         StructField('sourcesystemid', StringType(), True)
                         ])


def nationality(*args, **kwargs):
    nat = Nationality()
    nat.initialize_table(**kwargs)
    return nat
