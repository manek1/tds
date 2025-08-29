from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, DecimalType
from common_data_sets.common.configs import config


class Nationality(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "nationality"
    schema = StructType([StructField("customerid", StringType(), True),
                         StructField('nationalitycode', StringType(), True),
                         StructField('preferrednationalityindicator', BooleanType(), True),
                         StructField('nationalitypriority', DecimalType(10, 0), True),
                         StructField('sourcesystemid', StringType(), True)])


def nationality(*args, **kwargs):
    nation = Nationality()
    nation.initialize_table(**kwargs)
    return nation
