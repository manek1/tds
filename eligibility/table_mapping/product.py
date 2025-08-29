from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, BooleanType
from common_data_sets.common.configs import config


class Product(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "product"
    schema = StructType([StructField("productid", StringType(), True),
                         StructField("productname", StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def product(*args, **kwargs):
    prd = Product()
    prd.initialize_table(**kwargs)
    return prd
