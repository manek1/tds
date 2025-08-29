from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class ProductIdMapping(AbstractGlueTableMapping):
    schema_name = config.ANALYST_TEMP_DB_SCHEMA
    table_name = "tds_mapping_files_productid"
    schema = StructType([StructField("product_id", StringType(), True),
                         StructField("segment", StringType(), True),
                         StructField("market", StringType(), True)
                         ])


def product_id_map(*args, **kwargs):
    product_id_map = ProductIdMapping()
    product_id_map.initialize_table(**kwargs)
    return product_id_map

