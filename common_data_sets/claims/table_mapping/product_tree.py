from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType
from common_data_sets.common.configs import config


class ProductTree(AbstractGlueTableMapping):
    table_name = "product_tree"
    schema_name = config.RDR_SCHEMA
    schema = StructType([StructField('environment_key', StringType(), True),
                         StructField('business_line', StringType(), True),
                         StructField('business_line_desc', StringType(), True),
                         StructField('product_tree_key', StringType(), True),
                         StructField('product_tree_desc', StringType(), True),
                         StructField('business_book', StringType(), True),
                         StructField('business_book_desc', StringType(), True)])


def product_tree(*args, **kwargs):
    pt_table = ProductTree()
    pt_table.initialize_table(**kwargs)
    return pt_table
