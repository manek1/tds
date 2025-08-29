from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql.types import *


class Package(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "package"
    schema = StructType([StructField('packageid', StringType(), True),
                         StructField("productid", StringType(), True),
                         StructField('packagename', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)
                         ])


def package(*args, **kwargs):
    pkg = Package()
    pkg.initialize_table(**kwargs)
    return pkg
