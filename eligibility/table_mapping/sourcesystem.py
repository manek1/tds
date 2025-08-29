from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType
from common_data_sets.common.configs import config


class SourceSystem(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "sourcesystem"
    schema = StructType([StructField("name", StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def source_system_info(*args, **kwargs):
    source_system = SourceSystem()
    source_system.initialize_table(**kwargs)
    return source_system
