from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class AreaOfCover(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "areaofcover"

    @property
    def schema(self):
        schema = StructType([StructField("areaofcover", StringType(), True),
                             StructField("areaofcovercode", StringType(), True)])
        return schema


def area_of_cover_info(*args, **kwargs):
    area_of_cover_table = AreaOfCover()
    area_of_cover_table.initialize_table(**kwargs)
    return area_of_cover_table
