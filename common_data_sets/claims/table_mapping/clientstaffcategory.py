from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ClientStaffCategory(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "clientstaffcategory"
    schema = StructType([StructField("clientstaffcategoryname", StringType(), True),
                         StructField("clientstaffcategoryid", StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def clientstaffcategory(*args, **kwargs):
    staff_cat = ClientStaffCategory()
    staff_cat.initialize_table(**kwargs)
    return staff_cat
