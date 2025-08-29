from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType
from common_data_sets.common.configs import config


class Benefit(AbstractGlueTableMapping):
    table_name = "benefit"
    schema_name = config.IDS_SCHEMA
    schema = StructType([StructField('benefitname', StringType(), True),
                         StructField('benefitid', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def benefit(*args, **kwargs):
    benefit_info = Benefit()
    benefit_info.initialize_table(**kwargs)
    return benefit_info
