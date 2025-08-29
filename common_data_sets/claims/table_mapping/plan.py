from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql.types import *


class Plan(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "plan"
    schema = StructType([StructField('packageid', StringType(), True),
                         StructField("planid", StringType(), True),
                         StructField("lineofcovercode", StringType(), True),
                         StructField('sourcesystemid', StringType(), True),
                         StructField('planname', StringType(), True)
                         ])


def plan(*args, **kwargs):
    plan_info = Plan()
    plan_info.initialize_table(**kwargs)
    return plan_info
