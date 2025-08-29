from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, BooleanType
from common_data_sets.common.configs import config


class Plan(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "plan"
    schema = StructType([StructField("planid", StringType(), True),
                         StructField("planname", StringType(), True),
                         StructField("lineofcovercode", StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def elg_plan(*args, **kwargs):
    pln = Plan()
    pln.initialize_table(**kwargs)
    return pln
