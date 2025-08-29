from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType
from common_data_sets.common.configs import config


class EdwEligibility(AbstractGlueTableMapping):
    table_name = "eligibility"
    schema_name = config.RDR_SCHEMA
    distinct_flag = True
    schema = StructType([StructField('environment_key', StringType(), True),
                         StructField('subscriber_no', StringType(), True),
                         StructField('product_tree_key', StringType(), True),
                         StructField('person_no', StringType(), True)])


def edw_elig(*args, **kwargs):
    edw_elig_table = EdwEligibility()
    edw_elig_table.initialize_table(**kwargs)
    return edw_elig_table
