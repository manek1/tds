from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class InsTelling(AbstractGlueTableMapping):
    schema_name = config.ARCHMCC_SCHEMA
    table_name = "vw_aws_c_instelling"
    schema = StructType([StructField("businessline", StringType(), True),
                         StructField("codeinstelling", StringType(), True)
                         ])


def vw_aws_c_instelling(*args, **kwargs):
    instelling = InsTelling()
    instelling.initialize_table(**kwargs)
    return instelling

