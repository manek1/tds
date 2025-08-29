from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class Koers(AbstractGlueTableMapping):
    schema_name = config.ARCHMCC_SCHEMA
    table_name = "vw_aws_c_koers"
    schema = StructType([StructField("swiftmuntnaar", StringType(), True),
                         StructField("swiftmuntvan", StringType(), True),
                         StructField("codekoers", StringType(), True),
                         StructField("datumkoers", TimestampType(), True),
                         StructField("koers", DecimalType(13, 6), True)
                         ])


def vw_aws_c_koers(*args, **kwargs):
    koers = Koers()
    koers.initialize_table(**kwargs)
    return koers
