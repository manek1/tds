from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType
from common_data_sets.common.configs import config


class LineOfBusiness(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "lineofbusiness"
    schema = StructType([StructField('lineofbusinessid', StringType(), True),
                         StructField('lineofbusinessname', StringType(), True)
                         ])


def lineofbusiness(*args, **kwargs):
    lob = LineOfBusiness()
    lob.initialize_table(**kwargs)
    return lob
