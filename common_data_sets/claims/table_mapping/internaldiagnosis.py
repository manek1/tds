from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class InternalDiagnosis(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "internaldiagnosis"
    schema = StructType([
        StructField('internaldiagnosiscode', StringType(), True),
        StructField('internaldiagnosis', StringType(), True),
        StructField('sourcesystemid', StringType(), True)]
    )


def internaldiagnosis(*args, **kwargs):
    hos_cur_co = InternalDiagnosis()
    hos_cur_co.initialize_table(**kwargs)
    return hos_cur_co
