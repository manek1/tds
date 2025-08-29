from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class HospitalRevenueCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "hospitalrevenuecode"
    schema = StructType([
        StructField('hospitalrevenue', StringType(), True),
        StructField('hospitalrevenuecode', StringType(), True)]
    )


def hospitalrevenuecode(*args, **kwargs):
    hos_cur_co = HospitalRevenueCode()
    hos_cur_co.initialize_table(**kwargs)
    return hos_cur_co
