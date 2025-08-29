from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class Reason(AbstractGlueTableMapping):
    table_name = "reason"
    schema_name = config.IDS_SCHEMA
    distinct_flag = True
    schema = StructType([StructField('claimlineid', StringType(), True),
                         StructField('deniedreasoncode', StringType(), True),
                         StructField('deniedreasonuscode', StringType(), True),
                         StructField('reasonnotcovereduscode', StringType(), True),
                         StructField('reimbursementlimitreductionreasoncode', StringType(), True),
                         StructField('reasonableandcustomaryreductionreasoncode', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def reason(*args, **kwargs):
    rsn = Reason()
    rsn.initialize_table(**kwargs)
    return rsn
