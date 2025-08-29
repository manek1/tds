from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, TimestampType
from common_data_sets.common.configs import config


class Member(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "member"
    schema = StructType([StructField("memberid", StringType(), True),
                         StructField("customerid", StringType(), True),
                         StructField("familyunitid", StringType(), True),
                         StructField("clientstaffcategoryid", StringType(), True),
                         StructField('cignalinkscustomerindicator', BooleanType(), True),
                         StructField('memberadditiondate', TimestampType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def member(*args, **kwargs):
    mem = Member()
    mem.initialize_table(**kwargs)
    return mem
