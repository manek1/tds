from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class Customer(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "customer"
    schema = StructType([StructField("customerid", StringType(), True),
                         StructField("dateofbirth", TimestampType(), True),
                         StructField("gendercode", StringType(), True),
                         StructField('sourcesystemid', StringType(), True)])


def customer(*args, **kwargs):
    cstmr = Customer()
    cstmr.initialize_table(**kwargs)
    return cstmr
