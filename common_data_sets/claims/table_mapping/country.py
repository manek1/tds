from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class Country(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "country"
    distinct_flag = True
    schema = StructType([
        StructField('countrycode', StringType(), True),
        StructField('country', StringType(), True),
        StructField('sourcesystemid', StringType(), True)
    ])


def country(*args, **kwargs):
    cor = Country()
    cor.initialize_table(**kwargs)
    return cor
