from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class CountryOfResidence(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "countryofresidence"
    distinct_flag = True
    schema = StructType([StructField('customerid', StringType(), True),
                         StructField('countryofresidenceenddate', TimestampType(), True),
                         StructField('countryofresidencestartdate', TimestampType(), True),
                         StructField('countryofresidencecode', StringType(), True),
                         StructField('countryofresidencetypecode', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)]
                        )


def countryofresidence(*args, **kwargs):
    cor = CountryOfResidence()
    cor.initialize_table(**kwargs)
    return cor
