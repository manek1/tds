from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class Client(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "client"

    @property
    def schema(self):
        schema = StructType([StructField("clientid", StringType(), True),
                             StructField('parentclientid', StringType(), True),
                             StructField('clientlevel', IntegerType(), True),
                             StructField("clientname", StringType(), True),
                             StructField("clientcontractid", StringType(), True),
                             StructField("pricingmethodcode", StringType(), True),
                             StructField("clientinceptiondate", TimestampType(), True),
                             StructField("clientterminationdate", TimestampType(), True),
                             StructField("clientbusinesssegmentcode", StringType(), True),
                             StructField('premiumfundingarrangementcode', StringType(), True),
                             StructField("claimsriskarrangementcode", StringType(), True),
                             StructField("internalindustrysectorcode", StringType(), True),
                             StructField('sourcesystemid', StringType(), True)])
        return schema


def client(*args, **kwargs):
    client_table = Client()
    client_table.initialize_table(**kwargs)
    return client_table
