from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ClaimSubmission(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = 'claimsubmission'
    schema = StructType([StructField('claimsubmissionid', StringType(), True),
                         StructField('claimsubmissiondate', TimestampType(), True),
                         StructField('claimsettlementid', StringType(), True),
                         StructField('claimchannelcode', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)]
                        )


def claimsubmission(*args, **kwargs):
    cs = ClaimSubmission()
    cs.initialize_table(**kwargs)
    return cs
