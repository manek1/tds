from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class ClaimSubmissionClaimChannelCode(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = 'claimsubmissionclaimchannelcode'
    schema = StructType([StructField('claimchannel', StringType(), True),
                         StructField('claimchannelcode', StringType(), True)]
                        )


def claimsubmissionclaimchannelcode(*args, **kwargs):
    cs = ClaimSubmissionClaimChannelCode()
    cs.initialize_table(**kwargs)
    return cs
