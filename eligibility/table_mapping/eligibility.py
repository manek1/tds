from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType, DecimalType, DateType
from common_data_sets.common.configs import config


class Eligibility(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "eligibility"

    @property
    def schema(self):
        schema = StructType(
            [StructField('planid', StringType(), True),
             StructField('policyid', StringType(), True),
             StructField('etlchecksum', StringType(), True),
             StructField('relationshiptypecode', StringType(), True),
             StructField('familyunittypecode', StringType(), True),
             StructField('memberid', StringType(), True),
             StructField('premiumloadingpercentage', DecimalType(5, 2), True),
             StructField('eligibilityfromdate', TimestampType(), True),
             StructField('eligibilitytodate', TimestampType(), True),
             StructField('eligibilityterminationdate', TimestampType(), True),
             StructField('areaofcovercode', StringType(), True),
             StructField('sourcesystemid', StringType(), True)
             ]
        )

        return schema


def eligibility(*args, **kwargs):
    elg = Eligibility()
    elg.initialize_table(**kwargs)
    return elg
