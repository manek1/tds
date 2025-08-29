from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType, IntegerType, BooleanType
from common_data_sets.common.configs import config


class DiagnosisClaim(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "diagnosisclaim"

    @property
    def schema(self):
        schema = StructType(
            [StructField('claimlineid', StringType(), True),
             StructField('internaldiagnosiscode', StringType(), True),
             StructField('internationalstatisticalclassificationofdiseasesicd10code', StringType(), True),
             StructField('internationalstatisticalclassificationofdiseasesicd9code', StringType(), True),
             StructField('primarydiagnosisindicator', BooleanType(), True),
             StructField('sourcesystemid', StringType(), True)
             ]
        )

        return schema


def diagnosis_claim_info(*args, **kwargs):
    diagnosis_claim = DiagnosisClaim()
    diagnosis_claim.initialize_table(**kwargs)
    return diagnosis_claim
