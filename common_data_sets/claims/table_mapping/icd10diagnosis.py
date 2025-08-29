from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType, IntegerType, BooleanType
from common_data_sets.common.configs import config


class Icd10Diagnosis(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "icd10diagnosis"

    schema = StructType(
        [StructField('internationalstatisticalclassificationofdiseasesicd10code', StringType(), True),
         StructField('internationalstatisticalclassificationofdiseasesicd10', StringType(), True)
         ]
    )


def icd10diagnosis(*args, **kwargs):
    icd10 = Icd10Diagnosis()
    icd10.initialize_table(**kwargs)
    return icd10
