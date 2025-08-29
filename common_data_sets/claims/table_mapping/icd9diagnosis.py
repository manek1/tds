from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType, IntegerType, BooleanType
from common_data_sets.common.configs import config


class Icd9Diagnosis(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "icd9diagnosis"

    schema = StructType(
        [StructField('internationalstatisticalclassificationofdiseasesicd9code', StringType(), True),
         StructField('internationalstatisticalclassificationofdiseasesicd9', StringType(), True)
         ]
    )


def icd9diagnosis(*args, **kwargs):
    icd9 = Icd9Diagnosis()
    icd9.initialize_table(**kwargs)
    return icd9
