from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType
from common_data_sets.common.configs import config


class ClaimLineAdmissionTypeCode(AbstractGlueTableMapping):
    table_name = "claimlineadmissiontypecode"
    schema_name = config.IDS_SCHEMA
    schema = StructType([StructField('admissiontype', StringType(), True),
                         StructField('admissiontypecode', StringType(), True)
                         ])


def claimlineadmissiontypecode(*args, **kwargs):
    admission = ClaimLineAdmissionTypeCode()
    admission.initialize_table(**kwargs)
    return admission
