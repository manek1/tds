from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType
from common_data_sets.common.configs import config


class ReasonReimbursementLimitreductionReasoncode(AbstractGlueTableMapping):
    table_name = "reasonreimbursementlimitreductionreasoncode"
    schema_name = config.IDS_SCHEMA
    distinct_flag = True
    schema = StructType([StructField('reimbursementlimitreductionreasoncode', StringType(), True),
                         StructField('reimbursementlimitreductionreason', StringType(), True)
                         ])


def reasonreimbursementlimitreductionreasoncode(*args, **kwargs):
    rsn = ReasonReimbursementLimitreductionReasoncode()
    rsn.initialize_table(**kwargs)
    return rsn
