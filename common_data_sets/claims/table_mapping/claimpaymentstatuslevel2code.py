from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class ClaimPaymentStatusLevel2code(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimpaymentstatuslevel2code"
    schema = T.StructType([T.StructField("claimpaymentstatuslevel2", T.StringType(), True),
                           T.StructField("claimpaymentstatuslevel2code", T.StringType(), True)]
                          )


def claim_payment_status_level2code(*args, **kwargs):
    clm_ext_cst = ClaimPaymentStatusLevel2code()
    clm_ext_cst.initialize_table(**kwargs)
    return clm_ext_cst
