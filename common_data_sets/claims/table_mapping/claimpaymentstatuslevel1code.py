from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class ClaimPaymentStatusLevel1code(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimpaymentstatuslevel1code"
    schema = T.StructType([T.StructField("claimpaymentstatuslevel1", T.StringType(), True),
                           T.StructField("claimpaymentstatuslevel1code", T.StringType(), True)]
                          )


def claim_payment_status_level1code(*args, **kwargs):
    clm_ext_cst = ClaimPaymentStatusLevel1code()
    clm_ext_cst.initialize_table(**kwargs)
    return clm_ext_cst
