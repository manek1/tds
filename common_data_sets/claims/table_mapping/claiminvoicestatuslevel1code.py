from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class ClaimInvoiceStatusLevel1code(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claiminvoicestatuslevel1code"
    schema = T.StructType([T.StructField("claiminvoicestatuslevel1", T.StringType(), True),
                           T.StructField("claiminvoicestatuslevel1code", T.StringType(), True)]
                          )


def claim_invoice_status_level1code(*args, **kwargs):
    clm_ext_cst = ClaimInvoiceStatusLevel1code()
    clm_ext_cst.initialize_table(**kwargs)
    return clm_ext_cst
