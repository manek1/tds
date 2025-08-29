from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class ClaimInvoiceStatusLevel3code(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claiminvoicestatuslevel3code"
    schema = T.StructType([T.StructField("claiminvoicestatuslevel3", T.StringType(), True),
                           T.StructField("claiminvoicestatuslevel3code", T.StringType(), True)]
                          )


def claim_invoice_status_level3code(*args, **kwargs):
    clm_ext_cst = ClaimInvoiceStatusLevel3code()
    clm_ext_cst.initialize_table(**kwargs)
    return clm_ext_cst
