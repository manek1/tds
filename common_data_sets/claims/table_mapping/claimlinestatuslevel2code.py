from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class ClaimLineStatusLevel2code(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimlinestatuslevel2code"
    schema = T.StructType([T.StructField("claimlinestatuslevel2", T.StringType(), True),
                           T.StructField("claimlinestatuslevel2code", T.StringType(), True)]
                          )


def claim_line_status_level2code(*args, **kwargs):
    clm_ext_cst = ClaimLineStatusLevel2code()
    clm_ext_cst.initialize_table(**kwargs)
    return clm_ext_cst
