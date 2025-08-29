from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class ClaimLineStatusLevel1code(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimlinestatuslevel1code"
    schema = T.StructType([T.StructField("claimlinestatuslevel1", T.StringType(), True),
                           T.StructField("claimlinestatuslevel1code", T.StringType(), True)]
                          )


def claim_line_status_level1code(*args, **kwargs):
    clm_ext_cst = ClaimLineStatusLevel1code()
    clm_ext_cst.initialize_table(**kwargs)
    return clm_ext_cst
