from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType
from common_data_sets.common.configs import config


class MemberCoverage(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "membercoverage"
    schema = StructType([  # StructField('groupcoveragekey', StringType(), True),
        StructField('areaofcovercode', StringType(), True),
        StructField('relationshiptypecode', StringType(), True),
        StructField('policyid', StringType(), True),
        StructField('planid', StringType(), True),
        StructField('packageid', StringType(), True),
        StructField('clientstaffcategoryid', StringType(), True),
        StructField('memberid', StringType(), True),
        StructField('sourcesystemid', StringType(), True)]
    )


def membercoverage(*args, **kwargs):
    mem_cov = MemberCoverage()
    mem_cov.initialize_table(**kwargs)
    return mem_cov
