from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from common_data_sets.common.configs import config


class GroupCoverage(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "groupcoverage"
    schema = StructType([StructField('primarylegalentityid', StringType(), True),
                         StructField('medicalunderwritingcustomertypecode', StringType(), True),
                         # StructField('groupcoveragekey', StringType(), True),
                         StructField('areaofcovercode', StringType(), True),
                         StructField('relationshiptypecode', StringType(), True),
                         StructField('policyid', StringType(), True),
                         StructField('planid', StringType(), True),
                         StructField('packageid', StringType(), True),
                         StructField('clientstaffcategoryid', StringType(), True),
                         StructField('sourcesystemid', StringType(), True)]
                        )


def groupcoverage(*args, **kwargs):
    gc = GroupCoverage()
    gc.initialize_table(**kwargs)
    return gc
