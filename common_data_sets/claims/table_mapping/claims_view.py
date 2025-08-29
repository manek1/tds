from common_data_sets.common.abstract_glue_view_mapping import AbstractGlueViewMapping
from common_data_sets.common.configs import config
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType


class ClaimsView(AbstractGlueViewMapping):
    view_name = "claims_view"
    distinct_flag = True
    table_list = ["claims_view_aws_year0", "claims_view_aws_year1", "claims_view_aws_year10", "claims_view_aws_year2",
                  "claims_view_aws_year3", "claims_view_aws_year4", "claims_view_aws_year5", "claims_view_aws_year6",
                  "claims_view_aws_year7", "claims_view_aws_year8", "claims_view_aws_year9"]

    schema_name = config.RDR_SCHEMA

    schema = StructType([StructField('case_type_key', StringType(), True),
                         StructField('form_number', DecimalType(17, 2), True),
                         StructField('procedure_codes_key', DecimalType(17, 2), True),
                         StructField('claim_detail_entered_date', TimestampType(), True),
                         StructField('environment_key', StringType(), True),
                         StructField('reversal_flag', StringType(), True)])


def claims_view(*args, **kwargs):
    clm_v = ClaimsView()
    clm_v.initialize_view(**kwargs)
    return clm_v
