from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType
from common_data_sets.common.configs import config


class PaymentMethodCode(AbstractGlueTableMapping):
    table_name = "paymentmethodcode"
    schema_name = config.IDS_SCHEMA
    schema = StructType([StructField('paymentmethod', StringType(), True),
                         StructField('paymentmethodcode', StringType(), True)])


def paymentmethodcode(*args, **kwargs):
    rsn = PaymentMethodCode()
    rsn.initialize_table(**kwargs)
    return rsn
