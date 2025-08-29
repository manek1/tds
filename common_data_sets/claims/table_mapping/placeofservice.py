from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from common_data_sets.common.configs import config
from pyspark.sql import types as T


class PlaceOfService(AbstractGlueTableMapping):
    table_name = "placeofservicecode"
    schema_name = config.IDS_SCHEMA

    schema = T.StructType([T.StructField("placeofservice", T.StringType(), True),
                           T.StructField("placeofservicecode", T.StringType(), True)])


def placeofservice(*args, **kwargs):
    place_of_service = PlaceOfService()
    place_of_service.initialize_table(**kwargs)
    return place_of_service
