from abc import ABC
from pyspark.sql import types as T
from .abstract_view_mapping import AbstractViewMapping
from pyspark.sql import DataFrame


class AbstractGlueViewMapping(AbstractViewMapping, ABC):
    def __init__(self):
        self._key_column = ""
        self._s3_path = ""
        self._distinct_flag = False
        self._partition_predicate = ""

    @property
    def partition_predicate(self) -> str:
        return self._partition_predicate

    @partition_predicate.setter
    def partition_predicate(self, value: str = ""):
        part_predict = value.split(",")
        if len(part_predict) > 1:
            value = value.replace(",", " and ")
        self._partition_predicate = value

    @property
    def distinct_flag(self):
        return self._distinct_flag

    @distinct_flag.setter
    def distinct_flag(self, value):
        self._distinct_flag = value

    @staticmethod
    def struct_to_dict(schema: T.StructType) -> dict:
        dtype_mapping = dict()
        for field in schema:
            dtype_mapping[field.name] = field.dataType
        return dtype_mapping

    @property
    def s3_output_path(self) -> str:
        return self._s3_path

    @s3_output_path.setter
    def s3_output_path(self, value: str = ""):
        self._s3_path = value

    @property
    def columns(self) -> list:
        """
        Return a list (table_name, colum_name).
        """
        if self.schema:
            return self.schema.fieldNames()
        else:
            return ["*"]

    def validate_schema(self, df: DataFrame):
        if self.schema != df.schema:
            return False
        return True

    def initialize_view(self, **kwargs):
        if kwargs:
            self.s3_output_path = kwargs.get("s3_path", self._s3_path)

    @property
    def key_columns(self):
        return self._key_column

    @key_columns.setter
    def key_columns(self, value: list):
        self._key_column = value
