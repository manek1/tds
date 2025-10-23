import json
from abc import ABC, abstractmethod
from pyspark.sql import types as T
from .abstract_table_mapping import AbstractTableMapping
from pyspark.sql import DataFrame


class AbstractGlueTableMapping(AbstractTableMapping, ABC):
    def __init__(self):
        self._partition_predicate = ""
        self._partition_columns = list()
        self._key_column = ""
        self._table_list = list()
        self._distinct_flag = False

    @property
    def distinct_flag(self):
        return self._distinct_flag

    @distinct_flag.setter
    def distinct_flag(self, value):
        self._distinct_flag = value

    @property
    def partition_columns(self):
        return self._partition_columns

    @partition_columns.setter
    def partition_columns(self, cols: str):
        if isinstance(cols, str):
            self._partition_columns = [col.strip() for col in cols.split(",")]
        else:
            self._partition_columns = [""]

    @staticmethod
    def struct_to_dict(schema: T.StructType) -> dict:
        dtype_mapping = dict()
        for field in schema:
            dtype_mapping[field.name] = field.dataType
        return dtype_mapping

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

    @property
    def partition_predicate(self) -> str:
        return self._partition_predicate

    @partition_predicate.setter
    def partition_predicate(self, value: str = ""):
        part_predict = value.split(",")
        if len(part_predict) > 1:
            value = value.replace(",", " and ")
        self._partition_predicate = value

    def initialize_table(self, **kwargs):
        if kwargs:
            self.partition_predicate = kwargs.get("partition_predicate", self._partition_predicate)
            self.partition_columns = kwargs.get("partition_columns", self._partition_columns)

    @property
    def key_columns(self):
        return self._key_column

    @key_columns.setter
    def key_columns(self, value: list):
        self._key_column = value

    @property
    def table_list(self):
        return self._table_list

    @table_list.setter
    def table_list(self, value: list):
        self._table_list = value
