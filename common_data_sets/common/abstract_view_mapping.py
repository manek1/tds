import json
from abc import ABC, abstractmethod
from pyspark.sql import types as T


class AbstractViewMapping(ABC):
    @property
    @abstractmethod
    def view_name(self) -> str:
        """Name of the table"""

    @property
    @abstractmethod
    def schema_name(self):
        pass

    @property
    @abstractmethod
    def key_columns(self):
        pass

    @property
    @abstractmethod
    def schema(self) -> T.StructType:
        """
        Store the schema of the table using pyspark's StructType class.
        :return: T.StructType representing the schema.
        """

    @property
    def __repr__(self) -> str:
        """
        The string representation of a table mapping is its full table name.
        :return: The full table name.
        """
        return "`" + self.schema_name + "`.`" + self.view_name + "`"

    @abstractmethod
    def initialize_view(self, **kwargs):
        pass
