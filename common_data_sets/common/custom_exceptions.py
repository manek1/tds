from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping


class GlueTableNotFoundExceptions(Exception):
    """raised when table is not present"""

    def __init__(self, table):
        if isinstance(table, AbstractGlueTableMapping):
            self.table_name = table.table_name
        else:
            self.table_name = str(table)

        self.message = self.table_name + " not present/error in glue-catalog"
        super(GlueTableNotFoundExceptions, self).__init__(self.message)


class PreValidationError(Exception):
    """raised when source table validation fails"""

    def __init__(self, module_name):
        self.module_name = module_name
        self.message = "Pre-validation failed for " + self.module_name
        super(PreValidationError, self).__init__(self.message)


class InvalidTableSchemaError(Exception):
    def __init__(self, table_name, message=None):
        self.table_name = table_name
        if message is None:
            self.message = "Invalid schema for " + self.table_name
        else:
            self.message = message
        super(InvalidTableSchemaError, self).__init__(self.message)


class ColumnNotFoundError(Exception):
    def __init__(self, table_name, column):
        self.message = "Missing column " + str(column) + " in " + table_name
        super(ColumnNotFoundError, self).__init__(self.message)


class PostValidationError(Exception):
    """raised when target table validation fails"""

    def __init__(self, module_name):
        self.module_name = module_name
        self.message = "Post Load Validation failed for " + self.module_name
        super(PostValidationError, self).__init__(self.message)


class ThresholdValidationError(Exception):
    """raised when target table validation fails"""

    def __init__(self, module_name, key_name, key_value):
        self.module_name = str(module_name)
        self.key_value = str(key_value)
        self.key_name = str(key_name)
        self.message = "Threshold Validation failed for " + self.module_name + self.key_name + self.key_value
        super(ThresholdValidationError, self).__init__(self.message)


class InvalidConfigFileException(Exception):
    """Raised when wrong config is passed"""

    def __init__(self, config_file_name, custom_message=''):
        self.custom_message = custom_message
        self.config_file_name = config_file_name
        self.message = f"In valid configuration file {self.config_file_name} {self.custom_message}"
        super(InvalidConfigFileException, self).__init__(self.message)


class EmptyDataSetException(Exception):
    """Raised when wrong config is passed"""

    def __init__(self, dataset_name, custom_message=''):
        self.custom_message = custom_message
        self.dataset_name = dataset_name
        self.message = f"Data is not present file {self.dataset_name} {self.custom_message}"
        super(EmptyDataSetException, self).__init__(self.message)
