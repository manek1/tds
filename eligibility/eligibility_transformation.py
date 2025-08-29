from common_data_sets.common.abstract_data_quality_checks import AbstractDataQualityChecks
from .table_mapping import *
from common_data_sets.common.custom_exceptions import PreValidationError, InvalidTableSchemaError, ColumnNotFoundError


class EligibilityPreValidation(AbstractDataQualityChecks):
    def __init__(self):
        super(EligibilityPreValidation, self).__init__(module_name="eligibility")
        self.table_object_list = [eligibility(), client(), clientcontract(), clientstaffcategory(),
                                  countryofresidence(), customer(), groupcoverage(), legalentity(), lineofbusiness(),
                                  member(),
                                  membercoverage(), policy(), source_system_info(), nationality(), familyunit(),
                                  familyunittypecode(), package(), product(), lineofcovercode(),
                                  clientclaimsriskarrangementcode_info(),
                                  client_pricing_method_code_info(), client_premium_funding_arrangement_code_info(),
                                  client_business_segment_code_info(), area_of_cover_info(),
                                  client_internal_industry_sector_code_info(),
                                  party_address_info(), country(), product_tree(), edw_elig(), product_id_map(),
                                  vw_aws_c_InsTelling(), policystatus()]

    def validate(self):
        for table in self.table_object_list:
            try:
                self.check_table_columns(table, force=True)
                self.check_table_schema(table, force=True)
            except (InvalidTableSchemaError, ColumnNotFoundError):
                raise PreValidationError(self._module_name)
