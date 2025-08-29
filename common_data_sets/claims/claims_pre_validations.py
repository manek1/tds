from .table_mapping import *
from common_data_sets.common.abstract_data_quality_checks import AbstractDataQualityChecks
from common_data_sets.common.custom_exceptions import PreValidationError, InvalidTableSchemaError, ColumnNotFoundError


class ClaimsPreValidations(AbstractDataQualityChecks):
    def __init__(self):
        super().__init__(module_name="claims")
        self.table_object_list = [amountcopayment(), invoicerecovery(), claimrecovery(), amountdenied(),
                                  amountdiscount(), amounttax(), claiminvoice(), client(), policy(), claimline(),
                                  claimpayment(), diagnosis_claim_info(), client_risk_arrange(), claimsubmission(),
                                  countryofresidence(), customer(), family_unit(), invoicepayment(),
                                  legalentity(), member(), provider(), family_unit_type(), source_system_info(),
                                  party(), partyroletype(), product(), plan(), package(), lineofbusiness(),
                                  rdr_exchangerate(), detail_transaction(), product_tree(), claimline_service_code(),
                                  vw_aws_c_koers(), claiminvoicenetworkusagecode(), country(), hospitalrevenuecode(),
                                  proceduretypecode(), providergroupcode(), currencycode(), providertypecode(),
                                  claimsubmissionclaimchannelcode(), claimlineclaimtypecode(), lineofcovercode(),
                                  reason(), deniedreasoncode(), paymentmethodcode(), nationality(), claimsettlement(),
                                  clientbusinesssegmentcode(), claimexternalinvoice(), product_id_map(),
                                  vw_aws_c_instelling(), banking(), clientinternalindustrysectorcode(),
                                  claim_invoice_status_level1code(), claim_invoice_status_level2code(), icd9diagnosis(),
                                  claim_invoice_status_level3code(), claim_line_status_level1code(), icd10diagnosis(),
                                  claim_line_status_level2code(), claim_line_status_level3code(), internaldiagnosis(),
                                  claimlineadmissiontypecode(), currentproceduralterminologymodifiercode(),
                                  currentproceduralterminologycode(), reasonableandcustomaryreductionreasoncode(),
                                  reasonreimbursementlimitreductionreasoncode(), reasonnotcovereduscode()
                                  ]

    def validate(self):
        for table in self.table_object_list:
            try:
                self.check_table_columns(table, force=True)
                self.check_table_schema(table, force=True)
            except (InvalidTableSchemaError, ColumnNotFoundError):
                raise PreValidationError(self._module_name)
