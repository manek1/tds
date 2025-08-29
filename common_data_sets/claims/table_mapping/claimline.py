from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, DecimalType, TimestampType, IntegerType
from common_data_sets.common.configs import config


class ClaimLine(AbstractGlueTableMapping):
    schema_name = config.IDS_SCHEMA
    table_name = "claimline"

    @property
    def schema(self):
        schema = StructType(
            [StructField('claimlineid', StringType(), True), StructField('claiminvoiceid', StringType(), True),
             StructField('policyid', StringType(), True), StructField('renderingproviderid', StringType(), True),
             StructField('servicetodate', TimestampType(), True),
             StructField('sourcesystemid', StringType(), True),
             StructField('paymentid', StringType(), True),
             StructField('planid', StringType(), True),
             StructField('financialbooktypecode', StringType(), True),
             StructField('servicefromdate', TimestampType(), True),
             StructField('internalclaimstatuscode', StringType(), True),
             StructField('outofpocketbreachedamount', DecimalType(17, 2), True),
             StructField('externalclaimstatuscode', StringType(), True),
             StructField('claimlinestatuslevel1code', StringType(), True),
             StructField('claimlinestatuslevel2code', StringType(), True),
             StructField('claimlinestatuslevel3code', StringType(), True),
             # StructField('asofeeamount', DecimalType(17, 2), True),
             # StructField('costplusfeeamount', DecimalType(17, 2), True),
             StructField('codeondentalproceduresandnomenclaturecode', StringType(), True),
             StructField('servicedate', TimestampType(), True),
             StructField('incurredcurrencybilledamount', DecimalType(17, 2), True),
             StructField('unitquantitycount', IntegerType(), True),
             StructField('productcurrencypaymentamount', DecimalType(17, 2), True),
             StructField('paymentcurrencypaymentamount', DecimalType(17, 2), True),
             StructField('benefitid', StringType(), True), StructField('proceduretypecode', StringType(), True),
             StructField('internalprocedurecode', StringType(), True),
             StructField('bodypartidentifiercode', StringType(), True),
             StructField('allowedamount', DecimalType(17, 2), True),
             StructField('savingclientfeeamount', DecimalType(17, 2), True),
             StructField('reimbursementlimitreductionamount', DecimalType(17, 2), True),
             StructField('placeofservicecode', StringType(), True),
             StructField('otherinsureramount', DecimalType(17, 2), True),
             StructField('notpayableamount', DecimalType(17, 2), True),
             StructField('noncompliancepenaltyamount', DecimalType(17, 2), True),
             StructField('networkfeeamount', DecimalType(17, 2), True),
             StructField('networkclientfeeamount', DecimalType(17, 2), True),
             StructField('healthcarecommonprocedurecodingsystemcode', StringType(), True),
             StructField('servicecode', StringType(), True),
             StructField('hospitalrevenuecode', StringType(), True),
             StructField('customershareamount', DecimalType(17, 2), True),
             StructField('costplusamount', DecimalType(17, 2), True),
             StructField('costplusfeeamount', DecimalType(17, 2), True),
             StructField('deductibleamount', DecimalType(17, 2), True),
             StructField('currentproceduralterminologycode', StringType(), True),
             StructField('legalentityid', StringType(), True),
             StructField('admissiontypecode', StringType(), True),
             StructField('claimtypecode', StringType(), True),
             StructField('financetransactionid', StringType(), True),
             StructField('currentproceduralterminologymodifiercode', StringType(), True),
             StructField("dischargedate", TimestampType(), True),
             StructField("admissiondate", TimestampType(), True),
             StructField('productcurrencybilledamount', DecimalType(17, 2), True)
             ]
        )

        return schema


def claimline(*args, **kwargs):
    clm_line = ClaimLine()
    clm_line.initialize_table(**kwargs)
    return clm_line
