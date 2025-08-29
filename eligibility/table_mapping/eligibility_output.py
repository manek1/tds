from common_data_sets.common.abstract_glue_table_mapping import AbstractGlueTableMapping
from pyspark.sql.types import StringType, StructField, StructType, TimestampType, DateType,DecimalType, BooleanType, IntegerType
from common_data_sets.common.configs import config


class EligibilityOutPut(AbstractGlueTableMapping):
    schema_name = config.ELIGIBILITY_SCHEMA
    table_name = "eligibility"
    key_columns = ["sourcesystemid"]
    date_column = "record_from_date"
    metrics = []

    threshold = [{"prior_years": 1, "current_year_threshold": 5, "prior_year_threshold": 1, "prior_months": 1,
                  "current_month_threshold": 5, "prior_month_threshold": 2}]

    @property
    def schema(self):
        schema = StructType([
            StructField('policyid', StringType(), True),
            StructField("policystatuscode", StringType(), True),
            StructField('policyname', StringType(), True),
            StructField('legalentityid', StringType(), True),
            StructField('legalentityname', StringType(), True),
            StructField('lineofbusinessid', StringType(), True),
            StructField('lineofbusinessname', StringType(), True),
            StructField('edw_businessline', StringType(), True),
            StructField('edw_businesslinedesc', StringType(), True),
            StructField('edw_producttreekey', StringType(), True),
            StructField('edw_producttreedesc', StringType(), True),
            StructField('edw_businessbook', StringType(), True),
            StructField('edw_businessbookdesc', StringType(), True),
            StructField('segment', StringType(), True),
            StructField('market', StringType(), True),
            StructField('parentclientid', StringType(), True),
            StructField('clientid', StringType(), True),
            StructField('ancestorid', StringType(), True),
            StructField('clientlevel', IntegerType(), True),
            StructField('clientname', StringType(), True),
            StructField('claimsriskarrangement', StringType(), True),
            StructField("policystartdate", DateType(), True),
            StructField("policyenddate", DateType(), True),
            StructField('clientcontractstartdate', DateType(), True),
            StructField('clientcontractenddate', DateType(), True),
            StructField('clientinceptiondate', DateType(), True),
            StructField('clientterminationdate', DateType(), True),
            StructField('planid', StringType(), True),
            StructField('planname', StringType(), True),
            StructField('productid', StringType(), True),
            StructField('productname', StringType(), True),
            StructField('packageid', StringType(), True),
            StructField('packagename', StringType(), True),
            StructField('areaofcovercode', StringType(), True),
            StructField('areaofcover', StringType(), True),
            StructField('clientbusinesssegment', StringType(), True),
            StructField('pricingmethod', StringType(), True),
            StructField('premiumfundingarrangement', StringType(), True),
            StructField('customerid', StringType(), True),
            StructField('memberid', StringType(), True),
            StructField('familyunitid', StringType(), True),
            StructField('familyunittype', StringType(), True),
            StructField('lineofcovercode', StringType(), True),
            StructField('lineofcover', StringType(), True),
            StructField('dateofbirth', DateType(), True),
            StructField('tds_currentage', IntegerType(), True),
            StructField('nationalitycode', StringType(), True),
            StructField('nationality', StringType(), True),
            StructField('gendercode', StringType(), True),
            StructField('countryofresidencecode', StringType(), True),
            StructField('countryofresidence', StringType(), True),
            StructField('countryofassignmentcode', StringType(), True),
            StructField('countryofassignment', StringType(), True),
            StructField('townorcityofassignment', StringType(), True),
            StructField('homecountry', StringType(), True),
            StructField('relationshiptypecode', StringType(), True),
            StructField('eligibilityfromdate', DateType(), True),
            StructField('eligibilitytodate', DateType(), True),
            StructField('eligibilityterminationdate', DateType(), True),
            StructField('leavedate', DateType(), True),
            StructField('memberterminationdate', DateType(), True),
            StructField('memberadditiondate', DateType(), True),
            StructField('policydetend', DateType(), True),
            StructField('clientstaffcategoryid', StringType(), True),
            StructField('clientstaffcategoryname', StringType(), True),
            StructField('internalindustrysectorcode', StringType(), True),
            StructField('internalindustrysector', StringType(), True),
            StructField('medicalunderwritingcustomertypecode', StringType(), True),
            StructField('record_from_date', DateType(), True),
            StructField('record_to_date', DateType(), True),
            StructField('sourcesystemname', StringType(), True),
            StructField('sourcesystemid', StringType(), True),
            StructField('sysbatchdttm', TimestampType(), True)
                             ]
                            )
        return schema


def eligibility_output(*args, **kwargs):
    elg_out = EligibilityOutPut()
    elg_out.initialize_table(**kwargs)
    return elg_out
