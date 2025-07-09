from common_data_sets.common.spark_common_functions import *
from pyspark.sql.functions import col, when, to_timestamp, rank, coalesce, explode, expr, sequence
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType, DateType, IntegerType
from pyspark.sql.functions import broadcast, array_contains, array, month, year, to_date, date_format, \
    current_timestamp, concat, last_day, unix_timestamp, trim, count, max, datediff, min, sum, abs, expr
from pyspark.sql import Window
from datetime import datetime
from pyspark.sql.functions import row_number, dayofmonth

DATE_FORMAT_WITH_TS = '%Y-%m-%d %H:%M:%S'


class EligibilityMonthlyTransformation:
    def __init__(self, spark: SparkSession, is_debug: bool = False):
        self.spark = spark
        self.is_debug = is_debug
        self.logger = logger

    def create_elig_monthly_table(self, elig_df):
        elig_df = elig_df.withColumn('lineofcover', when(col('sourcesystemid') == '1', 'Medical')
                                     .otherwise(col('lineofcover'))).withColumn('lineofcovercode',
                                                                                when(col('sourcesystemid') == '1',
                                                                                     'MED')
                                                                                .otherwise(col('lineofcovercode')))

        current_month = datetime.now().month
        current_year = datetime.now().year
        elig_month_df = elig_df.filter(trim(col("lineofcovercode")) != "").na.drop(subset=["lineofcovercode"]) \
            .withColumn("eligible_month_dt_key",
                        explode(sequence(expr("TO_DATE(CONCAT('01-01-',YEAR(eligibilityfromdate)),'dd-MM-yyyy')"),
                                         expr("TO_DATE(CONCAT('31-12-',YEAR(eligibilitytodate)),'dd-MM-yyyy')"),
                                         expr("INTERVAL 1 MONTH")
                                         ))).filter(
            (col("eligible_month_dt_key") <= f"{current_year}-{current_month:02d}"))

        elig_month_df = elig_month_df.withColumn('eligibilityfromdate_checker',
                                                 concat(lit('01'), date_format(col("eligibilityfromdate"), "MMyyyy"))) \
            .withColumn('eligibilitytodate_checker', last_day(col('eligibilitytodate'))) \
            .withColumn("eligibilityfromdate_checker", to_date('eligibilityfromdate_checker', 'ddMMyy')) \
            .withColumn("eligibilitytodate_checker", to_date('eligibilitytodate_checker', 'ddMMyy'))
        elig_month_df = elig_month_df.withColumn("is_eligible",
                                                 when(
                                                     (col("eligible_month_dt_key") >= col(
                                                         "eligibilityfromdate_checker"))
                                                     & (col("eligible_month_dt_key") <= col(
                                                         "eligibilitytodate_checker")),
                                                     lit(1)).otherwise(0)).drop("eligibilityfromdate_checker",
                                                                                "eligibilitytodate_checker")

        distinct_line_of_cover_codes = elig_month_df.select("lineofcovercode").distinct().rdd.flatMap(
            lambda x: x).collect()

        elig_month_df = elig_month_df.groupBy('memberid', 'eligibilityfromdate', 'eligibilitytodate',
                                              'eligible_month_dt_key').pivot('lineofcovercode').agg(
            {"is_eligible": "first"}).na.fill('0'). \
            withColumn("Eligible", when(
            array_contains(array([col(col_name).cast('int') for col_name in distinct_line_of_cover_codes]), 1),
            1).otherwise(0)).select("memberid", "eligible_month_dt_key", "eligibilityfromdate",
                                    "eligibilitytodate",
                                    "Eligible", *distinct_line_of_cover_codes)

        elig_window = Window.partitionBy("memberid", "eligibilityfromdate", "eligibilitytodate").orderBy(
            "sysbatchdttm")
        elig_final_df = elig_df.withColumn("row_number", row_number().over(elig_window)).where(
            col('row_number') == 1).withColumnRenamed("eligibilityfromdate", "eligibilityfromdate_source") \
            .withColumnRenamed("eligibilitytodate", "eligibilitytodate_source") \
            .withColumnRenamed("memberid", "memberid_source")

        elig_month_df = elig_month_df.select("memberid", "eligibilityfromdate", "eligibilitytodate",
                                             "eligible_month_dt_key", "Eligible",
                                             *distinct_line_of_cover_codes)
        elig_month_df = elig_month_df.withColumn("eligibilityfromdate",
                                                 date_format(col("eligibilityfromdate"), 'yyyy-MM-dd')) \
            .withColumn("eligibilitytodate",
                        date_format(col("eligibilitytodate"), 'yyyy-MM-dd'))

        elig_final_df = elig_final_df.withColumn("eligibilityfromdate_source",
                                                 date_format(col("eligibilityfromdate_source"), 'yyyy-MM-dd')) \
            .withColumn("eligibilitytodate_source",
                        date_format(col("eligibilitytodate_source"), 'yyyy-MM-dd'))
        elig_month_final_df = elig_month_df.join(elig_final_df,
                                                 on=[elig_final_df["memberid_source"] == elig_month_df["memberid"],
                                                     elig_final_df["eligibilityfromdate_source"] == elig_month_df[
                                                         "eligibilityfromdate"],
                                                     elig_final_df["eligibilitytodate_source"] == elig_month_df[
                                                         "eligibilitytodate"]],
                                                 how="inner")
        elig_month_final_df.drop("memberid_source", "eligibilityfromdate_source", "eligibilitytodate_source")
        return elig_month_final_df, distinct_line_of_cover_codes

    def create_country_of_res_df(self, cor_df):
        self.logger.info("Creating countryofresidence Dataframe")
        cor_ed = 'countryofresidenceenddate'
        cor_st_ed = 'countryofresidencestartdate'

        cor_window = (Window.partitionBy("customerid", "sourcesystemid")
                      .orderBy("countryofresidencetypecode", col("countryofresidenceenddate").desc()))

        col_name = "countryofresidenceenddate"

        cor_df = (cor_df
                  .withColumn(col_name, when(col(col_name).isNull(), dt.strptime('9999-12-31 00:00:00',
                                                                                 DATE_FORMAT_WITH_TS))
                              .otherwise(col(col_name)))
                  .withColumn("country_rank", row_number().over(cor_window)))

        cor_df = (cor_df
                  .where((col("country_rank") == 1) &
                         ((col(cor_ed).isNotNull()) | (col(cor_st_ed).isNotNull())))
                  .withColumnRenamed('etlchecksum', 'cor_checksum')
                  )

        self.logger.info("created countryofresidence Dataframe")
        return cor_df.distinct()

    def create_eligibility_monthly_final_df(self, eligibility_monthly_df: DataFrame, cor_df: DataFrame):
        drop_cols = ["countryofresidenceenddate", "countryofresidencestartdate", "countryofresidencecode",
                     "countryofresidencetypecode"]
        eligibility_monthly_df = eligibility_monthly_df.drop(*drop_cols)
        cor_ed = 'countryofresidenceenddate'
        cor_std = 'countryofresidencestartdate'

        eligibility_final_df = (eligibility_monthly_df
                                .join(cor_df, ((eligibility_monthly_df.customerid == cor_df.customerid)
                                               & (eligibility_monthly_df.sourcesystemid == cor_df.sourcesystemid)
                                               &
                                               ((eligibility_monthly_df['eligibilityfromdate']
                                                 .between(coalesce(cor_df["countryofresidencestartdate"],
                                                                   to_timestamp(lit('1900-01-01'), 'yyyy-MM-dd')),
                                                          coalesce(cor_df["countryofresidenceenddate"],
                                                                   to_timestamp(lit('9999-01-01'), 'yyyy-MM-dd'))))

                                                |
                                                (coalesce(cor_df["countryofresidencestartdate"],
                                                          to_timestamp(lit('1900-01-01'), 'yyyy-MM-dd'))
                                                 .between(eligibility_monthly_df["eligibilityfromdate"],
                                                          eligibility_monthly_df["eligibilitytodate"]))
                                                )
                                               )
                                      , how='left')
                                .drop(cor_df.customerid)
                                .drop(cor_df.sourcesystemid)
                                )

        eligibility_final_df = (eligibility_final_df
                                .withColumn(cor_ed,
                                            when(col(cor_ed).isNull(),
                                                 to_timestamp(lit('9999-12-31'), 'yyyy-MM-dd')).otherwise(col(cor_ed)))
                                .withColumn(cor_std,
                                            when(col(cor_std).isNull(),
                                                 to_timestamp(lit('1900-01-01'), 'yyyy-MM-dd')).otherwise(
                                                col(cor_std))
                                            ))

        self.logger.info("Created the eligibility df")

        if self.is_debug:
            self.logger.info("Created the eligibility df with record count" + str(eligibility_final_df.count()))

        return eligibility_final_df

    def filter_elig_month_df(self, elig_df):
        elig_df = elig_df.withColumn("eligible_month_dt_key", col("eligible_month_dt_key").cast("date"))
        elig_df = elig_df.withColumn("eligibilityfromdate", col("eligibilityfromdate").cast("date"))
        elig_filter_window = Window.partitionBy("memberid", "eligible_month_dt_key").orderBy(col("Eligible").desc(),
                                                                                             abs(expr(
                                                                                                 "datediff(eligible_month_dt_key, eligibilityfromdate)")))

        elig_final_df = elig_df.withColumn("row_number", row_number().over(elig_filter_window)).where(
            col('row_number') == 1).withColumn("eligible_month_dt_key",
                                               date_format(
                                                   col('eligible_month_dt_key'),
                                                   'yyyy-MM-dd'))

        return elig_final_df

    def aggr_cols_add(self, df):
        df = df.withColumn("eligible_month_dt_key", to_date(col("eligible_month_dt_key"), 'yyyy-MM-dd')) \
            .withColumn("eligibilitytodate", to_date(col("eligibilitytodate"), 'yyyy-MM-dd')) \
            .withColumn("eligibilityfromdate", to_date(col("eligibilityfromdate"), 'yyyy-MM-dd')) \
            .withColumn("last_day_of_month", last_day(col("eligible_month_dt_key"))) \
            .withColumn("mid_of_month", expr("date_add(trunc(eligible_month_dt_key, 'MM'), 14)")) \
            .withColumn("Eligibility_EOM", when(
            (col("last_day_of_month").between(col("eligibilityfromdate"), col("eligibilitytodate"))), 1).otherwise(0)) \
            .withColumn("Eligibility_15th", when(
            (col("mid_of_month") >= col("eligibilityfromdate")) & (col("mid_of_month") <= col("eligibilitytodate")),
            1).otherwise(0)) \
            .withColumn("Eligible_days", datediff(col("eligibilityfromdate"), col('eligibilitytodate'))) \
            .withColumn('eligible_start', when(col('eligible_month_dt_key') > col('eligibilityfromdate'),
                                               col('eligible_month_dt_key')).otherwise(col('eligibilityfromdate'))) \
            .withColumn('eligible_end',
                        when(col('last_day_of_month') < col('eligibilitytodate'), col('last_day_of_month')).otherwise(
                            col('eligibilitytodate'))) \
            .withColumn('Eligible_days', when((col('eligible_month_dt_key') > col('eligibilitytodate')) | (
                col('last_day_of_month') < col('eligibilityfromdate')), 0).otherwise(
            datediff(col('eligible_end'), col('eligible_start')) + 1)) \
            .withColumn('days_in_month', dayofmonth(col('last_day_of_month'))) \
            .withColumn('pro_rata_membership', col('Eligible_days') / col('days_in_month'))
        drop_cols = ('mid_of_month', 'last_day_of_month', 'eligible_start', 'eligible_end', 'days_in_month')
        df = df.drop(*drop_cols)

        return df

    def convert_datetime_to_date_format(self, df, date_cols):
        for attr in date_cols:
            df = df.withColumn(attr, to_date(col(attr), 'yyyy-MM-dd'))
        return df

    def create_col_order(self, df, loc_cols):
        cols_start_order = ["memberid",
                            "familyunitid",
                            "familyunittype",
                            "dateofbirth",
                            "tds_currentage",
                            "nationalitycode",
                            "nationality",
                            "gendercode",
                            "relationshiptypecode",
                            "clientstaffcategoryid",
                            "clientstaffcategoryname",
                            "medicalunderwritingcustomertypecode",
                            "memberid_source",
                            "customerid",
                            "countryofresidencecode",
                            "eligibilityfromdate",
                            "eligibilitytodate",
                            "eligibilityterminationdate",
                            "memberterminationdate",
                            "memberadditiondate",
                            "eligible_month_dt_key",
                            "eligible",
                            "eligibility_eom",
                            "eligibility_15th",
                            "eligible_days",
                            "pro_rata_membership",
                            "eligibilityfromdate_source",
                            "eligibilitytodate_source",
                            "policyid",
                            "policystatuscode",
                            "policyname",
                            "policystartdate",
                            "policyenddate",
                            "productid",
                            "productname",
                            "packageid",
                            "packagename",
                            "areaofcovercode",
                            "claimsriskarrangement",
                            "parentclientid",
                            "clientid",
                            "ancestorid",
                            "clientlevel",
                            "clientname",
                            "clientcontractstartdate",
                            "clientcontractenddate",
                            "clientinceptiondate",
                            "clientterminationdate",
                            "legalentityid",
                            "legalentityname",
                            "lineofbusinessid",
                            "lineofbusinessname",
                            "segment",
                            "market"]

        cols_end_oder = ["sourcesystemid", "sourcesystemname"]
        all_monthly_table_cols = cols_start_order + loc_cols + cols_end_oder
        df = df.select(*all_monthly_table_cols)
        return df
