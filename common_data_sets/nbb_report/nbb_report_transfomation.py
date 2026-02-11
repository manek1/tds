from common_data_sets.common.spark_common_functions import month_diff, quarter_diff, year_diff, is_empty
from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql import DataFrame, Window as Win


class NbbTriangleReport:

    def __init__(self, spark: SparkSession, logger, is_debug: bool = False):
        self.spark = spark
        self.logger = logger
        self.is_debug = is_debug

    def enrich_amounts_currencies(self, nbb_claims_df: DataFrame, exchangerate_df: DataFrame,
                                  conversion_date: str = "reporteddate",
                                  conversion_currency_code: str = "productcurrencycode",
                                  conversion_amount:str = "productcurrencypaymentamount") -> DataFrame:

        if is_empty(exchangerate_df):
            self.logger.error("IDS NbbClaims table is empty, returning empty currencies conversions")
            nbb_claims_df = (nbb_claims_df
                             .withColumn("amount_usd", F.lit(None).cast("float"))
                             .withColumn("amount_chf", F.lit(None).cast("float"))
                             .withColumn("amount_gbp", F.lit(None).cast("float"))
                             .withColumn("amount_euro", F.lit(None).cast("float"))
                             )

            return nbb_claims_df

        self.logger.info(f"calculating USD, CHF, GBP, EUR amounts for {conversion_currency_code}")

        exchangerate_df = (exchangerate_df
                           .withColumn("exchange_priority",
                                       F.when(F.col('exchangeratesourcecode') == 'OAB', F.lit(1))
                                       .otherwise(F.lit(2))
                                       )
                           )

        exc_win = (Win.partitionBy(F.col('exchangeratedate'), F.col('exchangeratecurrencytocode')
                                   , F.col('exchangeratecurrencyfromcode'))
                   .orderBy(F.col('exchange_priority').asc(), F.col("exchangerate").asc()))

        exchangerate_df = exchangerate_df.withColumn("exchange_rank", F.rank().over(exc_win))

        final_exchange_df = (exchangerate_df
                             .where((F.col("exchangeratecurrencyfromcode").isin('USD', 'GBP', 'EUR', 'CHF'))
                                    & (F.col("exchange_rank") == 1))
                             .selectExpr('exchangeratecurrencyfromcode as newcurrencycode',
                                         'exchangeratecurrencytocode as basecurrencycode',
                                         'exchangeratedate',
                                         'round(1/exchangerate, 6) as exchangerate')
                             .distinct()
                             )

        final_exchange_df = (final_exchange_df
                             .withColumn("exchangerate_usd",
                                         F.when(F.col("newcurrencycode") == 'USD', F.col("exchangerate"))
                                         .otherwise(F.lit(0)))
                             .withColumn("exchangerate_gbp",
                                         F.when(F.col("newcurrencycode") == 'GBP', F.col("exchangerate"))
                                         .otherwise(F.lit(0)))
                             .withColumn("exchangerate_euro",
                                         F.when(F.col("newcurrencycode") == 'EUR', F.col("exchangerate"))
                                         .otherwise(F.lit(0)))
                             .withColumn("exchangerate_chf",
                                         F.when(F.col("newcurrencycode") == 'CHF', F.col("exchangerate"))
                                         .otherwise(F.lit(0)))
                             .withColumn("exchange_month", F.date_format(F.col('exchangeratedate'), 'yyyyMM'))
                             )

        self.logger.info("Distinct Months")

        final_exchange_df.where((F.col('exchange_month') == '202512')).select(F.col('newcurrencycode')).distinct().show()
        final_exchange_df.where((F.col('exchange_month') == '202512')).select(F.col('basecurrencycode')).distinct().show()

        self.logger.info("202512 Months")
        (final_exchange_df
         .where((F.col('exchange_month') == '202512')
                & (F.col('newcurrencycode') == 'GBP')
                )
         .distinct()
         .show())

        self.logger.info("GBP EUR")
        (final_exchange_df
         .where((F.col('exchange_month') == '202512')
                &(F.col('newcurrencycode') == 'GBP')
                &(F.col('basecurrencycode') == 'EUR')
                )
         .distinct()
         .show())

        nbb_claims_enriched_df = (nbb_claims_df
                                  .join(final_exchange_df,
                                        on=((nbb_claims_df[conversion_currency_code] == final_exchange_df[
                                            "basecurrencycode"])
                                            & (nbb_claims_df[conversion_date] == final_exchange_df[
                                                    "exchangeratedate"])),
                                        how="left")
                                  .withColumn("amount_usd", F.col(conversion_amount)*F.col("exchangerate_usd"))
                                  .withColumn("amount_chf", F.col(conversion_amount)*F.col("exchangerate_chf"))
                                  .withColumn("amount_gbp", F.col(conversion_amount)*F.col("exchangerate_gbp"))
                                  .withColumn("amount_euro",F.col(conversion_amount)*F.col("exchangerate_euro"))
                                  .withColumn("book_of_business", F.when(F.col("sourcesystemid") == '1', F.lit('IOH'))
                                              .when(F.col("sourcesystemid") == '5',
                                                    F.when(F.col('legalentityid') == '82813', F.lit('CGIC Qatar'))
                                                    .when(F.col('legalentityid') == '53154', F.lit('GHB Bahrain NA'))
                                                    .when(F.col('legalentityid') == '53134', F.lit('GHB Czech NA'))
                                                    .otherwise(F.lit('CGIC NA')))
                                              .when(F.col("sourcesystemid") == '8', F.lit('GIH'))
                                              .when(F.col("sourcesystemid").isin('2','3','4'),
                                                    F.when(F.col('legalentityid') == '82440', F.lit('GHB Singapore'))
                                                    .when(F.col('legalentityid').isin('82810','82811','82812'),
                                                          F.lit('GHB Africa'))
                                                    .otherwise(F.lit('GHB Europe')))
                                              .otherwise(None))
                                  )
        self.logger.info("After join cols")
        (nbb_claims_enriched_df
         .where((F.col('exchange_month') == '202512')
                & (F.col('newcurrencycode') == 'GBP')
                & (F.col('basecurrencycode') == 'EUR')
                )
         .distinct()
         .show())

        return nbb_claims_enriched_df

    def add_periods(self, nbb_claims_df: DataFrame, date_col: str) -> DataFrame:
        self.logger.info(f"Adding period data for date column: {date_col}")

        return (nbb_claims_df
                .withColumn("year_month", F.date_format(F.col(date_col), "yyyyMM"))
                .withColumn(
            "year_quarter",
            F.concat(
                F.date_format(F.col(date_col), "yyyy"),
                F.lit("Q"),
                F.quarter(F.col(date_col)).cast("string")
            )
        )
                .withColumn("year_only", F.date_format(F.col(date_col), "yyyy"))
                )

    def nbb_extract_all_periods(self,
                                nbb_claims_df: DataFrame,
                                exchangerate_df: DataFrame,
                                metric_value_col: str,
                                metric_type_str: str,
                                start_date_col: str="servicedate",
                                end_date_col: str="claiminvoicereceiveddate",
                                amount_col: str="productcurrencypaymentamount",
                                triangle_report_type: str="incurred to Received"
                                ) -> tuple[DataFrame, DataFrame, DataFrame]:

        nbb_claims_df = (nbb_claims_df
                        .withColumn("triangle_report_type", F.lit(triangle_report_type))
                        .where((F.upper(F.col('claimsriskarrangement')) == 'FULLY INSURED')
                                    & (F.col(start_date_col) <= F.col(end_date_col)))
                         )

        # Calculate additional currency amounts
        claims_pay_nbb_df = self.enrich_amounts_currencies(nbb_claims_df, exchangerate_df,
                                                           conversion_date=start_date_col,
                                                           conversion_currency_code=metric_value_col,
                                                           conversion_amount=amount_col)
        # claims_pay_nbb_df.show(5)
        # Add period columns
        nbb_claims_df = claims_pay_nbb_df.transform(lambda d: self.add_periods(d, start_date_col))

        # Add month difference
        nbb_claims_df = (
            nbb_claims_df.transform(
                lambda d: month_diff(d, F.col(start_date_col), F.col(end_date_col), "paid_lag_months"))
            .transform(lambda d: quarter_diff(d, F.col(start_date_col), F.col(end_date_col), "paid_lag_quarters"))
            .transform(lambda d: year_diff(d, F.col(start_date_col), F.col(end_date_col), "paid_lag_years")))

        # ------------------------
        # 1️⃣ YEAR AGGREGATION
        # ------------------------
        agg_year_df = (nbb_claims_df
                       .groupBy(
            F.col(metric_value_col).alias("metric_value"),
            "year_only",
            "legalentityname",
            "legalentityid",
            "book_of_business",
            "paid_lag_years",
            "triangle_report_type"
        )
                       .agg(F.sum(F.col(amount_col)).alias("sum_amount_base_currency"),
                            F.sum(F.col("amount_usd")).alias("sum_amount_usd"),
                            F.sum(F.col("amount_gbp")).alias("sum_amount_gbp"),
                            F.sum(F.col("amount_euro")).alias("sum_amount_euro"),
                            F.sum(F.col("amount_chf")).alias("sum_amount_chf"))
                       .withColumn("metric_type", F.lit(metric_type_str))
                       .withColumnRenamed("YEAR", "period_type")
                       .withColumnRenamed("year_only", "incurred_period")
                       .withColumnRenamed("legalentityname", "legal_entity_name")
                       .withColumnRenamed("legalentityid", "legal_entity_id")
                       .withColumnRenamed("paid_lag_years", "paid_lag_value")
                       .withColumn("incurred_period_type", F.lit("YEAR"))
                       )

        # ------------------------
        # 2️⃣ QUARTER AGGREGATION
        # ------------------------
        agg_quarter_df = (nbb_claims_df
                          .groupBy(
            F.col(metric_value_col).alias("metric_value"),
            "year_quarter",
            "legalentityname",
            "legalentityid",
            "book_of_business",
            "paid_lag_quarters",
            "triangle_report_type"
        )
                          .agg(F.sum(F.col(amount_col)).alias("sum_amount_base_currency")
                               ,F.sum(F.col("amount_usd")).alias("sum_amount_usd"),
                                    F.sum(F.col("amount_gbp")).alias("sum_amount_gbp"),
                                    F.sum(F.col("amount_euro")).alias("sum_amount_euro"),
                                    F.sum(F.col("amount_chf")).alias("sum_amount_chf"))
                          .withColumn("metric_type", F.lit(metric_type_str))
                          .withColumnRenamed("QUARTER", "period_type")
                          .withColumnRenamed("year_quarter", "incurred_period")
                          .withColumnRenamed("legalentityname", "legal_entity_name")
                          .withColumnRenamed("legalentityid", "legal_entity_id")
                          .withColumnRenamed("paid_lag_quarters", "paid_lag_value")
                          .withColumn("incurred_period_type", F.lit("QUARTER"))
                          )

        # ------------------------
        # 3️⃣ MONTH AGGREGATION
        # ------------------------
        agg_month_df = (nbb_claims_df
                        .groupBy(
            F.col(metric_value_col).alias("metric_value"),
            "year_month",
            "legalentityname",
            "legalentityid",
            "book_of_business",
            "paid_lag_months",
            "triangle_report_type"
        )
                        .agg(F.sum(F.col(amount_col)).alias("sum_amount_base_currency"),
                             F.sum(F.col("amount_usd")).alias("sum_amount_usd"),
                             F.sum(F.col("amount_gbp")).alias("sum_amount_gbp"),
                             F.sum(F.col("amount_euro")).alias("sum_amount_euro"),
                             F.sum(F.col("amount_chf")).alias("sum_amount_chf")
                             )
                        .withColumn("metric_type", F.lit(metric_type_str))
                        .withColumnRenamed("MONTH", "period_type")
                        .withColumnRenamed("year_month", "incurred_period")
                        .withColumnRenamed("paid_lag_months", "paid_lag_value")
                        .withColumnRenamed("legalentityname", "legal_entity_name")
                        .withColumnRenamed("legalentityid", "legal_entity_id")
                        .withColumn("incurred_period_type", F.lit("MONTH"))
                        )

        return agg_year_df, agg_quarter_df, agg_month_df

    def build_incurred_to_received(self, claims_nbb_df: DataFrame, exchangerate_df: DataFrame) -> DataFrame:

        claims_nbb_df = claims_nbb_df.where(F.col("servicedate") <= F.col("claiminvoicereceiveddate"))

        y1, q1, m1 = self.nbb_extract_all_periods(claims_nbb_df, exchangerate_df,
                                                  metric_value_col="productcurrencycode",
                                                  metric_type_str="Productcurrency", start_date_col="servicedate",
                                                  end_date_col="claiminvoicereceiveddate",
                                                  amount_col="productcurrencypaymentamount",
                                                  triangle_report_type="Incurred to Received")

        # Payment currency
        y2, q2, m2 = self.nbb_extract_all_periods(claims_nbb_df,  exchangerate_df, metric_value_col="paymentcurrencycode",
                                                  metric_type_str="Paymentcurrency", start_date_col="servicedate",
                                                  end_date_col="claiminvoicereceiveddate",
                                                  amount_col="paymentcurrencypaymentamount",
                                                  triangle_report_type="Incurred to Received")

        final_i2r_year = y1.unionByName(y2)
        final_i2r_quarter = q1.unionByName(q2)
        final_i2r_month = m1.unionByName(m2)

        return final_i2r_year.unionByName(final_i2r_quarter.unionByName(final_i2r_month))

    def build_incurred_to_paid(self, claims_nbb_df: DataFrame, exchangerate_df: DataFrame) -> DataFrame:

        claims_nbb_df = claims_nbb_df.where(F.col("servicedate") <= F.col("paymentdate"))

        y1, q1, m1 = self.nbb_extract_all_periods(claims_nbb_df, exchangerate_df,
                                                  metric_value_col="productcurrencycode",
                                                  metric_type_str="Productcurrency", start_date_col="servicedate",
                                                  end_date_col="paymentdate", amount_col="productcurrencypaymentamount",
                                                  triangle_report_type="Incurred to Paid")

        # Payment currency
        y2, q2, m2 = self.nbb_extract_all_periods(claims_nbb_df, exchangerate_df,
                                                  metric_value_col="paymentcurrencycode",
                                                  metric_type_str="Paymentcurrency", start_date_col="servicedate",
                                                  end_date_col="paymentdate", amount_col="productcurrencypaymentamount",
                                                  triangle_report_type="Incurred to Received")

        final_i2p_year = y1.unionByName(y2)
        final_i2p_quarter = q1.unionByName(q2)
        final_i2p_month = m1.unionByName(m2)

        return final_i2p_year.unionByName(final_i2p_quarter.unionByName(final_i2p_month))

    def build_received_to_paid(self, claims_nbb_df: DataFrame, exchangerate_df: DataFrame) -> DataFrame:

        claims_nbb_df = claims_nbb_df.where(F.col("claiminvoicereceiveddate") <= F.col("paymentdate"))

        y1, q1, m1 = self.nbb_extract_all_periods(claims_nbb_df, exchangerate_df,
                                                  metric_value_col="productcurrencycode",
                                                  metric_type_str="Productcurrency",
                                                  start_date_col="claiminvoicereceiveddate", end_date_col="paymentdate",
                                                  amount_col="productcurrencypaymentamount",
                                                  triangle_report_type="Received to Paid")

        # Payment currency
        y2, q2, m2 = self.nbb_extract_all_periods(claims_nbb_df, exchangerate_df,
                                                  metric_value_col="paymentcurrencycode",
                                                  metric_type_str="Paymentcurrency",
                                                  start_date_col="claiminvoicereceiveddate", end_date_col="paymentdate",
                                                  amount_col="productcurrencypaymentamount",
                                                  triangle_report_type="Received to Paid")

        final_r2p_year = y1.unionByName(y2)
        final_r2p_quarter = q1.unionByName(q2)
        final_r2p_month = m1.unionByName(m2)

        return final_r2p_year.unionByName(final_r2p_quarter.unionByName(final_r2p_month))

    def build_final_nbb_extract(self, final_i2r_df: DataFrame, final_i2p_df: DataFrame,final_r2p_df:DataFrame) -> DataFrame:
            return final_i2r_df.unionByName(final_i2p_df.unionByName(final_r2p_df))
