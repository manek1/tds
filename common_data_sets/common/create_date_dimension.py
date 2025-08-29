from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_format, weekofyear, year, month, dayofweek, dayofmonth, quarter, \
    expr, last_day, dayofyear, trunc, datediff, to_date, when
from common_data_sets.common.spark_common_functions import logger


class DateDimension:
    def __init__(self, spark_session, start_date="2000-01-01", end_date="2050-12-31"):
        self.spark = spark_session
        self.start_date = start_date
        self.end_date = end_date

    def create_date_dimension(self):
        # Generate a range of dates
        df = self.spark.sql(f"SELECT sequence(to_date('{self.start_date}'), to_date('{self.end_date}'), interval 1 "
                            f"day) as date_list")
        df = df.selectExpr("explode(date_list) as date_key")

        logger.info("Extracting Year, Quarter, Month, and Weekday Information")

        df = df.withColumn("cy_name", year(col("date_key"))) \
            .withColumn("cq_name", expr("concat('Q', quarter(date_key))")) \
            .withColumn("fq_name", expr("concat('Q', quarter(date_key))")) \
            .withColumn("fy_name", year(col("date_key"))) \
            .withColumn("cq_and_cy_name", expr("concat('Q', quarter(date_key), ' ', year(date_key))")) \
            .withColumn("fq_and_fy_name", expr("concat('Q', quarter(date_key), ' ', year(date_key))")) \
            .withColumn("month_name", date_format(col("date_key"), "MMM")) \
            .withColumn("month_and_cy_name", date_format(col("date_key"), "MMM-yy")) \
            .withColumn("month_and_fy_name", date_format(col("date_key"), "MMM-yy")) \
            .withColumn("weekday", date_format(col("date_key"), "E")) \
            .withColumn("week_number", weekofyear(col("date_key"))) \
            .withColumn("week_number_cy", weekofyear(col("date_key"))) \
            .withColumn("week_number_fy", weekofyear(col("date_key")))

        logger.info("Day Number Calculations")

        df = df.withColumn("day_number_in_cy", dayofyear(col("date_key"))) \
            .withColumn("day_number_til_end_cy", expr("365 - dayofyear(date_key)")) \
            .withColumn("day_number_in_cq", expr("datediff(date_key, trunc(date_key, 'Q')) + 1")) \
            .withColumn("day_number_til_end_cq", expr("datediff(last_day(trunc(date_key, 'Q')), date_key)")) \
            .withColumn("day_number_in_fq", expr("datediff(date_key, trunc(date_key, 'Q')) + 1")) \
            .withColumn("day_number_til_end_fq", expr("datediff(last_day(trunc(date_key, 'Q')), date_key)")) \
            .withColumn("day_number_in_fy", dayofyear(col("date_key"))) \
            .withColumn("day_number_til_end_fy", expr("365 - dayofyear(date_key)")) \
            .withColumn("day_number_in_month", dayofmonth(col("date_key"))) \
            .withColumn("day_number_in_week", dayofweek(col("date_key"))) \
            .withColumn("day_month_begin", expr("case when dayofmonth(date_key) = 1 then 1 else 0 end")) \
            .withColumn("day_month_end", expr("case when date_key = last_day(date_key) then 1 else 0 end")) \
            .withColumn(
            "last_day_quarter", expr("""to_date(
                    date_trunc('quarter', to_date(date_key) + interval 3 months)
                ) - interval 1 day
            """)) \
            .withColumn(
            "first_day_quarter", expr("to_date(date_trunc('quarter', to_date(date_key)))")) \
            .withColumn("last_day_month", expr("last_day(to_date(date_key))")) \
            .withColumn("last_working_day_month",
                        when(~dayofweek("last_day_month").isin('6', '7'), col('last_day_month'))
                        .when(dayofweek("last_day_month") == '7', expr("last_day_month - interval 1 day"))
                        .otherwise(expr("last_day_month - interval 2 day"))
                        )

        logger.info("Week Calculations")

        df = df.withColumn("week_monday", expr("date_sub(date_key, dayofweek(date_key)-2)")) \
            .withColumn("week_friday", expr("date_add(week_monday, 4)")) \
            .withColumn("week_saturday", expr("date_add(week_monday, 5)")) \
            .withColumn("week_sunday", expr("date_add(week_monday, 6)")) \
            .withColumn("week_number_til_end_cy", expr("53 - weekofyear(date_key) + 1")) \
            .withColumn("week_number_til_end_fy", expr("53 - weekofyear(date_key) + 1")) \
            .withColumn("week_number_cq", expr("weekofyear(date_key) - weekofyear(trunc(date_key, 'Q')) + 1")) \
            .withColumn("week_number_fq", expr("weekofyear(date_key) - weekofyear(trunc(date_key, 'Q')) + 1")) \
            .withColumn("week_number_til_end_cq", expr("14 - week_number_cq")) \
            .withColumn("week_number_til_end_fq", expr("14 - week_number_fq"))

        logger.info("Month Calculations")

        df = df.withColumn("month_number", expr("year(date_key) * 12 + month(date_key)")) \
            .withColumn("month_number_in_cq", expr("month(date_key) - (quarter(date_key)-1)*3")) \
            .withColumn("month_number_in_fq", expr("month(date_key) - (quarter(date_key)-1)*3")) \
            .withColumn("month_number_in_cy", month(col("date_key"))) \
            .withColumn("month_number_in_fy", month(col("date_key"))) \
            .withColumn("month_number_til_end_fy", expr("12 - month(date_key)")) \
            .withColumn("month_number_til_end_cy", expr("12 - month(date_key)")) \
            .withColumn("CIGNA_Month_End", when(col("last_working_day_month") == col("date_key"), 1).otherwise(0))

        logger.info("Julian Date Counter")

        df = df.withColumn("julian_counter", expr("datediff(date_key, '1900-01-01') + 1"))

        logger.info("Additional Flags")

        df = df.withColumn("vacation_day", lit(0)) \
            .withColumn("Bank_Holiday_UK", lit(0)) \
            .withColumn("Working_Day_UK", expr("case when dayofweek(date_key) not in (1,5) then 0 else 1 end")) \
            .withColumn("Bank_Holiday_CY", lit(0)) \
            .withColumn("Weekend_Working_Day_CY", lit(0)) \
            .withColumn("Working_Day_CY", expr("case when dayofweek(date_key) in (1,5) then 0 else 1 end")) \
            .withColumn("Working_Day_Counter_UK", expr("row_number() over (order by date_key)")) \
            .withColumn("Working_Day_Counter_CY", expr("row_number() over (order by date_key)")) \
            .withColumn("day_cq_begin",
                        when(col('first_day_quarter') == col("date_key"), 1).otherwise(0)) \
            .withColumn("day_cq_end", when(col('last_day_quarter') == col("date_key"), 1).otherwise(0)) \
            .withColumn("day_cy_begin",
                        when((col("month_number") == 1) & (date_format(col("date_key"), "dd") == "01"), 1).otherwise(0)) \
            .withColumn("day_cy_end",
                        when((col("month_number") == 12) & (col("date_key") == last_day(col("date_key"))), 1).otherwise(
                            0))

        logger.info("Copy Columns")

        df = (df
              .withColumn("date_key_REAL", col("date_key"))
              .withColumn("day_name", col("date_key"))
              .withColumn("day_fy_begin", col("day_cy_begin"))
              .withColumn("day_fy_end", col("day_cy_end"))
              .withColumn("day_fq_begin", col("day_cq_begin"))
              .withColumn("day_fq_end", col("day_cq_end"))
              .drop(*["day_number_til_end_cy",
                      "day_number_til_end_fy",
                      "first_day_quarter",
                      "last_day_month",
                      "last_day_quarter",
                      "last_working_day_month"])
              )

        df.show(10, False)

        return df
