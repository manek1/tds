from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, explode, sequence, expr, concat,
    date_format, lit, last_day, to_date, array_contains,
    array, first, dayofmonth, datediff, row_number
)
from pyspark.sql.window import Window

# ========== STEP 0: START SPARK SESSION ==========

spark = SparkSession.builder.appName("EligibilityHybrid").getOrCreate()

# ========== STEP 1: READ ELIGIBILITY TABLE WITH SPARK SQL ==========

elig_df = spark.sql("""
    SELECT *,
           CASE
               WHEN sourcesystemid = '1' THEN 'Medical'
               ELSE lineofcover
           END AS lineofcover,
           CASE
               WHEN sourcesystemid = '1' THEN 'MED'
               ELSE lineofcovercode
           END AS lineofcovercode
    FROM eligibility_output
    WHERE eligibilitytodate > eligibilityfromdate
      AND TRIM(lineofcovercode) != ''
      AND lineofcovercode IS NOT NULL
""")

# ========== STEP 2: EXPLODE MONTH SEQUENCE ==========

elig_df = elig_df.withColumn(
    "eligible_month_dt_key",
    explode(
        sequence(
            expr("TO_DATE(CONCAT('01-01-', YEAR(eligibilityfromdate)), 'dd-MM-yyyy')"),
            expr("TO_DATE(CONCAT('31-12-', YEAR(eligibilitytodate)), 'dd-MM-yyyy')"),
            expr("INTERVAL 1 MONTH")
        )
    )
)

# ========== STEP 3: CREATE ELIGIBILITY RANGE CHECKERS ==========

elig_df = elig_df.withColumn(
    'eligibilityfromdate_checker',
    to_date(concat(lit('01'), date_format(col("eligibilityfromdate"), "MMyyyy")), 'ddMMyy')
).withColumn(
    'eligibilitytodate_checker',
    last_day(col("eligibilitytodate"))
)

# ========== STEP 4: CALCULATE is_eligible FLAG ==========

elig_df = elig_df.withColumn(
    "is_eligible",
    when(
        (col("eligible_month_dt_key") >= col("eligibilityfromdate_checker")) &
        (col("eligible_month_dt_key") <= col("eligibilitytodate_checker")),
        lit(1)
    ).otherwise(0)
).drop("eligibilityfromdate_checker", "eligibilitytodate_checker")

# ========== STEP 5: PIVOT ON MED ONLY ==========

elig_df_pivoted = elig_df.groupBy(
    "memberid", "eligibilityfromdate", "eligibilitytodate", "eligible_month_dt_key"
).pivot(
    "lineofcovercode", ["MED"]
).agg(
    first("is_eligible")
).na.fill(0)

# ========== STEP 6: CREATE Eligible FLAG ==========

elig_df_pivoted = elig_df_pivoted.withColumn(
    "Eligible",
    when(col("MED").cast('int') == 1, 1).otherwise(0)
)

# ========== STEP 7: PREPARE SNAPSHOT DF ==========

window_spec = Window.partitionBy(
    "memberid", "eligibilityfromdate", "eligibilitytodate"
).orderBy(col("sysbatchdttm"))

elig_snapshot = elig_df.withColumn(
    "row_number",
    row_number().over(window_spec)
).filter(
    col("row_number") == 1
).drop("row_number")

elig_snapshot = elig_snapshot \
    .withColumnRenamed("eligibilityfromdate", "eligibilityfromdate_source") \
    .withColumnRenamed("eligibilitytodate", "eligibilitytodate_source") \
    .withColumnRenamed("memberid", "memberid_source")

# ========== STEP 8: JOIN PIVOTED DATA WITH SNAPSHOT ==========

elig_month_final_df = elig_df_pivoted.join(
    elig_snapshot,
    (elig_df_pivoted["memberid"] == elig_snapshot["memberid_source"]) &
    (date_format(elig_df_pivoted["eligibilityfromdate"], 'yyyy-MM-dd') ==
     date_format(elig_snapshot["eligibilityfromdate_source"], 'yyyy-MM-dd')) &
    (date_format(elig_df_pivoted["eligibilitytodate"], 'yyyy-MM-dd') ==
     date_format(elig_snapshot["eligibilitytodate_source"], 'yyyy-MM-dd')),
    "inner"
).drop(
    "memberid_source", "eligibilityfromdate_source", "eligibilitytodate_source"
)

# ========== STEP 9: ADD AGGREGATION COLUMNS ==========

elig_month_final_df = elig_month_final_df \
    .withColumn("eligible_month_dt_key", to_date(col("eligible_month_dt_key"), 'yyyy-MM-dd')) \
    .withColumn("eligibilitytodate", to_date(col("eligibilitytodate"), 'yyyy-MM-dd')) \
    .withColumn("eligibilityfromdate", to_date(col("eligibilityfromdate"), 'yyyy-MM-dd')) \
    .withColumn("last_day_of_month", expr("last_day(eligible_month_dt_key)")) \
    .withColumn("mid_of_month", expr("date_add(trunc(eligible_month_dt_key, 'MM'), 14)")) \
    .withColumn("Eligibility_EOM", when(
        (col("last_day_of_month").between(col("eligibilityfromdate"), col("eligibilitytodate"))), 1
    ).otherwise(0)) \
    .withColumn("Eligibility_15th", when(
        (col("mid_of_month") >= col("eligibilityfromdate")) &
        (col("mid_of_month") <= col("eligibilitytodate")), 1
    ).otherwise(0)) \
    .withColumn('eligible_start', when(col('eligible_month_dt_key') > col('eligibilityfromdate'),
                                       col('eligible_month_dt_key')).otherwise(col('eligibilityfromdate'))) \
    .withColumn('eligible_end', when(col('last_day_of_month') < col('eligibilitytodate'),
                                     col('last_day_of_month')).otherwise(col('eligibilitytodate'))) \
    .withColumn('Eligible_days',
                when((col('eligible_month_dt_key') > col('eligibilitytodate')) |
                     (col('last_day_of_month') < col('eligibilityfromdate')), 0)
                .otherwise(datediff(col('eligible_end'), col('eligible_start')) + 1)) \
    .withColumn('days_in_month', dayofmonth(col('last_day_of_month'))) \
    .withColumn('pro_rata_membership', col('Eligible_days') / col('days_in_month')) \
    .drop("mid_of_month", "last_day_of_month", "eligible_start", "eligible_end", "days_in_month")

# ========== STEP 10: FINAL COLUMN ORDER ==========

cols_start_order = [
    "memberid",
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
    "Eligible",
    "Eligibility_EOM",
    "Eligibility_15th",
    "Eligible_days",
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
    "market",
    "MED"
]

cols_end_order = ["sourcesystemid", "sourcesystemname"]

final_cols = cols_start_order + cols_end_order
final_cols = [c for c in final_cols if c in elig_month_final_df.columns]

elig_month_final_df = elig_month_final_df.select(*final_cols)

# ========== STEP 11: WRITE TO ATHENA TABLE ==========

elig_month_final_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3://your-bucket/athena-output-path/")

# optionally create/repair table in Athena if writing to external table
# spark.sql("MSCK REPAIR TABLE your_athena_table")
