
"""nbb_triangle_report_dq.py

Single-file Data Quality (DQ) checks for NBB Claims Triangle Report outputs.

Designed for Spark (PySpark) pipelines that produce the external table:
  nbb_claims_triangle_report

How to use (typical):
  from nbb_triangle_report_dq import run_triangle_report_dq
  results = run_triangle_report_dq(final_df, strict=True)

Optional FX join integrity checks:
  from nbb_triangle_report_dq import check_fx_join_integrity
  fx_results = check_fx_join_integrity(before_df, after_df, final_exchange_df)

Notes:
- These checks are intended as *gating* validations (fail-fast) when strict=True.
- When strict=False, the module returns a structured results dict without raising.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class DQRuleResult:
    name: str
    passed: bool
    failed_rows: int = 0
    details: str = ""


def _count(df: DataFrame) -> int:
    return int(df.count())


def dq_not_null(df: DataFrame, cols: Sequence[str], rule_name: str) -> DQRuleResult:
    missing_cols = [c for c in cols if c not in df.columns]
    if missing_cols:
        return DQRuleResult(rule_name, False, 0, f"Missing columns: {missing_cols}")

    expr = None
    for c in cols:
        cond = F.col(c).isNull()
        expr = cond if expr is None else (expr | cond)

    failed = _count(df.filter(expr))
    return DQRuleResult(rule_name, failed == 0, failed, f"Nulls in {list(cols)}")


def dq_in_set(df: DataFrame, col: str, allowed: Sequence[str], rule_name: str) -> DQRuleResult:
    if col not in df.columns:
        return DQRuleResult(rule_name, False, 0, f"Missing column: {col}")
    failed = _count(df.filter(~F.col(col).isin([*allowed])))
    return DQRuleResult(rule_name, failed == 0, failed, f"{col} not in {list(allowed)}")


def dq_regex(df: DataFrame, col: str, pattern: str, rule_name: str) -> DQRuleResult:
    if col not in df.columns:
        return DQRuleResult(rule_name, False, 0, f"Missing column: {col}")
    failed = _count(df.filter(~F.col(col).rlike(pattern)))
    return DQRuleResult(rule_name, failed == 0, failed, f"{col} does not match regex {pattern}")


def dq_no_nan(df: DataFrame, cols: Sequence[str], rule_name: str) -> DQRuleResult:
    missing_cols = [c for c in cols if c not in df.columns]
    if missing_cols:
        return DQRuleResult(rule_name, False, 0, f"Missing columns: {missing_cols}")

    expr = None
    for c in cols:
        cond = F.isnan(F.col(c))
        expr = cond if expr is None else (expr | cond)

    failed = _count(df.filter(expr))
    return DQRuleResult(rule_name, failed == 0, failed, f"NaN present in {list(cols)}")


def dq_triangle_partition_match(df: DataFrame,
                               triangle_value_col: str = "triangle_report_type_val",
                               triangle_partition_col: str = "triangle_report_type",
                               rule_name: str = "triangle_type_value_matches_partition") -> DQRuleResult:
    if triangle_value_col not in df.columns or triangle_partition_col not in df.columns:
        return DQRuleResult(rule_name, False, 0, f"Missing cols: {triangle_value_col} or {triangle_partition_col}")
    failed = _count(df.filter(F.col(triangle_value_col) != F.col(triangle_partition_col)))
    return DQRuleResult(rule_name, failed == 0, failed,
                        f"{triangle_value_col} must equal {triangle_partition_col}")


def dq_incurred_period_format(df: DataFrame,
                             period_col: str = "incurred_period",
                             period_type_col: str = "incurred_period_type",
                             rule_name: str = "incurred_period_format_by_type") -> DQRuleResult:
    if period_col not in df.columns or period_type_col not in df.columns:
        return DQRuleResult(rule_name, False, 0, f"Missing cols: {period_col} or {period_type_col}")

    bad_month = (F.col(period_type_col) == F.lit("MONTH")) & (~F.col(period_col).rlike(r"^[0-9]{6}$"))
    bad_year = (F.col(period_type_col) == F.lit("YEAR")) & (~F.col(period_col).rlike(r"^[0-9]{4}$"))
    bad_quarter = (F.col(period_type_col) == F.lit("QUARTER")) & (~F.col(period_col).rlike(r"^[0-9]{4}Q[1-4]$"))

    failed = _count(df.filter(bad_month | bad_year | bad_quarter))
    return DQRuleResult(rule_name, failed == 0, failed,
                        "MONTH must be yyyyMM, YEAR yyyy, QUARTER yyyyQ[1-4]")


def dq_lag_sanity(df: DataFrame,
                  lag_col: str = "paid_lag_value",
                  allow_negative: bool = False,
                  max_val: int = 99,
                  month_cap_59_to_99: bool = True,
                  period_type_col: str = "incurred_period_type",
                  rule_name: str = "paid_lag_sanity") -> DQRuleResult:
    if lag_col not in df.columns:
        return DQRuleResult(rule_name, False, 0, f"Missing column: {lag_col}")

    conds = []
    if not allow_negative:
        conds.append(F.col(lag_col) < 0)
    conds.append(F.col(lag_col) > max_val)

    if month_cap_59_to_99 and period_type_col in df.columns:
        # only apply for MONTH rows: if lag >59 then expect 99
        conds.append((F.col(period_type_col) == F.lit("MONTH")) & (F.col(lag_col) > 59) & (F.col(lag_col) != 99))

    expr = None
    for c in conds:
        expr = c if expr is None else (expr | c)

    failed = _count(df.filter(expr)) if expr is not None else 0
    details = f"allow_negative={allow_negative}, max_val={max_val}, month_cap_59_to_99={month_cap_59_to_99}"
    return DQRuleResult(rule_name, failed == 0, failed, details)


def dq_fx_missing_conversions(df: DataFrame,
                             base_col: str = "sum_amount_base_currency",
                             fx_cols: Sequence[str] = ("sum_amount_usd", "sum_amount_gbp", "sum_amount_euro", "sum_amount_chf"),
                             rule_name: str = "fx_conversions_present_when_base_present") -> DQRuleResult:
    missing = [c for c in (base_col, *fx_cols) if c not in df.columns]
    if missing:
        return DQRuleResult(rule_name, False, 0, f"Missing cols: {missing}")

    # base is non-null and non-zero AND all FX cols are null
    expr_all_null = None
    for c in fx_cols:
        cond = F.col(c).isNull()
        expr_all_null = cond if expr_all_null is None else (expr_all_null & cond)

    failed = _count(df.filter((F.col(base_col).isNotNull()) & (F.col(base_col) != 0) & expr_all_null))
    return DQRuleResult(rule_name, failed == 0, failed,
                        f"Base present but {list(fx_cols)} all null")


def dq_fx_key_uniqueness(final_exchange_df: DataFrame,
                         keys: Sequence[str] = ("basecurrencycode", "exchangeratedate"),
                         rule_name: str = "fx_join_keys_unique") -> DQRuleResult:
    missing = [k for k in keys if k not in final_exchange_df.columns]
    if missing:
        return DQRuleResult(rule_name, False, 0, f"Missing FX cols: {missing}")

    failed = _count(final_exchange_df.groupBy([F.col(k) for k in keys]).count().filter(F.col("count") > 1))
    return DQRuleResult(rule_name, failed == 0, failed, f"FX duplicates on keys {list(keys)}")


def dq_no_row_explosion(before_df: DataFrame,
                        after_df: DataFrame,
                        rule_name: str = "no_row_explosion_after_fx_join") -> DQRuleResult:
    before = _count(before_df)
    after = _count(after_df)
    passed = (before == after)
    details = f"before={before}, after={after}"
    return DQRuleResult(rule_name, passed, 0 if passed else (after - before), details)


def check_fx_join_integrity(before_claims_df: DataFrame,
                            after_enriched_df: DataFrame,
                            final_exchange_df: Optional[DataFrame] = None,
                            strict: bool = True) -> Dict[str, DQRuleResult]:
    """Run FX-related integrity checks.

    Parameters
    ----------
    before_claims_df : DataFrame
        Claims DF before joining to FX.
    after_enriched_df : DataFrame
        Claims DF after joining to FX.
    final_exchange_df : DataFrame, optional
        The FX table used for join. If supplied, uniqueness checks are applied.
    strict : bool
        If True, raises Exception when any rule fails.

    Returns
    -------
    Dict[str, DQRuleResult]
    """
    results: Dict[str, DQRuleResult] = {}

    r1 = dq_no_row_explosion(before_claims_df, after_enriched_df)
    results[r1.name] = r1

    if final_exchange_df is not None:
        r2 = dq_fx_key_uniqueness(final_exchange_df)
        results[r2.name] = r2

    if strict:
        failed = [r for r in results.values() if not r.passed]
        if failed:
            msg = "FX DQ FAILED: " + "; ".join([f"{r.name} ({r.details})" for r in failed])
            raise Exception(msg)

    return results


def run_triangle_report_dq(final_df: DataFrame,
                           strict: bool = True,
                           allow_negative_lag: bool = False) -> Dict[str, DQRuleResult]:
    """Run DQ checks on the final triangle report output DF.

    Parameters
    ----------
    final_df : DataFrame
        Output DF matching the external table schema.
    strict : bool
        If True, raises Exception when any rule fails.
    allow_negative_lag : bool
        If True, allows negative paid_lag_value (e.g., -1). Otherwise fails.

    Returns
    -------
    Dict[str, DQRuleResult]
    """

    results: Dict[str, DQRuleResult] = {}

    # Critical NOT NULL
    critical = [
        "metric_type", "metric_value", "book_of_business",
        "legal_entity_id", "legal_entity_name",
        "incurred_period", "incurred_period_type",
        "paid_lag_value",
        "triangle_report_type", "triangle_report_type_val",
    ]
    r = dq_not_null(final_df, critical, "critical_fields_not_null")
    results[r.name] = r

    # Allowed values
    results.update({
        "triangle_report_type_allowed": dq_in_set(
            final_df,
            "triangle_report_type",
            ["Incurred to Received", "Incurred to Paid", "Received to Paid"],
            "triangle_report_type_allowed"
        ),
        "incurred_period_type_allowed": dq_in_set(
            final_df,
            "incurred_period_type",
            ["MONTH", "QUARTER", "YEAR"],
            "incurred_period_type_allowed"
        ),
        "metric_type_allowed": dq_in_set(
            final_df,
            "metric_type",
            ["Productcurrency", "Paymentcurrency"],
            "metric_type_allowed"
        ),
    })

    # metric_value currency code sanity
    results["metric_value_currency_code_format"] = dq_regex(
        final_df, "metric_value", r"^[A-Z]{3}$", "metric_value_currency_code_format"
    )

    # Period format by type
    results["incurred_period_format_by_type"] = dq_incurred_period_format(final_df)

    # Lag sanity
    results["paid_lag_sanity"] = dq_lag_sanity(
        final_df,
        allow_negative=allow_negative_lag,
        max_val=99,
        month_cap_59_to_99=True
    )

    # Amount sanity
    amt_cols = [
        "sum_amount_base_currency",
        "sum_amount_usd", "sum_amount_gbp", "sum_amount_euro", "sum_amount_chf"
    ]
    results["no_nan_in_amounts"] = dq_no_nan(final_df, amt_cols, "no_nan_in_amounts")

    # FX conversions present when base present
    results["fx_conversions_present_when_base_present"] = dq_fx_missing_conversions(final_df)

    # Partition/value consistency
    results["triangle_type_value_matches_partition"] = dq_triangle_partition_match(final_df)

    if strict:
        failed = [r for r in results.values() if not r.passed]
        if failed:
            msg = "DQ FAILED: " + "; ".join([f"{r.name} (failed_rows={r.failed_rows}, details={r.details})" for r in failed])
            raise Exception(msg)

    return results


def pretty_print_results(results: Dict[str, DQRuleResult]) -> None:
    """Print a simple DQ summary to logs."""
    for name, r in results.items():
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {name} | failed_rows={r.failed_rows} | {r.details}")

