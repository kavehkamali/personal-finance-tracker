#!/usr/bin/env python3
"""
Build statistics from statement_audit output, using only full account-months.

Run ``scripts/statement_audit.py`` first. This script reads its reconciliation
report and the per-statement CSVs, then summarizes cash flow by account/month,
by account, and by month.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


SMART_CREDIT_CATEGORY_RULES: list[tuple[str, tuple[str, ...]]] = [
    (
        "Travel & Vacation",
        (
            "dcl reservations",
            "flighthub",
            "avion*air",
            "fallsview",
            "wc*fallsview",
            "hotel",
            "air canada",
            "airbnb",
            "expedia",
        ),
    ),
    (
        "Kids Education & Activities",
        (
            "brain power",
            "brainpowers",
            "kids den",
            "steamoji",
            "strategy games",
            "spelling bee",
            "math kangaroo",
            "chesskid",
            "inspire learner",
            "saliwan fight club",
            "tiger kicks",
            "eventbrite/richmondhil",
            "act*townrichmondhill",
            "townrichmondhill",
        ),
    ),
    (
        "Groceries",
        (
            "costco wholesale",
            "costco wholesal",
            "www costco",
            "walmart",
            "sobeys",
            "farm boy",
            "loblaws",
            "longo",
            "freshco",
            "freshpro",
            "coppa",
            "heeva",
            "mellat",
            "persian meat",
            "super arzon",
            "halal fine",
            "greek market",
            "natures emporium",
            "marche",
            "vicentina",
            "t&t",
        ),
    ),
    (
        "Dining & Coffee",
        (
            "starbucks",
            "tim hortons",
            "mcdonald",
            "stacked pancake",
            "state & main",
            "niku japanese",
            "donerlicious",
            "haida sandwich",
            "thai express",
            "a&w",
            "yogen fruz",
            "bread & roses",
            "yazdi pastry",
            "eisenbergs",
            "pur & simple",
            "onroute",
            "what a bagel",
            "mr. pretzels",
            "uncle tetsu",
            "second cup",
            "ritual-the bagel",
            "ritual-impact kitchen",
            "aashland",
            "lcbo",
        ),
    ),
    (
        "Fuel & Auto",
        (
            "shell",
            "esso",
            "petro-canada",
            "circle k",
            "parking",
            "407etr",
            "presto",
            "uber",
        ),
    ),
    (
        "Utilities & Insurance",
        (
            "bell mobility",
            "reliance home",
            "belair",
            "ass/ins",
            "insurance",
            "hydro",
            "paymentus",
            "videotron",
        ),
    ),
    (
        "Shopping & Personal",
        (
            "sephora",
            "lululemon",
            "browns shoes",
            "softmoc",
            "geox",
            "skechers",
            "gap.com",
            "jack jones",
            "calvin klein",
            "b r factory",
            "columbia sportswear",
            "arcteryx",
            "roots",
            "marshalls",
            "homesense",
            "hm ca",
            "long & mcquade",
            "finch gift shop",
            "*rfbt",
            "dollarama",
            "souris mini",
            "upper canada iso",
        ),
    ),
    (
        "Home, Pets & Household",
        (
            "home depot",
            "canadian tire",
            "sherwin-williams",
            "canyon hill animal",
            "petsmart",
            "pet valu",
            "ikea",
        ),
    ),
    (
        "Health & Pharmacy",
        (
            "shoppers drug mart",
            "sdm ",
            "jean coutu",
            "astra pharmacy",
            "southlake regional",
            "rexall",
        ),
    ),
    (
        "Entertainment",
        (
            "ticketmaster",
            "trapped escape",
            "merlin entertainments",
            "legoland",
            "funvilla",
            "imagine cinemas",
            "cineplex",
            "the jump city",
            "musee residences",
            "ldc toronto",
            "fv indoor waterpark",
            "wonderland foods",
            "wonderland merchand",
            "space center",
            "meridian arts centre",
        ),
    ),
    (
        "Subscriptions & Digital",
        (
            "apple.com/bill",
            "paddle.net",
            "netflix",
            "spotify",
            "audible",
            "google",
            "openai",
            "facebooktec",
        ),
    ),
    (
        "Marketplace / Amazon",
        (
            "amazon",
            "amzn",
        ),
    ),
    (
        "Government, IDs & Fees",
        (
            "service canada",
            "online police records",
            "annual renewal",
            "service fee",
            "annual fee",
        ),
    ),
]


OWN_ACCOUNT_TRANSFER_MARKERS = (
    "online banking transfer",
    "online transfer to deposit account",
    "credit card payment",
    "payment - thank you",
    "visa payment",
    "mc payment",
    "mastercard payment",
)

OWN_PERSON_TRANSFER_MARKERS = (
    "online transfer sent -",
    "kaveh kamali",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _money(value: float) -> float:
    return round(float(value), 2)


def _load_statement_rows(reconcile: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for _, row in reconcile.iterrows():
        csv_path = Path(str(row.get("csv_path") or ""))
        if not csv_path.is_file():
            continue
        try:
            df = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            continue
        if df.empty or "amount" not in df.columns:
            continue

        df = df.copy()
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        df["account_key"] = row["account_key"]
        df["account_kind"] = row["account_kind"]
        df["account_last4"] = str(row["account_last4"])
        df["statement_month"] = row["statement_month"]
        df["source_statement"] = row["filename"]
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _summarize_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                *group_cols,
                "transaction_count",
                "cash_in",
                "cash_out",
                "net_cash_flow",
                "absolute_activity",
            ]
        )

    work = df.copy()
    work["cash_in_component"] = work["amount"].clip(lower=0)
    work["cash_out_component"] = (-work["amount"].clip(upper=0))
    grouped = (
        work.groupby(group_cols, dropna=False)
        .agg(
            transaction_count=("amount", "size"),
            cash_in=("cash_in_component", "sum"),
            cash_out=("cash_out_component", "sum"),
            net_cash_flow=("amount", "sum"),
        )
        .reset_index()
    )
    grouped["absolute_activity"] = grouped["cash_in"] + grouped["cash_out"]
    for col in ("cash_in", "cash_out", "net_cash_flow", "absolute_activity"):
        grouped[col] = grouped[col].map(_money)
    return grouped


def _smart_credit_category(description: object) -> str:
    text = str(description or "").lower()
    text = " ".join(text.split())
    if "e-transfer sent" in text:
        if any(token in text for token in ("piano", "stem project", "soccer", "yip music", "tutor", "music")):
            return "Kids Education & Activities"
        return "Personal E-Transfers"
    if "cra-rev" in text or "tax" in text:
        return "Taxes"
    if "mortgage payment" in text or "loan payment" in text or "term loan" in text or "loan interest" in text:
        return "Mortgage, Loans & Debt"
    if "insurance" in text or "belair" in text or "rbcins" in text or "all life" in text:
        return "Utilities & Insurance"
    if "bill payment" in text or "utility bill" in text or "misc payment alectra" in text or "enbridge" in text:
        return "Utilities & Insurance"
    if "monthly fee" in text or "nsf item fee" in text or "overdraft interest" in text:
        return "Bank Fees & Interest"
    if "atm withdrawal" in text:
        return "Cash Withdrawal"
    for category, needles in SMART_CREDIT_CATEGORY_RULES:
        if any(needle in text for needle in needles):
            return category
    return "Other / Review"


def _is_own_account_transfer(description: object) -> bool:
    text = str(description or "").lower()
    text = " ".join(text.split())
    if any(marker in text for marker in OWN_ACCOUNT_TRANSFER_MARKERS):
        return True
    if "online transfer sent" in text and any(marker in text for marker in OWN_PERSON_TRANSFER_MARKERS):
        return True
    return False


def _is_internal_inflow(description: object) -> bool:
    text = str(description or "").lower()
    text = " ".join(text.split())
    return (
        "online banking transfer" in text
        or "online transfer received" in text
        or "online transfer to deposit account" in text
        or "kaveh kamali" in text
    )


def _income_source(description: object) -> str:
    text = str(description or "").lower()
    text = " ".join(text.split())
    if "payroll deposit lg electronics" in text:
        return "LG Electronics"
    if "payroll deposit unitytechcan-os" in text or "payroll deposit unitytech" in text:
        return "EL / UnityTechCAN-OS"
    if "payroll deposit" in text:
        return "Other Payroll"
    if "ei canada" in text:
        return "EI Canada"
    if "refund" in text or "rebate" in text or "item returned" in text:
        return "Refunds & Rebates"
    if "cra-rev" in text:
        return "CRA / Tax"
    if "medavie" in text:
        return "Benefits / Medavie"
    if "deposit interest" in text:
        return "Interest"
    if "e-transfer" in text or "autodeposit" in text:
        return "External E-Transfer"
    if _is_internal_inflow(text):
        return "Internal Transfer In"
    return "Other Inflow"


def _income_check(rows: pd.DataFrame, complete_months: set[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if rows.empty or not complete_months:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    inflow = rows[
        rows["account_kind"].eq("chequing")
        & rows["statement_month"].astype(str).isin(complete_months)
        & (rows["amount"] > 0)
    ].copy()
    if inflow.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    inflow["income_source"] = inflow["description"].map(_income_source)
    inflow["is_internal_inflow"] = inflow["description"].map(_is_internal_inflow)
    inflow["is_lg_payroll"] = inflow["income_source"].eq("LG Electronics")
    inflow["is_el_payroll"] = inflow["income_source"].eq("EL / UnityTechCAN-OS")

    rows_out: list[dict[str, object]] = []
    for month in sorted(complete_months):
        grp = inflow[inflow["statement_month"].astype(str).eq(month)]
        total_cash_in = float(grp["amount"].sum())
        internal_cash_in = float(grp.loc[grp["is_internal_inflow"], "amount"].sum())
        external_cash_in = total_cash_in - internal_cash_in
        lg = float(grp.loc[grp["is_lg_payroll"], "amount"].sum())
        el = float(grp.loc[grp["is_el_payroll"], "amount"].sum())
        payroll = lg + el
        rows_out.append(
            {
                "statement_month": month,
                "total_bank_cash_in": _money(total_cash_in),
                "excluded_internal_cash_in": _money(internal_cash_in),
                "external_cash_in": _money(external_cash_in),
                "lg_payroll": _money(lg),
                "el_unitytech_payroll": _money(el),
                "lg_plus_el_payroll": _money(payroll),
                "payroll_vs_total_cash_in_diff": _money(total_cash_in - payroll),
                "payroll_vs_external_cash_in_diff": _money(external_cash_in - payroll),
            }
        )

    by_month = pd.DataFrame(rows_out)
    totals = {
        "statement_month": "TOTAL",
        "total_bank_cash_in": _money(by_month["total_bank_cash_in"].sum()),
        "excluded_internal_cash_in": _money(by_month["excluded_internal_cash_in"].sum()),
        "external_cash_in": _money(by_month["external_cash_in"].sum()),
        "lg_payroll": _money(by_month["lg_payroll"].sum()),
        "el_unitytech_payroll": _money(by_month["el_unitytech_payroll"].sum()),
        "lg_plus_el_payroll": _money(by_month["lg_plus_el_payroll"].sum()),
        "payroll_vs_total_cash_in_diff": _money(by_month["payroll_vs_total_cash_in_diff"].sum()),
        "payroll_vs_external_cash_in_diff": _money(by_month["payroll_vs_external_cash_in_diff"].sum()),
    }
    by_month_with_total = pd.concat([by_month, pd.DataFrame([totals])], ignore_index=True)

    by_source = (
        inflow.groupby(["statement_month", "income_source"], dropna=False)
        .agg(amount=("amount", "sum"), transaction_count=("amount", "size"))
        .reset_index()
        .sort_values(["statement_month", "amount"], ascending=[True, False])
    )
    by_source["amount"] = by_source["amount"].map(_money)

    line_items = inflow[
        [
            "statement_month",
            "account_key",
            "transaction_date",
            "description",
            "amount",
            "income_source",
            "is_internal_inflow",
            "source_statement",
        ]
    ].sort_values(["statement_month", "amount"], ascending=[True, False])
    line_items["amount"] = line_items["amount"].map(_money)

    return by_month_with_total, by_source, line_items


def _spend_rows_for_complete_months(rows: pd.DataFrame, complete_months: set[str]) -> pd.DataFrame:
    if rows.empty or not complete_months:
        return pd.DataFrame()

    work = rows[rows["statement_month"].astype(str).isin(complete_months)].copy()
    if work.empty:
        return pd.DataFrame()

    credit = work[
        work["account_kind"].isin(["visa", "mastercard"])
        & (work["amount"] > 0)
    ].copy()
    credit["expense_amount"] = credit["amount"]
    credit["spend_source"] = "credit_card"
    credit["excluded_as_internal"] = False

    debit = work[
        work["account_kind"].eq("chequing")
        & (work["amount"] < 0)
    ].copy()
    if not debit.empty:
        debit["expense_amount"] = -debit["amount"]
        debit["spend_source"] = "debit_bank"
        debit["excluded_as_internal"] = debit["description"].map(_is_own_account_transfer)
        debit = debit[~debit["excluded_as_internal"]].copy()

    combined = pd.concat([credit, debit], ignore_index=True) if not debit.empty else credit
    if combined.empty:
        return combined

    combined["smart_category"] = combined["description"].map(_smart_credit_category)
    return combined


def _category_stats(rows: pd.DataFrame, complete_months: set[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if rows.empty or not complete_months:
        empty_summary = pd.DataFrame(
            columns=[
                "category",
                "total_expense",
                "share_of_total",
                "transaction_count",
                "avg_transaction",
                "months_count",
                "avg_monthly_expense",
            ]
        )
        return empty_summary, pd.DataFrame(), pd.DataFrame()

    expenses = rows[
        rows["account_kind"].isin(["visa", "mastercard"])
        & rows["statement_month"].astype(str).isin(complete_months)
        & (rows["amount"] > 0)
    ].copy()
    if expenses.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    expenses["smart_category"] = expenses["description"].map(_smart_credit_category)
    total = float(expenses["amount"].sum())
    month_count = len(complete_months)

    category_summary = (
        expenses.groupby("smart_category", dropna=False)
        .agg(
            total_expense=("amount", "sum"),
            transaction_count=("amount", "size"),
            avg_transaction=("amount", "mean"),
            months_count=("statement_month", "nunique"),
        )
        .reset_index()
        .rename(columns={"smart_category": "category"})
    )
    category_summary["share_of_total"] = category_summary["total_expense"] / total
    category_summary["avg_monthly_expense"] = category_summary["total_expense"] / max(1, month_count)
    for col in ("total_expense", "avg_transaction", "avg_monthly_expense"):
        category_summary[col] = category_summary[col].map(_money)
    category_summary["share_of_total"] = category_summary["share_of_total"].map(lambda x: round(float(x) * 100, 2))
    category_summary = category_summary.sort_values("total_expense", ascending=False)

    category_by_month = (
        expenses.groupby(["statement_month", "smart_category"], dropna=False)
        .agg(expense=("amount", "sum"), transaction_count=("amount", "size"))
        .reset_index()
        .rename(columns={"smart_category": "category"})
        .sort_values(["statement_month", "expense"], ascending=[True, False])
    )
    category_by_month["expense"] = category_by_month["expense"].map(_money)

    line_items = expenses[
        [
            "statement_month",
            "account_key",
            "transaction_date",
            "posting_date",
            "description",
            "amount",
            "smart_category",
            "source_statement",
        ]
    ].rename(columns={"smart_category": "category"})
    line_items = line_items.sort_values(["statement_month", "category", "amount"], ascending=[True, True, False])
    line_items["amount"] = line_items["amount"].map(_money)

    return category_summary, category_by_month, line_items


def _spend_category_stats(spend: pd.DataFrame, month_count: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if spend.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    total = float(spend["expense_amount"].sum())
    summary = (
        spend.groupby("smart_category", dropna=False)
        .agg(
            total_expense=("expense_amount", "sum"),
            transaction_count=("expense_amount", "size"),
            avg_transaction=("expense_amount", "mean"),
            months_count=("statement_month", "nunique"),
        )
        .reset_index()
        .rename(columns={"smart_category": "category"})
    )
    summary["share_of_total"] = summary["total_expense"] / total
    summary["avg_monthly_expense"] = summary["total_expense"] / max(1, month_count)
    for col in ("total_expense", "avg_transaction", "avg_monthly_expense"):
        summary[col] = summary[col].map(_money)
    summary["share_of_total"] = summary["share_of_total"].map(lambda x: round(float(x) * 100, 2))
    summary = summary.sort_values("total_expense", ascending=False)

    by_month = (
        spend.groupby(["statement_month", "smart_category"], dropna=False)
        .agg(expense=("expense_amount", "sum"), transaction_count=("expense_amount", "size"))
        .reset_index()
        .rename(columns={"smart_category": "category"})
        .sort_values(["statement_month", "expense"], ascending=[True, False])
    )
    by_month["expense"] = by_month["expense"].map(_money)

    by_source = (
        spend.groupby(["spend_source", "smart_category"], dropna=False)
        .agg(expense=("expense_amount", "sum"), transaction_count=("expense_amount", "size"))
        .reset_index()
        .rename(columns={"smart_category": "category"})
        .sort_values(["spend_source", "expense"], ascending=[True, False])
    )
    by_source["expense"] = by_source["expense"].map(_money)

    line_items = spend[
        [
            "statement_month",
            "account_key",
            "spend_source",
            "transaction_date",
            "posting_date",
            "description",
            "expense_amount",
            "smart_category",
            "source_statement",
        ]
    ].rename(columns={"expense_amount": "amount", "smart_category": "category"})
    line_items = line_items.sort_values(["statement_month", "category", "amount"], ascending=[True, True, False])
    line_items["amount"] = line_items["amount"].map(_money)
    return summary, by_month, by_source, line_items


def build_analytics(audit_dir: Path, output_dir: Path) -> dict[str, object]:
    reconcile_path = audit_dir / "statement_reconciliation.csv"
    coverage_path = audit_dir / "account_month_coverage.csv"
    if not reconcile_path.is_file() or not coverage_path.is_file():
        raise FileNotFoundError(
            f"Missing audit reports in {audit_dir}. Run scripts/statement_audit.py first."
        )

    reconcile = pd.read_csv(reconcile_path)
    coverage = pd.read_csv(coverage_path)

    full_keys = coverage.loc[
        coverage["status"].eq("full"),
        ["account_key", "month"],
    ].rename(columns={"month": "statement_month"})

    full_reconcile = reconcile.merge(
        full_keys,
        on=["account_key", "statement_month"],
        how="inner",
    )
    rows = _load_statement_rows(full_reconcile)

    output_dir.mkdir(parents=True, exist_ok=True)

    account_month = _summarize_group(
        rows,
        ["account_key", "account_kind", "account_last4", "statement_month"],
    ).sort_values(["account_key", "statement_month"])

    account_summary = pd.DataFrame()
    if not account_month.empty:
        account_summary = (
            account_month.groupby(["account_key", "account_kind", "account_last4"], dropna=False)
            .agg(
                full_months=("statement_month", "nunique"),
                transaction_count=("transaction_count", "sum"),
                total_cash_in=("cash_in", "sum"),
                total_cash_out=("cash_out", "sum"),
                total_net_cash_flow=("net_cash_flow", "sum"),
                total_activity=("absolute_activity", "sum"),
            )
            .reset_index()
        )
        for source, dest in (
            ("total_cash_in", "avg_monthly_cash_in"),
            ("total_cash_out", "avg_monthly_cash_out"),
            ("total_net_cash_flow", "avg_monthly_net_cash_flow"),
            ("total_activity", "avg_monthly_activity"),
        ):
            account_summary[dest] = account_summary[source] / account_summary["full_months"].clip(lower=1)
        for col in (
            "total_cash_in",
            "total_cash_out",
            "total_net_cash_flow",
            "total_activity",
            "avg_monthly_cash_in",
            "avg_monthly_cash_out",
            "avg_monthly_net_cash_flow",
            "avg_monthly_activity",
        ):
            account_summary[col] = account_summary[col].map(_money)
        account_summary = account_summary.sort_values(["account_kind", "account_key"])

    monthly_total = _summarize_group(rows, ["statement_month"]).sort_values("statement_month")
    if not monthly_total.empty:
        monthly_accounts = account_month.groupby("statement_month")["account_key"].nunique().reset_index(name="full_account_count")
        monthly_total = monthly_total.merge(monthly_accounts, on="statement_month", how="left")

    credit_cards = account_month[account_month["account_kind"].isin(["visa", "mastercard"])].copy()
    credit_accounts = sorted(credit_cards["account_key"].dropna().unique().tolist())
    expected_credit_accounts = len(credit_accounts)
    if credit_cards.empty:
        credit_expenses_by_month = pd.DataFrame(
            columns=[
                "statement_month",
                "credit_card_expense",
                "full_credit_card_count",
                "expected_credit_card_count",
                "all_credit_cards_present",
            ]
        )
    else:
        credit_expenses_by_month = (
            credit_cards.groupby("statement_month", dropna=False)
            .agg(
                credit_card_expense=("cash_in", "sum"),
                credit_card_payments_credits=("cash_out", "sum"),
                net_credit_card_activity=("net_cash_flow", "sum"),
                credit_card_transactions=("transaction_count", "sum"),
                full_credit_card_count=("account_key", "nunique"),
            )
            .reset_index()
            .sort_values("statement_month")
        )
        credit_expenses_by_month["expected_credit_card_count"] = expected_credit_accounts
        credit_expenses_by_month["all_credit_cards_present"] = (
            credit_expenses_by_month["full_credit_card_count"] == expected_credit_accounts
        )
        for col in ("credit_card_expense", "credit_card_payments_credits", "net_credit_card_activity"):
            credit_expenses_by_month[col] = credit_expenses_by_month[col].map(_money)

    complete_credit_months = (
        credit_expenses_by_month[credit_expenses_by_month["all_credit_cards_present"]]
        if not credit_expenses_by_month.empty
        else credit_expenses_by_month
    )
    complete_credit_month_set = set(complete_credit_months["statement_month"].astype(str).tolist()) if not complete_credit_months.empty else set()
    credit_total = _money(complete_credit_months["credit_card_expense"].sum()) if not complete_credit_months.empty else 0.0
    credit_month_count = int(len(complete_credit_months))
    credit_summary = {
        "basis": "Visa/MasterCard purchases & debits only; complete months require every discovered credit card account.",
        "credit_card_accounts": ", ".join(credit_accounts),
        "complete_credit_card_months": credit_month_count,
        "total_credit_card_expense_complete_months": credit_total,
        "avg_monthly_credit_card_expense_complete_months": _money(credit_total / max(1, credit_month_count)),
        "months_included": ", ".join(complete_credit_months["statement_month"].astype(str).tolist())
        if not complete_credit_months.empty
        else "",
        "months_excluded_as_partial": ", ".join(
            credit_expenses_by_month.loc[
                ~credit_expenses_by_month["all_credit_cards_present"],
                "statement_month",
            ].astype(str).tolist()
        )
        if not credit_expenses_by_month.empty
        else "",
    }

    category_summary, category_by_month, categorized_line_items = _category_stats(rows, complete_credit_month_set)
    all_spend = _spend_rows_for_complete_months(rows, complete_credit_month_set)
    all_spend_summary, all_spend_by_month, all_spend_by_source, all_spend_line_items = _spend_category_stats(
        all_spend,
        len(complete_credit_month_set),
    )
    all_spend_total = _money(all_spend["expense_amount"].sum()) if not all_spend.empty else 0.0
    all_spend_debit_total = (
        _money(all_spend.loc[all_spend["spend_source"].eq("debit_bank"), "expense_amount"].sum())
        if not all_spend.empty
        else 0.0
    )
    all_spend_credit_total = (
        _money(all_spend.loc[all_spend["spend_source"].eq("credit_card"), "expense_amount"].sum())
        if not all_spend.empty
        else 0.0
    )
    all_spend_summary_row = {
        "basis": (
            "Complete credit-card months only; credit-card purchases plus full chequing debit outflows. "
            "Obvious own-account transfers are excluded; e-transfers to tutors/classes are included."
        ),
        "months_included": ", ".join(sorted(complete_credit_month_set)),
        "total_expense": all_spend_total,
        "credit_card_expense": all_spend_credit_total,
        "debit_bank_expense": all_spend_debit_total,
        "avg_monthly_total_expense": _money(all_spend_total / max(1, len(complete_credit_month_set))),
    }
    income_by_month, income_by_source, income_line_items = _income_check(rows, complete_credit_month_set)

    totals = {
        "full_account_months": int(len(account_month)),
        "full_accounts": int(account_month["account_key"].nunique()) if not account_month.empty else 0,
        "months_with_any_full_accounts": int(account_month["statement_month"].nunique()) if not account_month.empty else 0,
        "transaction_count": int(account_month["transaction_count"].sum()) if not account_month.empty else 0,
        "total_cash_in": _money(account_month["cash_in"].sum()) if not account_month.empty else 0.0,
        "total_cash_out": _money(account_month["cash_out"].sum()) if not account_month.empty else 0.0,
        "total_net_cash_flow": _money(account_month["net_cash_flow"].sum()) if not account_month.empty else 0.0,
    }
    month_count = max(1, int(totals["months_with_any_full_accounts"]))
    totals["avg_monthly_cash_in"] = _money(float(totals["total_cash_in"]) / month_count)
    totals["avg_monthly_cash_out"] = _money(float(totals["total_cash_out"]) / month_count)
    totals["avg_monthly_net_cash_flow"] = _money(float(totals["total_net_cash_flow"]) / month_count)

    account_month.to_csv(output_dir / "full_month_account_month_stats.csv", index=False)
    account_summary.to_csv(output_dir / "full_month_account_summary.csv", index=False)
    monthly_total.to_csv(output_dir / "full_month_total_by_month.csv", index=False)
    pd.DataFrame([totals]).to_csv(output_dir / "full_month_totals.csv", index=False)
    credit_expenses_by_month.to_csv(output_dir / "credit_card_expenses_by_month.csv", index=False)
    pd.DataFrame([credit_summary]).to_csv(output_dir / "credit_card_expense_summary.csv", index=False)
    category_summary.to_csv(output_dir / "credit_card_category_summary.csv", index=False)
    category_by_month.to_csv(output_dir / "credit_card_category_by_month.csv", index=False)
    categorized_line_items.to_csv(output_dir / "credit_card_categorized_transactions.csv", index=False)
    all_spend_summary.to_csv(output_dir / "all_spend_category_summary.csv", index=False)
    all_spend_by_month.to_csv(output_dir / "all_spend_category_by_month.csv", index=False)
    all_spend_by_source.to_csv(output_dir / "all_spend_category_by_source.csv", index=False)
    all_spend_line_items.to_csv(output_dir / "all_spend_categorized_transactions.csv", index=False)
    pd.DataFrame([all_spend_summary_row]).to_csv(output_dir / "all_spend_summary.csv", index=False)
    income_by_month.to_csv(output_dir / "income_cash_in_check_by_month.csv", index=False)
    income_by_source.to_csv(output_dir / "income_cash_in_by_source.csv", index=False)
    income_line_items.to_csv(output_dir / "income_cash_in_transactions.csv", index=False)

    return {**totals, **credit_summary, **all_spend_summary_row}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--audit-dir",
        type=Path,
        default=Path("input_statements/statement_audit"),
        help="Directory created by scripts/statement_audit.py",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for analytics CSVs (default: <audit-dir>/analytics)",
    )
    args = parser.parse_args()

    root = _repo_root()
    for candidate in (root, root / "src"):
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))

    audit_dir = args.audit_dir.expanduser().resolve()
    output_dir = (args.output_dir or (audit_dir / "analytics")).expanduser().resolve()

    try:
        totals = build_analytics(audit_dir, output_dir)
    except Exception as exc:
        print(f"Analytics failed: {exc}", file=sys.stderr)
        return 1

    print(f"Analytics written to: {output_dir}")
    print(f"Full account-months used: {totals['full_account_months']}")
    print(f"Accounts represented: {totals['full_accounts']}")
    print(f"Months represented: {totals['months_with_any_full_accounts']}")
    print(f"Transactions used: {totals['transaction_count']}")
    print(f"Total cash in: {totals['total_cash_in']:.2f}")
    print(f"Total cash out: {totals['total_cash_out']:.2f}")
    print(f"Total net cash flow: {totals['total_net_cash_flow']:.2f}")
    print(f"Avg monthly cash in: {totals['avg_monthly_cash_in']:.2f}")
    print(f"Avg monthly cash out: {totals['avg_monthly_cash_out']:.2f}")
    print(f"Avg monthly net cash flow: {totals['avg_monthly_net_cash_flow']:.2f}")
    print(f"Credit card expense months included: {totals['months_included'] or 'none'}")
    print(f"Credit card expense months excluded as partial: {totals['months_excluded_as_partial'] or 'none'}")
    print(f"Total credit card expense: {totals['total_credit_card_expense_complete_months']:.2f}")
    print(f"Avg monthly credit card expense: {totals['avg_monthly_credit_card_expense_complete_months']:.2f}")
    print(f"All categorized spend total: {totals['total_expense']:.2f}")
    print(f"  credit cards: {totals['credit_card_expense']:.2f}")
    print(f"  debit bank: {totals['debit_bank_expense']:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
