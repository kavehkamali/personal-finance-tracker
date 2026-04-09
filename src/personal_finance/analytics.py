from __future__ import annotations

from typing import Any

import pandas as pd


def _series_to_records(series: pd.Series, value_name: str) -> list[dict[str, Any]]:
    if series.empty:
        return []
    return [{"label": str(index), value_name: round(float(value), 2)} for index, value in series.items()]


def _frame_to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    out = frame.copy()
    for column in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[column]):
            out[column] = out[column].dt.strftime("%Y-%m-%d")
        elif pd.api.types.is_float_dtype(out[column]):
            out[column] = out[column].round(2)
    return out.to_dict(orient="records")


def apply_filters(
    df: pd.DataFrame,
    owner: str | None = None,
    account: str | None = None,
    category: str | None = None,
    necessity: str | None = None,
    beneficiary: str | None = None,
    month: str | None = None,
    include_internal: bool = False,
) -> pd.DataFrame:
    filtered = df.copy()
    if owner:
        filtered = filtered[filtered["owner"] == owner]
    if account:
        filtered = filtered[filtered["account_label"] == account]
    if category:
        filtered = filtered[filtered["category"] == category]
    if necessity:
        filtered = filtered[filtered["necessity"] == necessity]
    if beneficiary:
        filtered = filtered[filtered["beneficiary"] == beneficiary]
    if month:
        filtered = filtered[filtered["month"].astype(str) == str(month)]
    if not include_internal and "is_internal" in filtered.columns:
        filtered = filtered[~filtered["is_internal"].fillna(False)]
    return filtered


def _build_owner_beneficiary_sankey(expense_rows: pd.DataFrame) -> dict[str, Any]:
    if expense_rows.empty:
        return {"nodes": [], "links": []}

    owner_nodes = sorted(expense_rows["owner"].dropna().unique().tolist())
    beneficiary_nodes = sorted(expense_rows["beneficiary"].dropna().unique().tolist())
    category_nodes = sorted(expense_rows["category"].dropna().unique().tolist())
    all_nodes = owner_nodes + beneficiary_nodes + category_nodes
    node_index = {name: idx for idx, name in enumerate(all_nodes)}

    links = []
    owner_beneficiary = expense_rows.groupby(["owner", "beneficiary"])["expense_amount"].sum().reset_index()
    for row in owner_beneficiary.itertuples():
        links.append(
            {
                "source": node_index[row.owner],
                "target": node_index[row.beneficiary],
                "value": round(float(row.expense_amount), 2),
            }
        )

    grouped = expense_rows.groupby(["beneficiary", "category"])["expense_amount"].sum().reset_index()
    for row in grouped.itertuples():
        links.append(
            {
                "source": node_index[row.beneficiary],
                "target": node_index[row.category],
                "value": round(float(row.expense_amount), 2),
            }
        )
    return {"nodes": all_nodes, "links": links}


def _build_waterfall(expense_rows: pd.DataFrame) -> list[dict[str, Any]]:
    if expense_rows.empty:
        return []

    by_category = expense_rows.groupby("category")["expense_amount"].sum().sort_values(ascending=False)
    top = by_category.head(6)
    records = [{"label": category, "value": round(float(value), 2)} for category, value in top.items()]
    other_value = float(by_category.iloc[6:].sum()) if len(by_category) > 6 else 0.0
    if other_value > 0:
        records.append({"label": "Other Categories", "value": round(other_value, 2)})
    return records


def build_dashboard_payload(
    df: pd.DataFrame,
    meta: dict[str, Any],
    owner: str | None = None,
    account: str | None = None,
    category: str | None = None,
    necessity: str | None = None,
    beneficiary: str | None = None,
    month: str | None = None,
    include_internal: bool = False,
) -> dict[str, Any]:
    if df.empty:
        return {
            "meta": meta,
            "overview": {
                "transaction_count": 0,
                "statement_count": 0,
                "account_count": 0,
                "owner_count": 0,
                "expense_total": 0.0,
                "cash_in_total": 0.0,
                "internal_total": 0.0,
                "date_start": None,
                "date_end": None,
                "matched_internal_pairs": 0,
                "unmatched_internal_rows": 0,
                "top_category": None,
                "top_category_amount": 0.0,
                "avg_expense_transaction": 0.0,
            },
            "filters": {"owners": [], "accounts": [], "categories": [], "months": [], "necessities": [], "beneficiaries": []},
            "monthly_expenses": [],
            "daily_expenses": [],
            "category_breakdown": [],
            "necessity_breakdown": [],
            "beneficiary_breakdown": [],
            "monthly_category_breakdown": [],
            "monthly_necessity_breakdown": [],
            "monthly_beneficiary_breakdown": [],
            "merchant_breakdown": [],
            "owner_breakdown": [],
            "account_breakdown": [],
            "owner_beneficiary_breakdown": [],
            "flow_breakdown": [],
            "weekday_breakdown": [],
            "treemap_breakdown": [],
            "sunburst_breakdown": [],
            "waterfall_breakdown": [],
            "matched_transfers": [],
            "unmatched_internal": [],
            "recent_transactions": [],
            "statement_breakdown": [],
            "sankey": {"nodes": [], "links": []},
        }

    base = df.copy()
    scope = apply_filters(
        base,
        owner=owner,
        account=account,
        category=category,
        necessity=necessity,
        beneficiary=beneficiary,
        month=month,
        include_internal=True,
    )
    filtered = apply_filters(
        base,
        owner=owner,
        account=account,
        category=category,
        necessity=necessity,
        beneficiary=beneficiary,
        month=month,
        include_internal=include_internal,
    )
    expense_rows = filtered[filtered["expense_amount"] > 0].copy()

    overview = {
        "transaction_count": int(len(filtered)),
        "statement_count": int(scope["statement_id"].nunique()),
        "account_count": int(scope["account_label"].nunique()),
        "owner_count": int(scope["owner"].nunique()),
        "expense_total": round(float(expense_rows["expense_amount"].sum()), 2),
        "cash_in_total": round(float(scope.loc[scope["cash_flow_amount"] > 0, "cash_flow_amount"].sum()), 2),
        "internal_total": round(float(scope.loc[scope["is_internal"], "cash_flow_amount"].abs().sum() / 2), 2),
        "date_start": scope["transaction_date"].min().strftime("%Y-%m-%d") if scope["transaction_date"].notna().any() else None,
        "date_end": scope["transaction_date"].max().strftime("%Y-%m-%d") if scope["transaction_date"].notna().any() else None,
        "matched_internal_pairs": int(scope.loc[scope["internal_match_status"] == "Matched", "match_id"].nunique()),
        "unmatched_internal_rows": int(scope["internal_match_status"].eq("Unmatched").sum()),
    }

    filters = {
        "owners": sorted(base["owner"].dropna().unique().tolist()),
        "accounts": sorted(base["account_label"].dropna().unique().tolist()),
        "categories": sorted(base["category"].dropna().unique().tolist()),
        "necessities": sorted(base["necessity"].dropna().unique().tolist()),
        "beneficiaries": sorted(base["beneficiary"].dropna().unique().tolist()),
        "months": sorted(base["month"].dropna().unique().tolist()),
    }

    monthly_expenses = (
        expense_rows.groupby("month")["expense_amount"].sum().sort_index().reset_index().rename(columns={"expense_amount": "value"})
    )
    daily_expenses = (
        expense_rows.groupby(expense_rows["transaction_date"].dt.strftime("%Y-%m-%d"))["expense_amount"]
        .sum()
        .reset_index()
        .rename(columns={"transaction_date": "date", "expense_amount": "value"})
    )
    category_breakdown = (
        expense_rows.groupby("category")["expense_amount"].sum().sort_values(ascending=False).reset_index().rename(columns={"expense_amount": "value"})
    )
    if category_breakdown.empty:
        overview["top_category"] = None
        overview["top_category_amount"] = 0.0
    else:
        overview["top_category"] = str(category_breakdown.iloc[0]["category"])
        overview["top_category_amount"] = round(float(category_breakdown.iloc[0]["value"]), 2)
    overview["avg_expense_transaction"] = (
        round(float(expense_rows["expense_amount"].mean()), 2) if not expense_rows.empty else 0.0
    )
    necessity_breakdown = (
        expense_rows.groupby("necessity")["expense_amount"].sum().sort_values(ascending=False).reset_index().rename(columns={"expense_amount": "value"})
    )
    beneficiary_breakdown = (
        expense_rows.groupby("beneficiary")["expense_amount"].sum().sort_values(ascending=False).reset_index().rename(columns={"expense_amount": "value"})
    )
    monthly_category_breakdown = (
        expense_rows.groupby(["month", "category"])["expense_amount"]
        .sum()
        .reset_index()
        .rename(columns={"expense_amount": "value"})
    )
    monthly_necessity_breakdown = (
        expense_rows.groupby(["month", "necessity"])["expense_amount"]
        .sum()
        .reset_index()
        .rename(columns={"expense_amount": "value"})
    )
    monthly_beneficiary_breakdown = (
        expense_rows.groupby(["month", "beneficiary"])["expense_amount"]
        .sum()
        .reset_index()
        .rename(columns={"expense_amount": "value"})
    )
    merchant_breakdown = (
        expense_rows.groupby("merchant")["expense_amount"].sum().sort_values(ascending=False).head(12).reset_index().rename(columns={"expense_amount": "value"})
    )
    owner_breakdown = (
        expense_rows.groupby("owner")["expense_amount"].sum().sort_values(ascending=False).reset_index().rename(columns={"expense_amount": "value"})
    )
    account_breakdown = (
        expense_rows.groupby("account_label")["expense_amount"].sum().sort_values(ascending=False).reset_index().rename(columns={"expense_amount": "value"})
    )
    owner_beneficiary_breakdown = (
        expense_rows.groupby(["owner", "beneficiary"])["expense_amount"]
        .sum()
        .reset_index()
        .rename(columns={"expense_amount": "value"})
    )
    flow_breakdown = (
        filtered.groupby("flow_type")["cash_flow_amount"].sum().sort_values(ascending=False).reset_index().rename(columns={"cash_flow_amount": "value"})
    )
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_breakdown = (
        expense_rows.groupby("weekday")["expense_amount"].sum().reindex(weekday_order, fill_value=0).reset_index().rename(columns={"expense_amount": "value"})
    )
    statement_breakdown = (
        filtered.groupby(["statement_id", "account_label"])["expense_amount"]
        .agg(["sum", "count"])
        .reset_index()
        .rename(columns={"sum": "expense_total", "count": "transaction_count"})
        .sort_values("expense_total", ascending=False)
    )
    treemap_breakdown = (
        expense_rows.groupby(["necessity", "category", "merchant"])["expense_amount"]
        .sum()
        .reset_index()
        .sort_values("expense_amount", ascending=False)
        .head(80)
        .rename(columns={"expense_amount": "value"})
    )
    sunburst_breakdown = (
        expense_rows.groupby(["beneficiary", "category", "merchant"])["expense_amount"]
        .sum()
        .reset_index()
        .sort_values("expense_amount", ascending=False)
        .head(100)
        .rename(columns={"expense_amount": "value"})
    )

    transfer_pairs = scope[scope["internal_match_status"] == "Matched"].copy()
    matched_transfers = []
    for match_id, rows in transfer_pairs.groupby("match_id"):
        rows = rows.sort_values("transaction_date")
        if len(rows) < 2:
            continue
        first = rows.iloc[0]
        second = rows.iloc[1]
        matched_transfers.append(
            {
                "match_id": match_id,
                "date_left": first["transaction_date"].strftime("%Y-%m-%d") if pd.notna(first["transaction_date"]) else None,
                "date_right": second["transaction_date"].strftime("%Y-%m-%d") if pd.notna(second["transaction_date"]) else None,
                "account_left": first["account_label"],
                "account_right": second["account_label"],
                "description_left": first["description"],
                "description_right": second["description"],
                "amount": round(abs(float(first["cash_flow_amount"])), 2),
            }
        )

    unmatched_internal = (
        scope[scope["internal_match_status"] == "Unmatched"][
            ["transaction_date", "account_label", "owner", "description", "cash_flow_amount"]
        ]
        .sort_values("transaction_date", ascending=False)
        .rename(columns={"cash_flow_amount": "value"})
        .head(25)
    )

    recent_transactions = (
        filtered[
            [
                "transaction_date",
                "owner",
                "account_label",
                "merchant",
                "description",
                "category",
                "necessity",
                "beneficiary",
                "flow_type",
                "amount",
                "expense_amount",
                "internal_match_status",
            ]
        ]
        .sort_values("transaction_date", ascending=False)
        .head(80)
    )

    return {
        "meta": meta,
        "overview": overview,
        "filters": filters,
        "monthly_expenses": _frame_to_records(monthly_expenses.rename(columns={"month": "label"})),
        "daily_expenses": _frame_to_records(daily_expenses.rename(columns={"date": "label"})),
        "category_breakdown": _frame_to_records(category_breakdown.rename(columns={"category": "label"})),
        "necessity_breakdown": _frame_to_records(necessity_breakdown.rename(columns={"necessity": "label"})),
        "beneficiary_breakdown": _frame_to_records(beneficiary_breakdown.rename(columns={"beneficiary": "label"})),
        "monthly_category_breakdown": _frame_to_records(monthly_category_breakdown),
        "monthly_necessity_breakdown": _frame_to_records(monthly_necessity_breakdown),
        "monthly_beneficiary_breakdown": _frame_to_records(monthly_beneficiary_breakdown),
        "merchant_breakdown": _frame_to_records(merchant_breakdown.rename(columns={"merchant": "label"})),
        "owner_breakdown": _frame_to_records(owner_breakdown.rename(columns={"owner": "label"})),
        "account_breakdown": _frame_to_records(account_breakdown.rename(columns={"account_label": "label"})),
        "owner_beneficiary_breakdown": _frame_to_records(owner_beneficiary_breakdown),
        "flow_breakdown": _frame_to_records(flow_breakdown.rename(columns={"flow_type": "label"})),
        "weekday_breakdown": _frame_to_records(weekday_breakdown.rename(columns={"weekday": "label"})),
        "treemap_breakdown": _frame_to_records(treemap_breakdown),
        "sunburst_breakdown": _frame_to_records(sunburst_breakdown),
        "waterfall_breakdown": _build_waterfall(expense_rows),
        "matched_transfers": matched_transfers,
        "unmatched_internal": _frame_to_records(unmatched_internal),
        "recent_transactions": _frame_to_records(recent_transactions),
        "statement_breakdown": _frame_to_records(statement_breakdown),
        "sankey": _build_owner_beneficiary_sankey(expense_rows),
    }
