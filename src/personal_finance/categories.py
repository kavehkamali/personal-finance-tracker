from __future__ import annotations

import html
import json
import re
from pathlib import Path

from personal_finance.config import CATEGORY_RULES_JSON, SETTINGS_DIR
from personal_finance.merchant_aliases import resolve_canonical_merchant


AUTO_VALUE = "Auto"

CATEGORY_OPTIONS = [
    "Groceries",
    "Dining & Coffee",
    "Transportation",
    "Fuel",
    "Marketplace",
    "Shopping",
    "Subscriptions",
    "Utilities & Bills",
    "Housing",
    "Health & Pharmacy",
    "Travel",
    "Home",
    "Kids & Family",
    "Entertainment",
    "Business",
    "Education",
    "Gifts & Charity",
    "Fees",
    "Internal Transfer",
    "Income",
    "Refund",
    "Other",
]

NECESSITY_OPTIONS = [
    AUTO_VALUE,
    "Essential",
    "Fixed Bills",
    "Discretionary",
    "Child & Family",
    "Business",
    "Savings & Transfers",
    "Income",
    "Refund",
]

BENEFICIARY_OPTIONS = [
    AUTO_VALUE,
    "Shared",
    "Soren",
    "Faniya",
    "Kaveh",
]

DEFAULT_CATEGORY_RULES = [
    {"keyword": "farm boy", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "sobeys", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "loblaws", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "costco", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "heeva fine foods", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "mellat fine foods", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "freshco", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "instacart", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "act*town", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "vicentina", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "t&t", "category": "Groceries", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "starbucks", "category": "Dining & Coffee", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "mcdonald", "category": "Dining & Coffee", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "tim hortons", "category": "Dining & Coffee", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "panera", "category": "Dining & Coffee", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "pizza", "category": "Dining & Coffee", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "dominos", "category": "Dining & Coffee", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "ritual", "category": "Dining & Coffee", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "what a bagel", "category": "Dining & Coffee", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "forno cultura", "category": "Dining & Coffee", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "uber", "category": "Transportation", "necessity": "Essential", "beneficiary": AUTO_VALUE},
    {"keyword": "presto", "category": "Transportation", "necessity": "Essential", "beneficiary": AUTO_VALUE},
    {"keyword": "407etr", "category": "Transportation", "necessity": "Essential", "beneficiary": AUTO_VALUE},
    {"keyword": "parking", "category": "Transportation", "necessity": "Essential", "beneficiary": AUTO_VALUE},
    {"keyword": "bike share", "category": "Transportation", "necessity": "Essential", "beneficiary": AUTO_VALUE},
    {"keyword": "esso", "category": "Fuel", "necessity": "Essential", "beneficiary": AUTO_VALUE},
    {"keyword": "shell", "category": "Fuel", "necessity": "Essential", "beneficiary": AUTO_VALUE},
    {"keyword": "petro", "category": "Fuel", "necessity": "Essential", "beneficiary": AUTO_VALUE},
    {"keyword": "amzn", "category": "Marketplace", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "amazon", "category": "Marketplace", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "roots", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "marshalls", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "winners", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "zwilling", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "sporting life", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "long & mcquade", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "staples", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "hm", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "geox", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "arcteryx", "category": "Shopping", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "shoppers drug mart", "category": "Health & Pharmacy", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "videotron", "category": "Utilities & Bills", "necessity": "Fixed Bills", "beneficiary": "Shared"},
    {"keyword": "paymentus", "category": "Utilities & Bills", "necessity": "Fixed Bills", "beneficiary": "Shared"},
    {"keyword": "reliance", "category": "Utilities & Bills", "necessity": "Fixed Bills", "beneficiary": "Shared"},
    {"keyword": "hydro", "category": "Utilities & Bills", "necessity": "Fixed Bills", "beneficiary": "Shared"},
    {"keyword": "corporation of", "category": "Utilities & Bills", "necessity": "Fixed Bills", "beneficiary": "Shared"},
    {"keyword": "city of", "category": "Utilities & Bills", "necessity": "Fixed Bills", "beneficiary": "Shared"},
    {"keyword": "mortgage", "category": "Housing", "necessity": "Fixed Bills", "beneficiary": "Shared"},
    {"keyword": "rent", "category": "Housing", "necessity": "Fixed Bills", "beneficiary": "Shared"},
    {"keyword": "property tax", "category": "Housing", "necessity": "Fixed Bills", "beneficiary": "Shared"},
    {"keyword": "home depot", "category": "Home", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "tree valley garden", "category": "Home", "necessity": "Essential", "beneficiary": "Shared"},
    {"keyword": "flighthub", "category": "Travel", "necessity": "Discretionary", "beneficiary": "Shared"},
    {"keyword": "emirates", "category": "Travel", "necessity": "Discretionary", "beneficiary": "Shared"},
    {"keyword": "hotel", "category": "Travel", "necessity": "Discretionary", "beneficiary": "Shared"},
    {"keyword": "airbnb", "category": "Travel", "necessity": "Discretionary", "beneficiary": "Shared"},
    {"keyword": "admtoronto", "category": "Travel", "necessity": "Discretionary", "beneficiary": "Shared"},
    {"keyword": "tiger kicks", "category": "Kids & Family", "necessity": "Child & Family", "beneficiary": "Soren"},
    {"keyword": "space center", "category": "Kids & Family", "necessity": "Child & Family", "beneficiary": "Soren"},
    {"keyword": "wonderland", "category": "Kids & Family", "necessity": "Child & Family", "beneficiary": "Soren"},
    {"keyword": "souris mini", "category": "Kids & Family", "necessity": "Child & Family", "beneficiary": "Soren"},
    {"keyword": "childrens", "category": "Kids & Family", "necessity": "Child & Family", "beneficiary": "Soren"},
    {"keyword": "brain", "category": "Kids & Family", "necessity": "Child & Family", "beneficiary": "Soren"},
    {"keyword": "netflix", "category": "Subscriptions", "necessity": "Discretionary", "beneficiary": "Shared"},
    {"keyword": "spotify", "category": "Subscriptions", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "audible", "category": "Subscriptions", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "google one", "category": "Subscriptions", "necessity": "Discretionary", "beneficiary": "Shared"},
    {"keyword": "apple.com/bill", "category": "Subscriptions", "necessity": "Discretionary", "beneficiary": AUTO_VALUE},
    {"keyword": "openai", "category": "Business", "necessity": "Business", "beneficiary": "Kaveh"},
    {"keyword": "ownr", "category": "Business", "necessity": "Business", "beneficiary": "Kaveh"},
    {"keyword": "minute book", "category": "Business", "necessity": "Business", "beneficiary": "Kaveh"},
    {"keyword": "envoy business", "category": "Business", "necessity": "Business", "beneficiary": "Kaveh"},
    {"keyword": "service fee", "category": "Fees", "necessity": "Fixed Bills", "beneficiary": AUTO_VALUE},
    {"keyword": "annual fee", "category": "Fees", "necessity": "Fixed Bills", "beneficiary": AUTO_VALUE},
]

TRANSFER_KEYWORDS = (
    "payment - thank you",
    "paiement - merci",
    "credit card payment",
    "bill payment",
    "paymentus",
    "e-transfer",
    "etransfer",
    "transfer",
    "autopay",
)

REFUND_KEYWORDS = (
    "refund",
    "reversal",
    "return",
    "credit voucher",
)

INCOME_KEYWORDS = (
    "payroll",
    "salary",
    "deposit",
    "cashback",
    "rebate",
    "interest paid",
)

FEE_KEYWORDS = (
    "interest charge",
    "annual fee",
    "late fee",
    "service fee",
)


def normalize_text(value: str) -> str:
    text = html.unescape(str(value or "")).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_merchant(description: str) -> str:
    raw = normalize_text(description)
    canonical = resolve_canonical_merchant(raw)
    if canonical:
        return canonical
    clean = raw.upper()
    clean = re.sub(r"\b\d{10,}\b", "", clean)
    clean = re.sub(r"\b\d{3}-\d{3}-\d{4}\b", "", clean)
    clean = re.sub(r"\s+", " ", clean).strip(" -")
    return clean.title() or "Unknown Merchant"


def default_category_rules() -> list[dict[str, str]]:
    return [dict(rule) for rule in DEFAULT_CATEGORY_RULES]


def _normalize_rule(rule: dict[str, str]) -> dict[str, str] | None:
    keyword = normalize_text(rule.get("keyword", "")).lower()
    category = normalize_text(rule.get("category", "Other"))
    necessity = normalize_text(rule.get("necessity", AUTO_VALUE)) or AUTO_VALUE
    beneficiary = normalize_text(rule.get("beneficiary", AUTO_VALUE)) or AUTO_VALUE

    if not keyword:
        return None
    if category not in CATEGORY_OPTIONS:
        category = "Other"
    if necessity not in NECESSITY_OPTIONS:
        necessity = AUTO_VALUE
    if beneficiary not in BENEFICIARY_OPTIONS:
        beneficiary = AUTO_VALUE
    return {
        "keyword": keyword,
        "category": category,
        "necessity": necessity,
        "beneficiary": beneficiary,
    }


def load_category_rules() -> list[dict[str, str]]:
    SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
    if not CATEGORY_RULES_JSON.exists():
        rules = default_category_rules()
        CATEGORY_RULES_JSON.write_text(json.dumps(rules, indent=2), encoding="utf-8")
        return rules

    loaded = json.loads(CATEGORY_RULES_JSON.read_text(encoding="utf-8"))
    normalized = [_normalize_rule(rule) for rule in loaded]
    rules = [rule for rule in normalized if rule]
    if not rules:
        rules = default_category_rules()
        CATEGORY_RULES_JSON.write_text(json.dumps(rules, indent=2), encoding="utf-8")
    return rules


def save_category_rules(rules: list[dict[str, str]]) -> list[dict[str, str]]:
    SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
    normalized = [_normalize_rule(rule) for rule in rules]
    cleaned = [rule for rule in normalized if rule]
    if not cleaned:
        cleaned = default_category_rules()
    CATEGORY_RULES_JSON.write_text(json.dumps(cleaned, indent=2), encoding="utf-8")
    return cleaned


def _match_rule(text: str, rules: list[dict[str, str]]) -> dict[str, str] | None:
    for rule in rules:
        if rule["keyword"] in text:
            return rule
    return None


def _infer_necessity_from_category(category: str) -> str:
    mapping = {
        "Groceries": "Essential",
        "Transportation": "Essential",
        "Fuel": "Essential",
        "Health & Pharmacy": "Essential",
        "Home": "Essential",
        "Utilities & Bills": "Fixed Bills",
        "Housing": "Fixed Bills",
        "Kids & Family": "Child & Family",
        "Business": "Business",
        "Dining & Coffee": "Discretionary",
        "Marketplace": "Discretionary",
        "Shopping": "Discretionary",
        "Subscriptions": "Discretionary",
        "Travel": "Discretionary",
        "Entertainment": "Discretionary",
        "Gifts & Charity": "Discretionary",
        "Education": "Essential",
        "Other": "Discretionary",
    }
    return mapping.get(category, "Discretionary")


def _infer_beneficiary(owner: str, category: str) -> str:
    owner_lower = normalize_text(owner).lower()
    if category == "Kids & Family":
        return "Soren"
    if category in {"Groceries", "Utilities & Bills", "Housing", "Home", "Travel", "Health & Pharmacy"}:
        return "Shared"
    if "faniya" in owner_lower:
        return "Faniya"
    if "kaveh" in owner_lower:
        return "Kaveh"
    return "Shared"


def infer_flow_type(description: str, amount: float, statement_type: str) -> str:
    text = normalize_text(description).lower()

    if any(keyword in text for keyword in TRANSFER_KEYWORDS):
        return "Internal Transfer"
    if any(keyword in text for keyword in REFUND_KEYWORDS):
        return "Refund"
    if any(keyword in text for keyword in INCOME_KEYWORDS):
        return "Income"
    if any(keyword in text for keyword in FEE_KEYWORDS):
        return "Fees"

    if statement_type == "credit_card":
        return "Expense" if amount > 0 else "Credit"

    return "Expense" if amount < 0 else "Income"


def classify_transaction(
    description: str,
    merchant: str,
    owner: str,
    flow_type: str,
    rules: list[dict[str, str]] | None = None,
) -> dict[str, str]:
    applied_rules = rules or load_category_rules()
    text = f"{normalize_text(description).lower()} {normalize_text(merchant).lower()}".strip()

    if flow_type == "Internal Transfer":
        return {
            "category": "Internal Transfer",
            "necessity": "Savings & Transfers",
            "beneficiary": "Shared",
            "matched_keyword": "internal transfer",
        }
    if flow_type == "Refund":
        return {"category": "Refund", "necessity": "Refund", "beneficiary": _infer_beneficiary(owner, "Other"), "matched_keyword": ""}
    if flow_type == "Income":
        return {"category": "Income", "necessity": "Income", "beneficiary": "Shared", "matched_keyword": ""}
    if flow_type == "Fees":
        return {"category": "Fees", "necessity": "Fixed Bills", "beneficiary": _infer_beneficiary(owner, "Other"), "matched_keyword": ""}

    rule = _match_rule(text, applied_rules)
    category = rule["category"] if rule else "Other"
    necessity = rule["necessity"] if rule and rule["necessity"] != AUTO_VALUE else _infer_necessity_from_category(category)
    beneficiary = rule["beneficiary"] if rule and rule["beneficiary"] != AUTO_VALUE else _infer_beneficiary(owner, category)
    return {
        "category": category,
        "necessity": necessity,
        "beneficiary": beneficiary,
        "matched_keyword": rule["keyword"] if rule else "",
    }
