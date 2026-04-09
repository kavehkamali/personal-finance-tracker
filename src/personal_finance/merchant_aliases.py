"""Map noisy bank/card merchant strings to a single display name."""

from __future__ import annotations

import html
import json
import re
from pathlib import Path

from personal_finance.config import MERCHANT_ALIASES_JSON, SETTINGS_DIR

# Longer "contains" strings are matched first so specific phrases beat generic ones.
_DEFAULT_ALIASES: list[tuple[str, str]] = [
    ("costco wholesale", "Costco"),
    ("costco gas", "Costco"),
    ("costco business", "Costco"),
    ("costco ca", "Costco"),
    ("costco", "Costco"),
    ("amzn mktp", "Amazon"),
    ("amazon marketplace", "Amazon"),
    ("amazon.com", "Amazon"),
    ("amazon.ca", "Amazon"),
    ("amazon pay", "Amazon"),
    ("amazon web", "Amazon"),
    ("amazon", "Amazon"),
    ("amzn", "Amazon"),
    ("walmart supercenter", "Walmart"),
    ("walmart", "Walmart"),
    ("uber eats", "Uber"),
    ("uber trip", "Uber"),
    ("ubertrip", "Uber"),
    ("uber canada", "Uber"),
    ("uber ca", "Uber"),
    ("uber*", "Uber"),
    ("lyft", "Lyft"),
    ("doordash", "DoorDash"),
    ("dd *", "DoorDash"),
    ("starbucks", "Starbucks"),
    ("mcdonald", "McDonald's"),
    ("tim hortons", "Tim Hortons"),
    ("tim horton", "Tim Hortons"),
    ("metro #", "Metro"),
    ("metro inc", "Metro"),
    ("metro ontario", "Metro"),
    ("loblaws", "Loblaws"),
    ("real canadian superstore", "Loblaws"),
    ("superstore", "Loblaws"),
    ("sobeys", "Sobeys"),
    ("farm boy", "Farm Boy"),
    ("instacart", "Instacart"),
    ("shoppers drug mart", "Shoppers Drug Mart"),
    ("shoppers dr", "Shoppers Drug Mart"),
    ("shell ", "Shell"),
    ("shell#", "Shell"),
    ("petro-canada", "Petro-Canada"),
    ("petro canada", "Petro-Canada"),
    ("esso ", "Esso"),
    ("canadian tire", "Canadian Tire"),
    ("home depot", "Home Depot"),
    ("home dep", "Home Depot"),
    ("winners", "Winners"),
    ("marshalls", "Marshalls"),
    ("sport chek", "Sport Chek"),
    ("best buy", "Best Buy"),
    ("netflix", "Netflix"),
    ("spotify", "Spotify"),
    ("apple.com/bill", "Apple"),
    ("apple store", "Apple"),
    ("google one", "Google"),
    ("paypal *", "PayPal"),
    ("paypal", "PayPal"),
]

_aliases_cache: list[tuple[str, str]] | None = None


def _normalize_whitespace(value: str) -> str:
    text = html.unescape(str(value or "")).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def merchant_haystack(description: str) -> str:
    """Lowercase string used for substring alias matching."""
    t = _normalize_whitespace(description).lower()
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"www\.[\w.]+\s*", " ", t)
    # Strip common card-present / gateway prefixes
    strip_prefixes = (
        "purchase ",
        "point of sale ",
        "pos ",
        "debit ",
        "visa ",
        "mastercard ",
        "mc ",
        "contactless ",
        "tap ",
        "recurring ",
        "preauthorized ",
        "pre-authorized ",
        "e-commerce ",
        "ecommerce ",
        "online payment ",
        "sq *",
        "sq*",
        "tst*",
        "sp *",
        "sp*",
    )
    changed = True
    while changed:
        changed = False
        for p in strip_prefixes:
            if t.startswith(p):
                t = t[len(p) :].lstrip()
                changed = True
    t = re.sub(r"\b\d{10,}\b", " ", t)
    t = re.sub(r"\b\d{3}-\d{3}-\d{4}\b", " ", t)
    t = re.sub(r"\s+#\s*\d+(\s+#\d+)*\s*$", "", t)
    t = re.sub(r"\s+store\s+#?\d+\s*$", "", t, flags=re.I)
    t = re.sub(r"\s+loc(ation)?\s*\d+\s*$", "", t, flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _parse_alias_file(raw: list[object]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        needle = _normalize_whitespace(str(item.get("contains", ""))).lower()
        label = _normalize_whitespace(str(item.get("label", "")))
        if needle and label:
            out.append((needle, label))
    return out


def load_merchant_aliases(*, force: bool = False) -> list[tuple[str, str]]:
    global _aliases_cache
    if _aliases_cache is not None and not force:
        return _aliases_cache

    SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
    user: list[tuple[str, str]] = []
    if not MERCHANT_ALIASES_JSON.exists():
        MERCHANT_ALIASES_JSON.write_text(
            json.dumps(
                [
                    {"contains": "costco wholesale", "label": "Costco"},
                    {"contains": "costco", "label": "Costco"},
                    {"contains": "amzn mktp", "label": "Amazon"},
                    {"contains": "amzn", "label": "Amazon"},
                ],
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    try:
        loaded = json.loads(MERCHANT_ALIASES_JSON.read_text(encoding="utf-8"))
        if isinstance(loaded, list):
            user = _parse_alias_file(loaded)
    except (json.JSONDecodeError, OSError):
        user = []

    merged = user + list(_DEFAULT_ALIASES)
    merged.sort(key=lambda pair: len(pair[0]), reverse=True)
    _aliases_cache = merged
    return _aliases_cache


def invalidate_merchant_aliases_cache() -> None:
    global _aliases_cache
    _aliases_cache = None


def resolve_canonical_merchant(description: str) -> str | None:
    hay = merchant_haystack(description)
    if not hay:
        return None
    for needle, label in load_merchant_aliases():
        if needle in hay:
            return label
    return None
