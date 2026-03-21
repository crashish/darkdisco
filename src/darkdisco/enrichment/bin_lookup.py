"""BIN (Bank Identification Number) lookup and enrichment.

Extracts BIN prefixes from content (card numbers, dumps) and looks up
issuer information from the bin_records table.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.orm import Session

from darkdisco.common.models import BINRecord

logger = logging.getLogger(__name__)

# Patterns that match potential card numbers (13-19 digits) or BIN prefixes (6-8 digits)
# Captures common CC dump formats: 4532015112830366, 4532-0151-1283-0366, etc.
_CARD_NUMBER_RE = re.compile(
    r"(?<!\d)"  # not preceded by digit
    r"(\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{1,7})"  # card number with optional separators
    r"(?!\d)"  # not followed by digit
)

# Standalone BIN prefix (6-8 digits not part of a longer number)
_BIN_PREFIX_RE = re.compile(
    r"(?<!\d)(\d{6,8})(?!\d)"
)

# CC dump format: track data with BIN visible
# e.g. 4532015112830366=2512101xxxxx or |4532015112830366|
_DUMP_RE = re.compile(
    r"(\d{13,19})\s*[=|]\s*\d{4}"
)


@dataclass
class BINLookupResult:
    """Result of looking up a single BIN prefix."""
    bin_prefix: str
    found: bool = False
    issuer_name: str | None = None
    card_brand: str | None = None
    card_type: str | None = None
    card_level: str | None = None
    country_code: str | None = None
    country_name: str | None = None
    bank_url: str | None = None
    bank_phone: str | None = None


@dataclass
class BINEnrichmentResult:
    """Result of BIN enrichment for a finding's content."""
    bins_found: list[BINLookupResult] = field(default_factory=list)
    unique_issuers: list[str] = field(default_factory=list)
    unique_brands: list[str] = field(default_factory=list)
    card_count: int = 0

    def to_dict(self) -> dict:
        """Serialize for storage in finding metadata."""
        return {
            "bins_found": [
                {k: v for k, v in b.__dict__.items() if v is not None}
                for b in self.bins_found
            ],
            "unique_issuers": self.unique_issuers,
            "unique_brands": self.unique_brands,
            "card_count": self.card_count,
        }


def extract_bin_prefixes(content: str) -> list[str]:
    """Extract potential BIN prefixes from content.

    Returns deduplicated list of 6-8 digit prefixes found in the content.
    """
    prefixes: set[str] = set()

    # Extract from full card numbers
    for m in _CARD_NUMBER_RE.finditer(content):
        digits = re.sub(r"[\s\-]", "", m.group(1))
        if len(digits) >= 13:
            prefixes.add(digits[:8])
            prefixes.add(digits[:6])

    # Extract from dump format
    for m in _DUMP_RE.finditer(content):
        digits = m.group(1)
        prefixes.add(digits[:8])
        prefixes.add(digits[:6])

    # Extract standalone BIN prefixes
    for m in _BIN_PREFIX_RE.finditer(content):
        prefix = m.group(1)
        if _is_plausible_bin(prefix):
            prefixes.add(prefix)

    return sorted(prefixes)


def _is_plausible_bin(prefix: str) -> bool:
    """Quick heuristic: does this look like a real BIN prefix?

    Filters out common false positives like dates (202603), timestamps, etc.
    """
    if len(prefix) < 6:
        return False

    first_digit = prefix[0]
    # Major card network ranges: 2 (MC), 3 (Amex/JCB/Diners), 4 (Visa), 5 (MC), 6 (Discover/UnionPay)
    if first_digit not in "234569":
        return False

    # Filter out date-like patterns (YYYYMM, YYMMDD)
    if len(prefix) == 6:
        year_prefix = int(prefix[:4])
        if 1990 <= year_prefix <= 2030:
            return False

    # Filter out expiry-date-like patterns from track data (YYMM followed by service code)
    if len(prefix) >= 6:
        yy = int(prefix[:2])
        mm = int(prefix[2:4])
        if 20 <= yy <= 35 and 1 <= mm <= 12:
            return False

    return True


def lookup_bin(prefix: str, session: Session) -> BINLookupResult:
    """Look up a single BIN prefix in the database.

    Tries exact prefix match first, then falls back to range-based lookup.
    Tries 8-digit first, then 6-digit for legacy compatibility.
    """
    result = BINLookupResult(bin_prefix=prefix)

    # Try exact prefix match
    record = session.execute(
        select(BINRecord).where(BINRecord.bin_prefix == prefix).limit(1)
    ).scalar_one_or_none()

    if not record and len(prefix) == 8:
        # Fall back to 6-digit prefix
        record = session.execute(
            select(BINRecord).where(BINRecord.bin_prefix == prefix[:6]).limit(1)
        ).scalar_one_or_none()

    if not record:
        # Try range-based lookup
        record = session.execute(
            select(BINRecord).where(
                BINRecord.bin_range_start.isnot(None),
                BINRecord.bin_range_end.isnot(None),
                BINRecord.bin_range_start <= prefix,
                BINRecord.bin_range_end >= prefix,
            ).limit(1)
        ).scalar_one_or_none()

    if record:
        result.found = True
        result.issuer_name = record.issuer_name
        result.card_brand = record.card_brand.value if record.card_brand else None
        result.card_type = record.card_type.value if record.card_type else None
        result.card_level = record.card_level
        result.country_code = record.country_code
        result.country_name = record.country_name
        result.bank_url = record.bank_url
        result.bank_phone = record.bank_phone

    return result


def enrich_bins(content: str, session: Session) -> BINEnrichmentResult:
    """Extract BIN prefixes from content and look up issuer information.

    Returns enrichment data suitable for storing in finding metadata.
    """
    result = BINEnrichmentResult()

    prefixes = extract_bin_prefixes(content)
    if not prefixes:
        return result

    # Count card numbers for stats
    card_numbers = set()
    for m in _CARD_NUMBER_RE.finditer(content):
        digits = re.sub(r"[\s\-]", "", m.group(1))
        if len(digits) >= 13:
            card_numbers.add(digits)
    for m in _DUMP_RE.finditer(content):
        card_numbers.add(m.group(1))
    result.card_count = len(card_numbers)

    # Look up each unique prefix (prefer 8-digit, skip 6-digit if 8-digit found same issuer)
    seen_issuers: set[str] = set()
    seen_brands: set[str] = set()
    seen_prefixes: set[str] = set()

    for prefix in prefixes:
        # Skip if we already looked up the 8-digit version of this 6-digit prefix
        if len(prefix) == 6 and any(p.startswith(prefix) and len(p) == 8 for p in seen_prefixes):
            continue

        lookup = lookup_bin(prefix, session)
        if lookup.found:
            result.bins_found.append(lookup)
            seen_prefixes.add(prefix)
            if lookup.issuer_name and lookup.issuer_name not in seen_issuers:
                seen_issuers.add(lookup.issuer_name)
                result.unique_issuers.append(lookup.issuer_name)
            if lookup.card_brand and lookup.card_brand not in seen_brands:
                seen_brands.add(lookup.card_brand)
                result.unique_brands.append(lookup.card_brand)

    if result.bins_found:
        logger.info(
            "BIN enrichment: found %d BINs, %d unique issuers, %d card numbers",
            len(result.bins_found),
            len(result.unique_issuers),
            result.card_count,
        )

    return result
