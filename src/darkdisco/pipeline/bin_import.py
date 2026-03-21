"""BIN database import — CSV and PDF ingestion for BIN records.

Supports:
- CSV import (generic BIN databases, custom columns)
- PDF import (Visa/Mastercard BIN table publications via pdfplumber)
"""

from __future__ import annotations

import csv
import io
import logging
import re
from dataclasses import dataclass, field
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from darkdisco.common.models import BINRecord, CardBrand, CardType

logger = logging.getLogger(__name__)


def _create_sync_session() -> Session:
    """Create a sync SQLAlchemy session for BIN import operations."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from darkdisco.config import settings

    db_url = settings.database_url
    # Convert async URL to sync
    if db_url.startswith("postgresql+asyncpg"):
        db_url = db_url.replace("postgresql+asyncpg", "postgresql+psycopg2", 1)
    elif db_url.startswith("postgresql://"):
        pass  # already sync
    engine = create_engine(db_url, pool_pre_ping=True)
    return sessionmaker(bind=engine)()

# Map common brand names to enum values
_BRAND_MAP = {
    "visa": CardBrand.visa,
    "mastercard": CardBrand.mastercard,
    "master card": CardBrand.mastercard,
    "mc": CardBrand.mastercard,
    "amex": CardBrand.amex,
    "american express": CardBrand.amex,
    "discover": CardBrand.discover,
    "jcb": CardBrand.jcb,
    "unionpay": CardBrand.unionpay,
    "union pay": CardBrand.unionpay,
    "china unionpay": CardBrand.unionpay,
    "cup": CardBrand.unionpay,
    "diners": CardBrand.diners,
    "diners club": CardBrand.diners,
    "maestro": CardBrand.maestro,
}

_TYPE_MAP = {
    "credit": CardType.credit,
    "debit": CardType.debit,
    "prepaid": CardType.prepaid,
    "charge": CardType.charge,
}

# Common CSV column name aliases
_COLUMN_ALIASES = {
    "bin": "bin_prefix",
    "iin": "bin_prefix",
    "bin_prefix": "bin_prefix",
    "prefix": "bin_prefix",
    "bin/iin": "bin_prefix",
    "range_start": "bin_range_start",
    "range_end": "bin_range_end",
    "bin_range_start": "bin_range_start",
    "bin_range_end": "bin_range_end",
    "issuer": "issuer_name",
    "issuer_name": "issuer_name",
    "bank": "issuer_name",
    "bank_name": "issuer_name",
    "issuing_bank": "issuer_name",
    "brand": "card_brand",
    "card_brand": "card_brand",
    "scheme": "card_brand",
    "network": "card_brand",
    "type": "card_type",
    "card_type": "card_type",
    "funding": "card_type",
    "level": "card_level",
    "card_level": "card_level",
    "product": "card_level",
    "sub_brand": "card_level",
    "country": "country_name",
    "country_name": "country_name",
    "country_code": "country_code",
    "iso_country": "country_code",
    "alpha2": "country_code",
    "alpha_2": "country_code",
    "url": "bank_url",
    "bank_url": "bank_url",
    "website": "bank_url",
    "phone": "bank_phone",
    "bank_phone": "bank_phone",
}


@dataclass
class ImportResult:
    """Result of a BIN import operation."""
    imported: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)
    source: str = ""


def _normalize_brand(value: str | None) -> CardBrand | None:
    if not value:
        return None
    return _BRAND_MAP.get(value.strip().lower(), CardBrand.other)


def _normalize_type(value: str | None) -> CardType | None:
    if not value:
        return None
    return _TYPE_MAP.get(value.strip().lower(), CardType.unknown)


def import_csv(file_content: str | bytes, source_label: str = "csv", session: Session | None = None) -> ImportResult:
    """Import BIN records from CSV content.

    Auto-detects column mapping from headers. Supports various CSV formats
    from open BIN databases.

    If session is provided, the caller is responsible for commit/close.
    Otherwise creates and manages its own session.
    """
    result = ImportResult(source=source_label)
    owns_session = session is None

    if isinstance(file_content, bytes):
        file_content = file_content.decode("utf-8-sig")  # handle BOM

    reader = csv.DictReader(io.StringIO(file_content))
    if not reader.fieldnames:
        result.errors.append("CSV has no headers")
        return result

    # Map CSV columns to our fields
    col_map = {}
    for csv_col in reader.fieldnames:
        normalized = csv_col.strip().lower().replace(" ", "_")
        if normalized in _COLUMN_ALIASES:
            col_map[csv_col] = _COLUMN_ALIASES[normalized]

    if "bin_prefix" not in col_map.values():
        result.errors.append(
            f"No BIN/IIN column found. Headers: {reader.fieldnames}. "
            f"Expected one of: bin, iin, prefix, bin_prefix"
        )
        return result

    # Invert to: our_field -> csv_col
    field_to_csv = {}
    for csv_col, our_field in col_map.items():
        if our_field not in field_to_csv:
            field_to_csv[our_field] = csv_col

    if owns_session:
        session = _create_sync_session()

    try:
        batch = []
        for i, row in enumerate(reader):
            try:
                bin_prefix = row.get(field_to_csv.get("bin_prefix", ""), "").strip()
                if not bin_prefix or not bin_prefix.isdigit():
                    result.skipped += 1
                    continue

                # Normalize to 6 or 8 digits
                if len(bin_prefix) < 6:
                    result.skipped += 1
                    continue
                if len(bin_prefix) > 8:
                    bin_prefix = bin_prefix[:8]

                record = BINRecord(
                    id=str(uuid4()),
                    bin_prefix=bin_prefix,
                    bin_range_start=row.get(field_to_csv.get("bin_range_start", ""), "").strip() or None,
                    bin_range_end=row.get(field_to_csv.get("bin_range_end", ""), "").strip() or None,
                    issuer_name=row.get(field_to_csv.get("issuer_name", ""), "").strip() or None,
                    card_brand=_normalize_brand(row.get(field_to_csv.get("card_brand", ""), "")),
                    card_type=_normalize_type(row.get(field_to_csv.get("card_type", ""), "")),
                    card_level=row.get(field_to_csv.get("card_level", ""), "").strip() or None,
                    country_code=row.get(field_to_csv.get("country_code", ""), "").strip()[:3] or None,
                    country_name=row.get(field_to_csv.get("country_name", ""), "").strip() or None,
                    bank_url=row.get(field_to_csv.get("bank_url", ""), "").strip() or None,
                    bank_phone=row.get(field_to_csv.get("bank_phone", ""), "").strip() or None,
                    source=source_label,
                )
                batch.append(record)

                if len(batch) >= 1000:
                    _upsert_batch(session, batch, result)
                    batch = []

            except Exception as e:
                result.errors.append(f"Row {i + 1}: {e}")
                if len(result.errors) > 100:
                    result.errors.append("Too many errors, stopping")
                    break

        if batch:
            _upsert_batch(session, batch, result)

        if owns_session:
            session.commit()
    except Exception as e:
        if owns_session:
            session.rollback()
        result.errors.append(f"Import failed: {e}")
    finally:
        if owns_session:
            session.close()

    logger.info(
        "BIN CSV import (%s): %d imported, %d updated, %d skipped, %d errors",
        source_label, result.imported, result.updated, result.skipped, len(result.errors),
    )
    return result


def _upsert_batch(session: Session, batch: list[BINRecord], result: ImportResult):
    """Insert or update a batch of BIN records."""
    for record in batch:
        existing = session.execute(
            select(BINRecord).where(
                BINRecord.bin_prefix == record.bin_prefix,
                BINRecord.source == record.source,
            ).limit(1)
        ).scalar_one_or_none()

        if existing:
            # Update existing record
            for attr in ["issuer_name", "card_brand", "card_type", "card_level",
                         "country_code", "country_name", "bank_url", "bank_phone",
                         "bin_range_start", "bin_range_end"]:
                new_val = getattr(record, attr)
                if new_val is not None:
                    setattr(existing, attr, new_val)
            result.updated += 1
        else:
            session.add(record)
            result.imported += 1


def import_pdf(file_content: bytes, source_label: str = "pdf", session: Session | None = None) -> ImportResult:
    """Import BIN records from a PDF (Visa/Mastercard BIN table publication).

    Uses pdfplumber to extract tables from each page, then parses rows
    for BIN prefix/range and issuer information.
    """
    result = ImportResult(source=source_label)
    owns_session = session is None

    try:
        import pdfplumber
    except ImportError:
        result.errors.append("pdfplumber not installed. Install with: pip install pdfplumber")
        return result

    if owns_session:
        session = _create_sync_session()

    try:
        pdf = pdfplumber.open(io.BytesIO(file_content))
        batch = []

        for page_num, page in enumerate(pdf.pages):
            tables = page.extract_tables()

            for table in tables:
                if not table or len(table) < 2:
                    continue

                # First row is likely headers
                headers = [str(h).strip().lower() if h else "" for h in table[0]]

                # Try to identify BIN-related columns
                bin_col = _find_col(headers, ["bin", "iin", "prefix", "bin/iin", "range"])
                issuer_col = _find_col(headers, ["issuer", "bank", "institution", "issuing"])
                brand_col = _find_col(headers, ["brand", "scheme", "network", "product"])
                country_col = _find_col(headers, ["country"])

                if bin_col is None:
                    continue

                for row_idx, row in enumerate(table[1:], start=2):
                    try:
                        if not row or bin_col >= len(row):
                            continue

                        cell = str(row[bin_col]).strip() if row[bin_col] else ""
                        if not cell:
                            continue

                        # Parse BIN prefix or range
                        prefix, range_start, range_end = _parse_bin_cell(cell)
                        if not prefix:
                            continue

                        issuer = str(row[issuer_col]).strip() if issuer_col is not None and issuer_col < len(row) and row[issuer_col] else None
                        brand_str = str(row[brand_col]).strip() if brand_col is not None and brand_col < len(row) and row[brand_col] else None
                        country = str(row[country_col]).strip() if country_col is not None and country_col < len(row) and row[country_col] else None

                        # Detect brand from source label if not in table
                        if not brand_str:
                            if "visa" in source_label.lower():
                                brand_str = "visa"
                            elif "master" in source_label.lower():
                                brand_str = "mastercard"

                        record = BINRecord(
                            id=str(uuid4()),
                            bin_prefix=prefix,
                            bin_range_start=range_start,
                            bin_range_end=range_end,
                            issuer_name=issuer,
                            card_brand=_normalize_brand(brand_str),
                            country_name=country,
                            source=source_label,
                        )
                        batch.append(record)

                        if len(batch) >= 1000:
                            _upsert_batch(session, batch, result)
                            batch = []

                    except Exception as e:
                        result.errors.append(f"Page {page_num + 1}, row {row_idx}: {e}")

        if batch:
            _upsert_batch(session, batch, result)

        if owns_session:
            session.commit()
        pdf.close()

    except Exception as e:
        if owns_session:
            session.rollback()
        result.errors.append(f"PDF import failed: {e}")
    finally:
        if owns_session:
            session.close()

    logger.info(
        "BIN PDF import (%s): %d imported, %d updated, %d skipped, %d errors",
        source_label, result.imported, result.updated, result.skipped, len(result.errors),
    )
    return result


def _find_col(headers: list[str], keywords: list[str]) -> int | None:
    """Find the column index that matches any of the keywords."""
    for i, h in enumerate(headers):
        for kw in keywords:
            if kw in h:
                return i
    return None


def _parse_bin_cell(cell: str) -> tuple[str | None, str | None, str | None]:
    """Parse a BIN cell which may be a single prefix or a range.

    Examples:
        "453201" -> ("453201", None, None)
        "400000-400099" -> ("400000", "400000", "400099")
        "4000 00 - 4000 99" -> ("400000", "400000", "400099")
    """
    # Remove spaces within numbers
    cell = re.sub(r"(\d)\s+(\d)", r"\1\2", cell)

    # Range format: 400000-400099
    range_match = re.match(r"(\d{6,8})\s*[-–—]\s*(\d{6,8})", cell)
    if range_match:
        start = range_match.group(1)
        end = range_match.group(2)
        prefix = start[:min(len(start), 8)]
        return prefix, start, end

    # Single prefix
    prefix_match = re.match(r"(\d{6,8})", cell)
    if prefix_match:
        prefix = prefix_match.group(1)
        return prefix, None, None

    return None, None, None
