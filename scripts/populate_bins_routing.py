#!/usr/bin/env python3
"""Populate institution BIN ranges and routing numbers from public data.

Usage:
    # Update all institutions from built-in data:
    python scripts/populate_bins_routing.py

    # Load from a JSON file:
    python scripts/populate_bins_routing.py --file data/bins_routing.json

    # Dry-run (show what would change without writing):
    python scripts/populate_bins_routing.py --dry-run

The JSON file format is an array of objects:
[
  {
    "name": "Navy Federal Credit Union",
    "bin_ranges": ["489480", "489481", "414720"],
    "routing_numbers": ["256074974"]
  },
  ...
]

Matching is by institution name (case-insensitive). Each run is idempotent:
existing values are preserved, new values are merged, and watch terms are
created for any newly-added BIN or routing number.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from uuid import uuid4

# Allow running from repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from darkdisco.common.models import (
    Institution,
    WatchTerm,
    WatchTermType,
)
from darkdisco.config import settings


# ---------------------------------------------------------------------------
# Built-in BIN/routing data keyed by institution name
# ---------------------------------------------------------------------------
# Sourced from:
#   - Routing numbers: Federal Reserve E-Payments Routing Directory, FDIC/NCUA
#   - BIN ranges: Public IIN/BIN databases (representative prefixes)
# ---------------------------------------------------------------------------

def _builtin_data() -> list[dict]:
    """Return built-in BIN/routing data for all 100 institutions.

    Imported lazily from seed_institutions.py to avoid duplicating the data.
    """
    # Import the seed data lists
    parent = Path(__file__).resolve().parent
    sys.path.insert(0, str(parent))
    from seed_institutions import CREDIT_UNIONS, COMMUNITY_BANKS

    entries = []
    for inst in CREDIT_UNIONS + COMMUNITY_BANKS:
        entries.append({
            "name": inst["name"],
            "bin_ranges": inst.get("bin_ranges", []),
            "routing_numbers": inst.get("routing_numbers", []),
        })
    return entries


def _load_file_data(path: str) -> list[dict]:
    """Load BIN/routing data from a JSON file."""
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON array, got {type(data).__name__}")
    for i, entry in enumerate(data):
        if "name" not in entry:
            raise ValueError(f"Entry {i} missing required 'name' field")
        entry.setdefault("bin_ranges", [])
        entry.setdefault("routing_numbers", [])
    return data


async def populate(
    db_url: str | None = None,
    data: list[dict] | None = None,
    dry_run: bool = False,
) -> dict:
    """Populate BIN ranges and routing numbers for institutions.

    Returns a summary dict with counts of updates.
    """
    url = db_url or settings.database_url
    engine = create_async_engine(url, echo=False, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    if data is None:
        data = _builtin_data()

    stats = {
        "matched": 0,
        "skipped_not_found": 0,
        "bins_added": 0,
        "routing_added": 0,
        "watch_terms_created": 0,
        "institutions_updated": 0,
    }

    async with session_factory() as session:
        # Build a lookup of all institutions by lowercase name
        result = await session.execute(select(Institution))
        all_institutions = result.scalars().all()
        inst_by_name: dict[str, Institution] = {}
        for inst in all_institutions:
            inst_by_name[inst.name.lower()] = inst
            if inst.short_name:
                inst_by_name[inst.short_name.lower()] = inst

        for entry in data:
            name = entry["name"]
            new_bins = entry.get("bin_ranges", [])
            new_rtns = entry.get("routing_numbers", [])

            inst = inst_by_name.get(name.lower())
            if not inst:
                stats["skipped_not_found"] += 1
                print(f"  [not found] {name}")
                continue

            stats["matched"] += 1

            # Merge BIN ranges (deduplicate)
            existing_bins = set(inst.bin_ranges or [])
            added_bins = [b for b in new_bins if b not in existing_bins]

            # Merge routing numbers (deduplicate)
            existing_rtns = set(inst.routing_numbers or [])
            added_rtns = [r for r in new_rtns if r not in existing_rtns]

            if not added_bins and not added_rtns:
                print(f"  [up to date] {name}")
                continue

            # Get existing watch term values to avoid duplicates
            existing_terms_result = await session.execute(
                select(WatchTerm.value).where(
                    WatchTerm.institution_id == inst.id,
                    WatchTerm.term_type.in_([
                        WatchTermType.routing_number,
                        WatchTermType.bin_range,
                    ]),
                )
            )
            existing_term_values = {row[0] for row in existing_terms_result.all()}

            new_watch_terms: list[WatchTerm] = []

            if added_bins:
                inst.bin_ranges = list(existing_bins | set(new_bins))
                stats["bins_added"] += len(added_bins)
                for b in added_bins:
                    if b not in existing_term_values:
                        new_watch_terms.append(WatchTerm(
                            id=str(uuid4()),
                            institution_id=inst.id,
                            term_type=WatchTermType.bin_range,
                            value=b,
                            enabled=True,
                            case_sensitive=False,
                            notes=f"Card BIN prefix ({len(b)}-digit)",
                        ))

            if added_rtns:
                inst.routing_numbers = list(existing_rtns | set(new_rtns))
                stats["routing_added"] += len(added_rtns)
                for r in added_rtns:
                    if r not in existing_term_values:
                        new_watch_terms.append(WatchTerm(
                            id=str(uuid4()),
                            institution_id=inst.id,
                            term_type=WatchTermType.routing_number,
                            value=r,
                            enabled=True,
                            case_sensitive=False,
                            notes="ABA routing number (FDIC/NCUA public data)",
                        ))

            if not dry_run:
                for wt in new_watch_terms:
                    session.add(wt)

            stats["watch_terms_created"] += len(new_watch_terms)
            stats["institutions_updated"] += 1

            detail_parts = []
            if added_bins:
                detail_parts.append(f"+{len(added_bins)} BINs")
            if added_rtns:
                detail_parts.append(f"+{len(added_rtns)} RTNs")
            if new_watch_terms:
                detail_parts.append(f"+{len(new_watch_terms)} watch terms")
            print(f"  [{'would update' if dry_run else 'updated'}] {name} ({', '.join(detail_parts)})")

        if not dry_run:
            await session.commit()

    await engine.dispose()
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Populate institution BIN ranges and routing numbers",
    )
    parser.add_argument(
        "--file", "-f",
        help="Path to JSON file with BIN/routing data (default: use built-in data)",
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Show what would change without writing to the database",
    )
    parser.add_argument(
        "--db-url",
        help="Database URL override (default: from settings)",
    )
    args = parser.parse_args()

    if args.file:
        data = _load_file_data(args.file)
        print(f"Loaded {len(data)} entries from {args.file}")
    else:
        data = _builtin_data()
        print(f"Using built-in data for {len(data)} institutions")

    if args.dry_run:
        print("DRY RUN — no changes will be written\n")
    else:
        print()

    stats = asyncio.run(populate(db_url=args.db_url, data=data, dry_run=args.dry_run))

    print(f"\n--- Summary ---")
    print(f"  Matched:             {stats['matched']}")
    print(f"  Not found:           {stats['skipped_not_found']}")
    print(f"  Institutions updated: {stats['institutions_updated']}")
    print(f"  BINs added:          {stats['bins_added']}")
    print(f"  Routing nums added:  {stats['routing_added']}")
    print(f"  Watch terms created: {stats['watch_terms_created']}")

    if args.dry_run:
        print("\nDry run complete — no changes written.")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
