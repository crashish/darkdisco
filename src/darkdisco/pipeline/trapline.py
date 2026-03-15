"""Trapline client watchlist sync — push institution data to trapline's watchlist API.

Trapline's client watchlist API scores +15 points in triage for matches against
registered domains, brands, and BIN ranges. DarkDisco pushes its monitored
institutions so trapline can detect phishing targeting them.
"""

from __future__ import annotations

import logging

import httpx

from darkdisco.config import settings

logger = logging.getLogger(__name__)

_TIMEOUT = 30  # seconds


def _client() -> httpx.Client:
    """Create an httpx client configured for the trapline API."""
    return httpx.Client(
        base_url=settings.trapline_api_url.rstrip("/"),
        headers={
            "X-API-Key": settings.trapline_api_key,
            "Content-Type": "application/json",
        },
        timeout=_TIMEOUT,
    )


def _build_domain_entries(institution) -> list[dict]:
    """Build watchlist domain entries from an institution's domains."""
    entries: list[dict] = []

    if institution.primary_domain:
        entries.append({
            "type": "domain",
            "value": institution.primary_domain,
        })

    # Also add institution name as a brand entry
    if institution.name:
        entries.append({
            "type": "brand",
            "value": institution.name.lower(),
        })

    for domain in institution.additional_domains or []:
        if isinstance(domain, str) and domain.strip():
            entries.append({
                "type": "domain",
                "value": domain.strip(),
            })

    return entries


def _build_bin_entries(institution) -> list[dict]:
    """Build watchlist BIN entries from an institution's BIN ranges."""
    entries: list[dict] = []
    for bin_range in institution.bin_ranges or []:
        if isinstance(bin_range, str) and bin_range.strip():
            entries.append({
                "bin": bin_range.strip(),
                "institution_id": institution.id,
                "institution_name": institution.name,
            })
        elif isinstance(bin_range, dict):
            # Support structured BIN entries like {"prefix": "411111", "length": 6}
            entry = dict(bin_range)
            entry["institution_id"] = institution.id
            entry["institution_name"] = institution.name
            entries.append(entry)

    return entries


def sync_institution(institution) -> dict:
    """Sync a single institution's domains and BINs to trapline's client watchlist.

    Returns a summary dict with counts and any errors.
    """
    if not settings.trapline_api_url or not settings.trapline_api_key:
        logger.debug("Trapline integration not configured, skipping sync")
        return {"skipped": True, "reason": "not_configured"}

    result = {"institution_id": institution.id, "domains": 0, "bins": 0, "errors": []}

    domain_entries = _build_domain_entries(institution)
    bin_entries = _build_bin_entries(institution)

    if not domain_entries and not bin_entries:
        logger.debug("Institution %s has no domains or BINs to sync", institution.name)
        return result

    with _client() as client:
        # Sync domains (including brand name)
        if domain_entries:
            payload = {
                "entries": domain_entries,
                "brand": institution.name,
            }
            try:
                resp = client.post("/api/v1/watchlist/domains", json=payload)
                resp.raise_for_status()
                result["domains"] = len(domain_entries)
                logger.info(
                    "Synced %d domains for %s to trapline",
                    len(domain_entries), institution.name,
                )
            except httpx.HTTPError as exc:
                error = f"domains sync failed: {exc}"
                result["errors"].append(error)
                logger.warning("Trapline %s for %s", error, institution.name)

        # Sync BIN ranges
        if bin_entries:
            payload = {"bins": [{"bin_prefix": e.get("bin", e.get("bin_prefix", "")), "issuer": e.get("institution_name", "")} for e in bin_entries]}
            try:
                resp = client.post("/api/v1/watchlist/bins", json=payload)
                resp.raise_for_status()
                result["bins"] = len(bin_entries)
                logger.info(
                    "Synced %d BIN ranges for %s to trapline",
                    len(bin_entries), institution.name,
                )
            except httpx.HTTPError as exc:
                error = f"bins sync failed: {exc}"
                result["errors"].append(error)
                logger.warning("Trapline %s for %s", error, institution.name)

    return result


def sync_all_institutions(session) -> dict:
    """Sync all active institutions to trapline's client watchlist.

    Args:
        session: SQLAlchemy sync session.

    Returns:
        Summary dict with total counts.
    """
    from sqlalchemy import select

    from darkdisco.common.models import Institution

    if not settings.trapline_api_url or not settings.trapline_api_key:
        return {"skipped": True, "reason": "not_configured"}

    institutions = session.execute(
        select(Institution).where(Institution.active.is_(True))
    ).scalars().all()

    total = {"synced": 0, "domains": 0, "bins": 0, "errors": 0}

    for inst in institutions:
        result = sync_institution(inst)
        if result.get("skipped"):
            continue
        total["synced"] += 1
        total["domains"] += result["domains"]
        total["bins"] += result["bins"]
        total["errors"] += len(result["errors"])

    logger.info(
        "Trapline watchlist sync complete: %d institutions, %d domains, %d bins, %d errors",
        total["synced"], total["domains"], total["bins"], total["errors"],
    )
    return total
