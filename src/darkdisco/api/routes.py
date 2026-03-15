"""DarkDisco API routes — full CRUD for all core entities."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from darkdisco.api.auth import (
    create_access_token,
    get_current_user,
    verify_password,
)
from darkdisco.api.schemas import (
    AlertRuleCreate,
    AlertRuleOut,
    AlertRuleUpdate,
    ChannelAdd,
    ChannelOut,
    ChannelRemoveOut,
    ClientCreate,
    ClientOut,
    ClientUpdate,
    DashboardStats,
    DiscordChannelAdd,
    DiscordChannelRemoveOut,
    DiscordGuildChannelOut,
    DomainMatchResult,
    DryRunMatch,
    DryRunRequest,
    DryRunResult,
    FindingCreate,
    FindingOut,
    FindingStatusTransition,
    FindingUpdate,
    InstitutionCreate,
    InstitutionDomainExport,
    InstitutionOut,
    InstitutionUpdate,
    NotificationMarkRead,
    NotificationOut,
    PipelineStatus,
    RawMentionOut,
    RawMentionPromote,
    SeverityCount,
    SourceCreate,
    SourceOut,
    SourceUpdate,
    StatusCount,
    TokenRequest,
    TokenResponse,
    BinRoutingEntry,
    BinRoutingResult,
    BinRoutingSummary,
    DiscoveredChannelOut,
    DiscoveredChannelUpdate,
    DownloadQueueStatus,
    DownloadTaskInfo,
    WatchTermCreate,
    WatchTermOut,
    WatchTermUpdate,
)
from darkdisco.common.database import get_session
from darkdisco.common.models import (
    AlertRule,
    Client,
    DiscoveredChannel,
    DiscoveryStatus,
    ExtractedFile,
    Finding,
    FindingStatus,
    Institution,
    Notification,
    RawMention,
    Severity,
    Source,
    SourceType,
    User,
    WatchTerm,
    WatchTermType,
)

logger = logging.getLogger(__name__)

# Public routes (no auth required) — health check and login only
router = APIRouter()

# Protected routes (JWT required) — all authenticated endpoints
protected = APIRouter(dependencies=[Depends(get_current_user)])

# Valid status transitions: from_status -> set of allowed to_statuses
_VALID_TRANSITIONS: dict[FindingStatus, set[FindingStatus]] = {
    FindingStatus.new: {FindingStatus.reviewing, FindingStatus.false_positive},
    FindingStatus.reviewing: {
        FindingStatus.escalated,
        FindingStatus.resolved,
        FindingStatus.false_positive,
    },
    FindingStatus.escalated: {FindingStatus.resolved, FindingStatus.reviewing},
    FindingStatus.resolved: {FindingStatus.reviewing},  # reopen
    FindingStatus.false_positive: {FindingStatus.reviewing},  # reopen
}


def _extract_archive_file_list(
    metadata: dict | None, search: str | None = None
) -> list[dict]:
    """Pull extracted_file_contents from metadata, optionally filter by search term.

    Legacy fallback: used when ExtractedFile rows don't exist for a mention.
    """
    if not metadata:
        return []
    files = metadata.get("extracted_file_contents")
    if not files or not isinstance(files, list):
        return []

    result = []
    needle = search.lower() if search else None
    for f in files:
        filename = f.get("filename", "")
        content = f.get("content", "")
        if needle and needle not in filename.lower() and needle not in content.lower():
            continue
        result.append({
            "filename": filename,
            "size": len(content),
            "preview": content[:500] if content else "",
            "content": content,
        })
    return result


# ---- Health ----------------------------------------------------------------

@router.get("/health")
async def health():
    return {"status": "ok"}


# ---- Auth ------------------------------------------------------------------

@router.post("/auth/login", response_model=TokenResponse)
async def login(
    body: TokenRequest,
    db: AsyncSession = Depends(get_session),
):
    result = await db.execute(select(User).where(User.username == body.username))
    user = result.scalar_one_or_none()
    if not user or user.disabled or not verify_password(body.password, user.hashed_password):
        raise HTTPException(401, "Invalid credentials")
    token = create_access_token(user.username, user.role.value)
    user.last_login = datetime.now(timezone.utc)
    await db.commit()
    return TokenResponse(access_token=token)


# ---- Integration (trapline connector) --------------------------------------


@router.get(
    "/integration/match-domain",
    response_model=list[DomainMatchResult],
)
async def match_domain(
    domain: str = Query(..., min_length=1),
    db: AsyncSession = Depends(get_session),
):
    """Match institutions by domain similarity for trapline integration.

    Checks primary_domain, additional_domains, and fuzzy institution name matching.
    """
    domain_lower = domain.lower().strip()
    results: list[DomainMatchResult] = []

    stmt = select(Institution).where(Institution.active.is_(True))
    rows = (await db.execute(stmt)).scalars().all()

    # Extract the base name from the query domain for fuzzy name matching
    # e.g. "first-national-bank.com" -> "first national bank"
    domain_base = domain_lower.rsplit(".", 1)[0]  # strip TLD
    domain_words = set(domain_base.replace("-", " ").replace("_", " ").split())

    for inst in rows:
        # Check exact primary domain match
        if inst.primary_domain and inst.primary_domain.lower() == domain_lower:
            results.append(DomainMatchResult(
                institution_id=inst.id,
                name=inst.name,
                primary_domain=inst.primary_domain,
                additional_domains=inst.additional_domains,
                bin_ranges=inst.bin_ranges,
                match_type="exact_primary",
                score=1.0,
            ))
            continue

        # Check additional domains
        additional = inst.additional_domains or []
        if any(d.lower() == domain_lower for d in additional if isinstance(d, str)):
            results.append(DomainMatchResult(
                institution_id=inst.id,
                name=inst.name,
                primary_domain=inst.primary_domain,
                additional_domains=inst.additional_domains,
                bin_ranges=inst.bin_ranges,
                match_type="exact_additional",
                score=1.0,
            ))
            continue

        # Fuzzy name matching: check if institution name words overlap with domain
        if domain_words:
            name_words = set(inst.name.lower().replace("-", " ").replace("_", " ").split())
            overlap = domain_words & name_words
            if len(overlap) >= 2 or (
                len(overlap) == 1
                and len(domain_words) == 1
                and len(next(iter(overlap))) >= 4
            ):
                score = len(overlap) / max(len(domain_words), len(name_words))
                results.append(DomainMatchResult(
                    institution_id=inst.id,
                    name=inst.name,
                    primary_domain=inst.primary_domain,
                    additional_domains=inst.additional_domains,
                    bin_ranges=inst.bin_ranges,
                    match_type="fuzzy_name",
                    score=round(score, 3),
                ))

    # Sort: exact matches first, then by score descending
    results.sort(key=lambda r: (-r.score, r.match_type))
    return results


@router.get(
    "/integration/institutions/domains",
    response_model=list[InstitutionDomainExport],
)
async def list_institution_domains(
    db: AsyncSession = Depends(get_session),
):
    """Bulk export all monitored institution domains for trapline to cache locally."""
    stmt = (
        select(Institution)
        .where(Institution.active.is_(True))
        .order_by(Institution.name)
    )
    rows = (await db.execute(stmt)).scalars().all()
    return [
        InstitutionDomainExport(
            institution_id=inst.id,
            name=inst.name,
            primary_domain=inst.primary_domain,
            additional_domains=inst.additional_domains or [],
            bin_ranges=inst.bin_ranges,
        )
        for inst in rows
        if inst.primary_domain or inst.additional_domains
    ]


# ---- Clients ---------------------------------------------------------------

@protected.get("/clients", response_model=list[ClientOut])
async def list_clients(
    active: bool | None = None,
    db: AsyncSession = Depends(get_session),
):
    stmt = select(Client).order_by(Client.name)
    if active is not None:
        stmt = stmt.where(Client.active == active)
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.post("/clients", response_model=ClientOut, status_code=201)
async def create_client(
    body: ClientCreate,
    db: AsyncSession = Depends(get_session),
):
    client = Client(**body.model_dump())
    db.add(client)
    await db.commit()
    await db.refresh(client)
    return client


@protected.get("/clients/{client_id}", response_model=ClientOut)
async def get_client(
    client_id: str,
    db: AsyncSession = Depends(get_session),
):
    client = await db.get(Client, client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    return client


@protected.put("/clients/{client_id}", response_model=ClientOut)
async def update_client(
    client_id: str,
    body: ClientUpdate,
    db: AsyncSession = Depends(get_session),
):
    client = await db.get(Client, client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    for key, val in body.model_dump(exclude_unset=True).items():
        setattr(client, key, val)
    await db.commit()
    await db.refresh(client)
    return client


@protected.delete("/clients/{client_id}", status_code=204)
async def delete_client(
    client_id: str,
    db: AsyncSession = Depends(get_session),
):
    client = await db.get(Client, client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    await db.delete(client)
    await db.commit()


# ---- Trapline sync helper --------------------------------------------------


def _trigger_trapline_sync(institution_id: str) -> None:
    """Dispatch a Celery task to sync an institution to trapline's watchlist."""
    try:
        from darkdisco.pipeline.worker import sync_institution_to_trapline
        sync_institution_to_trapline.delay(institution_id)
    except Exception:
        logger.warning("Failed to dispatch trapline sync for %s", institution_id, exc_info=True)


# ---- Institutions ----------------------------------------------------------

@protected.get("/institutions", response_model=list[InstitutionOut])
async def list_institutions(
    client_id: str | None = None,
    active: bool | None = None,
    db: AsyncSession = Depends(get_session),
):
    stmt = select(Institution).order_by(Institution.name)
    if client_id is not None:
        stmt = stmt.where(Institution.client_id == client_id)
    if active is not None:
        stmt = stmt.where(Institution.active == active)
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.post("/institutions", response_model=InstitutionOut, status_code=201)
async def create_institution(
    body: InstitutionCreate,
    db: AsyncSession = Depends(get_session),
):
    # Verify parent client exists
    parent = await db.get(Client, body.client_id)
    if not parent:
        raise HTTPException(400, "Client not found")
    inst = Institution(**body.model_dump(by_alias=False))
    db.add(inst)
    await db.commit()
    await db.refresh(inst)
    _trigger_trapline_sync(inst.id)
    return inst


@protected.get("/institutions/{institution_id}", response_model=InstitutionOut)
async def get_institution(
    institution_id: str,
    db: AsyncSession = Depends(get_session),
):
    inst = await db.get(Institution, institution_id)
    if not inst:
        raise HTTPException(404, "Institution not found")
    return inst


@protected.put("/institutions/{institution_id}", response_model=InstitutionOut)
async def update_institution(
    institution_id: str,
    body: InstitutionUpdate,
    db: AsyncSession = Depends(get_session),
):
    inst = await db.get(Institution, institution_id)
    if not inst:
        raise HTTPException(404, "Institution not found")
    for key, val in body.model_dump(exclude_unset=True, by_alias=False).items():
        setattr(inst, key, val)
    await db.commit()
    await db.refresh(inst)
    _trigger_trapline_sync(inst.id)
    return inst


@protected.delete("/institutions/{institution_id}", status_code=204)
async def delete_institution(
    institution_id: str,
    db: AsyncSession = Depends(get_session),
):
    inst = await db.get(Institution, institution_id)
    if not inst:
        raise HTTPException(404, "Institution not found")
    await db.delete(inst)
    await db.commit()


@protected.post("/institutions/populate-bins-routing", response_model=BinRoutingSummary)
async def populate_bins_routing(
    entries: list[BinRoutingEntry],
    db: AsyncSession = Depends(get_session),
):
    """Bulk-populate BIN ranges and routing numbers for institutions.

    Merges new values with existing data (no duplicates). Automatically creates
    watch terms for each newly-added BIN prefix and routing number so the
    matcher can flag them in Telegram chats and data dumps.
    """
    from uuid import uuid4

    # Build institution lookup by lowercase name / short_name
    result = await db.execute(select(Institution))
    all_insts = result.scalars().all()
    inst_by_name: dict[str, Institution] = {}
    for inst in all_insts:
        inst_by_name[inst.name.lower()] = inst
        if inst.short_name:
            inst_by_name[inst.short_name.lower()] = inst

    results: list[BinRoutingResult] = []
    summary = {
        "matched": 0, "not_found": 0, "institutions_updated": 0,
        "bins_added": 0, "routing_added": 0, "watch_terms_created": 0,
    }

    for entry in entries:
        inst = inst_by_name.get(entry.name.lower())
        if not inst:
            summary["not_found"] += 1
            results.append(BinRoutingResult(name=entry.name, status="not_found"))
            continue

        summary["matched"] += 1

        existing_bins = set(inst.bin_ranges or [])
        added_bins = [b for b in entry.bin_ranges if b not in existing_bins]

        existing_rtns = set(inst.routing_numbers or [])
        added_rtns = [r for r in entry.routing_numbers if r not in existing_rtns]

        if not added_bins and not added_rtns:
            results.append(BinRoutingResult(name=entry.name, status="up_to_date"))
            continue

        # Get existing watch term values to avoid duplicates
        existing_terms_result = await db.execute(
            select(WatchTerm.value).where(
                WatchTerm.institution_id == inst.id,
                WatchTerm.term_type.in_([
                    WatchTermType.routing_number,
                    WatchTermType.bin_range,
                ]),
            )
        )
        existing_term_values = {row[0] for row in existing_terms_result.all()}

        wt_count = 0

        if added_bins:
            inst.bin_ranges = list(existing_bins | set(entry.bin_ranges))
            summary["bins_added"] += len(added_bins)
            for b in added_bins:
                if b not in existing_term_values:
                    db.add(WatchTerm(
                        id=str(uuid4()),
                        institution_id=inst.id,
                        term_type=WatchTermType.bin_range,
                        value=b,
                        enabled=True,
                        case_sensitive=False,
                        notes=f"Card BIN prefix ({len(b)}-digit)",
                    ))
                    wt_count += 1

        if added_rtns:
            inst.routing_numbers = list(existing_rtns | set(entry.routing_numbers))
            summary["routing_added"] += len(added_rtns)
            for r in added_rtns:
                if r not in existing_term_values:
                    db.add(WatchTerm(
                        id=str(uuid4()),
                        institution_id=inst.id,
                        term_type=WatchTermType.routing_number,
                        value=r,
                        enabled=True,
                        case_sensitive=False,
                        notes="ABA routing number (FDIC/NCUA public data)",
                    ))
                    wt_count += 1

        summary["watch_terms_created"] += wt_count
        summary["institutions_updated"] += 1

        _trigger_trapline_sync(inst.id)

        results.append(BinRoutingResult(
            name=entry.name,
            status="updated",
            bins_added=len(added_bins),
            routing_added=len(added_rtns),
            watch_terms_created=wt_count,
        ))

    await db.commit()
    return BinRoutingSummary(**summary, results=results)


# ---- Watch Terms -----------------------------------------------------------

@protected.get("/watch-terms", response_model=list[WatchTermOut])
async def list_watch_terms(
    institution_id: str | None = None,
    enabled: bool | None = None,
    db: AsyncSession = Depends(get_session),
):
    stmt = select(WatchTerm).order_by(WatchTerm.created_at.desc())
    if institution_id is not None:
        stmt = stmt.where(WatchTerm.institution_id == institution_id)
    if enabled is not None:
        stmt = stmt.where(WatchTerm.enabled == enabled)
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.post("/watch-terms", response_model=WatchTermOut, status_code=201)
async def create_watch_term(
    body: WatchTermCreate,
    db: AsyncSession = Depends(get_session),
):
    parent = await db.get(Institution, body.institution_id)
    if not parent:
        raise HTTPException(400, "Institution not found")
    wt = WatchTerm(**body.model_dump())
    db.add(wt)
    await db.commit()
    await db.refresh(wt)
    return wt


@protected.get("/watch-terms/{term_id}", response_model=WatchTermOut)
async def get_watch_term(
    term_id: str,
    db: AsyncSession = Depends(get_session),
):
    wt = await db.get(WatchTerm, term_id)
    if not wt:
        raise HTTPException(404, "Watch term not found")
    return wt


@protected.put("/watch-terms/{term_id}", response_model=WatchTermOut)
async def update_watch_term(
    term_id: str,
    body: WatchTermUpdate,
    db: AsyncSession = Depends(get_session),
):
    wt = await db.get(WatchTerm, term_id)
    if not wt:
        raise HTTPException(404, "Watch term not found")
    for key, val in body.model_dump(exclude_unset=True).items():
        setattr(wt, key, val)
    await db.commit()
    await db.refresh(wt)
    return wt


@protected.delete("/watch-terms/{term_id}", status_code=204)
async def delete_watch_term(
    term_id: str,
    db: AsyncSession = Depends(get_session),
):
    wt = await db.get(WatchTerm, term_id)
    if not wt:
        raise HTTPException(404, "Watch term not found")
    await db.delete(wt)
    await db.commit()


# ---- Sources ---------------------------------------------------------------

@protected.get("/sources", response_model=list[SourceOut])
async def list_sources(
    enabled: bool | None = None,
    source_type: str | None = None,
    db: AsyncSession = Depends(get_session),
):
    stmt = select(Source).order_by(Source.name)
    if enabled is not None:
        stmt = stmt.where(Source.enabled == enabled)
    if source_type is not None:
        stmt = stmt.where(Source.source_type == source_type)
    result = await db.execute(stmt)
    sources = result.scalars().all()

    # Compute finding counts
    count_q = (
        select(Finding.source_id, func.count(Finding.id))
        .group_by(Finding.source_id)
    )
    count_rows = {r[0]: r[1] for r in (await db.execute(count_q)).all()}

    now = datetime.now(timezone.utc)
    out = []
    for s in sources:
        health = "offline"
        if s.enabled and s.last_polled_at:
            age_min = (now - s.last_polled_at).total_seconds() / 60
            if s.last_error:
                health = "degraded"
            elif age_min < 30:
                health = "healthy"
            else:
                health = "degraded"
        elif s.enabled:
            health = "offline"

        out.append(SourceOut(
            **{c.key: getattr(s, c.key) for c in Source.__table__.columns},
            health=health,
            finding_count=count_rows.get(s.id, 0),
            avg_poll_seconds=s.poll_interval_seconds,
            last_poll=s.last_polled_at,
        ))
    return out


@protected.post("/sources", response_model=SourceOut, status_code=201)
async def create_source(
    body: SourceCreate,
    db: AsyncSession = Depends(get_session),
):
    source = Source(**body.model_dump())
    db.add(source)
    await db.commit()
    await db.refresh(source)
    return source


@protected.get("/sources/{source_id}", response_model=SourceOut)
async def get_source(
    source_id: str,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")

    # Compute health consistently with list endpoint
    count_q = select(func.count(Finding.id)).where(Finding.source_id == source_id)
    finding_count = (await db.execute(count_q)).scalar() or 0

    now = datetime.now(timezone.utc)
    health = "offline"
    if source.enabled and source.last_polled_at:
        age_min = (now - source.last_polled_at).total_seconds() / 60
        if source.last_error:
            health = "degraded"
        elif age_min < 30:
            health = "healthy"
        else:
            health = "degraded"
    elif source.enabled:
        health = "offline"

    return SourceOut(
        **{c.key: getattr(source, c.key) for c in Source.__table__.columns},
        health=health,
        finding_count=finding_count,
        avg_poll_seconds=source.poll_interval_seconds,
        last_poll=source.last_polled_at,
    )


@protected.put("/sources/{source_id}", response_model=SourceOut)
async def update_source(
    source_id: str,
    body: SourceUpdate,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    for key, val in body.model_dump(exclude_unset=True).items():
        setattr(source, key, val)
    await db.commit()
    await db.refresh(source)

    # Compute health consistently with list endpoint
    count_q = select(func.count(Finding.id)).where(Finding.source_id == source_id)
    finding_count = (await db.execute(count_q)).scalar() or 0

    now = datetime.now(timezone.utc)
    health = "offline"
    if source.enabled and source.last_polled_at:
        age_min = (now - source.last_polled_at).total_seconds() / 60
        if source.last_error:
            health = "degraded"
        elif age_min < 30:
            health = "healthy"
        else:
            health = "degraded"
    elif source.enabled:
        health = "offline"

    return SourceOut(
        **{c.key: getattr(source, c.key) for c in Source.__table__.columns},
        health=health,
        finding_count=finding_count,
        avg_poll_seconds=source.poll_interval_seconds,
        last_poll=source.last_polled_at,
    )


@protected.delete("/sources/{source_id}", status_code=204)
async def delete_source(
    source_id: str,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    await db.delete(source)
    await db.commit()


# ---- Source Channel Management (Telegram) ----------------------------------

@protected.get(
    "/sources/{source_id}/channels", response_model=list[ChannelOut]
)
async def list_channels(
    source_id: str,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    if source.source_type not in (SourceType.telegram, SourceType.telegram_intel):
        raise HTTPException(400, "Channel management is only available for Telegram sources")
    cfg = source.config or {}
    channels = cfg.get("channels", [])
    hwm = cfg.get("last_message_ids", {})
    return [
        ChannelOut(channel=ch, last_message_id=hwm.get(ch))
        for ch in channels
    ]


@protected.post(
    "/sources/{source_id}/channels",
    response_model=ChannelOut,
    status_code=201,
)
async def add_channel(
    source_id: str,
    body: ChannelAdd,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    if source.source_type not in (SourceType.telegram, SourceType.telegram_intel):
        raise HTTPException(400, "Channel management is only available for Telegram sources")

    cfg = dict(source.config or {})
    channels: list[str] = list(cfg.get("channels", []))

    if body.channel in channels:
        raise HTTPException(409, f"Channel already configured: {body.channel}")

    # Optionally join the channel via Telegram API
    if body.join:
        from darkdisco.discovery.connectors.telegram import TelegramConnector

        connector = TelegramConnector(cfg)
        try:
            await connector.setup()
            joined = await connector.join_channel(body.channel)
            if not joined:
                raise HTTPException(
                    502, f"Failed to join channel: {body.channel}"
                )
        finally:
            await connector.teardown()

    channels.append(body.channel)
    cfg["channels"] = channels
    source.config = cfg
    await db.commit()
    await db.refresh(source)
    return ChannelOut(channel=body.channel)


@protected.delete(
    "/sources/{source_id}/channels/{channel:path}",
    response_model=ChannelRemoveOut,
)
async def remove_channel(
    source_id: str,
    channel: str,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    if source.source_type not in (SourceType.telegram, SourceType.telegram_intel):
        raise HTTPException(400, "Channel management is only available for Telegram sources")

    cfg = dict(source.config or {})
    channels: list[str] = list(cfg.get("channels", []))

    if channel not in channels:
        raise HTTPException(404, f"Channel not configured: {channel}")

    channels.remove(channel)
    cfg["channels"] = channels
    # Clean up high-water mark
    hwm: dict = dict(cfg.get("last_message_ids", {}))
    hwm.pop(channel, None)
    cfg["last_message_ids"] = hwm
    source.config = cfg
    await db.commit()
    return ChannelRemoveOut(removed=channel)


# ---- Discord Guild/Channel Management -------------------------------------

@protected.get(
    "/sources/{source_id}/discord-channels",
    response_model=list[DiscordGuildChannelOut],
)
async def list_discord_channels(
    source_id: str,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    if source.source_type != SourceType.discord:
        raise HTTPException(400, "Discord channel management is only available for Discord sources")
    cfg = source.config or {}
    guild_channels = cfg.get("guild_channels", {})
    return [
        DiscordGuildChannelOut(guild_id=gid, channel_ids=cids)
        for gid, cids in guild_channels.items()
    ]


@protected.post(
    "/sources/{source_id}/discord-channels",
    response_model=DiscordGuildChannelOut,
    status_code=201,
)
async def add_discord_channel(
    source_id: str,
    body: DiscordChannelAdd,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    if source.source_type != SourceType.discord:
        raise HTTPException(400, "Discord channel management is only available for Discord sources")

    cfg = dict(source.config or {})
    guild_channels: dict[str, list[str]] = dict(cfg.get("guild_channels", {}))

    channels = list(guild_channels.get(body.guild_id, []))
    if body.channel_id in channels:
        raise HTTPException(409, f"Channel already configured: {body.guild_id}/{body.channel_id}")

    channels.append(body.channel_id)
    guild_channels[body.guild_id] = channels
    cfg["guild_channels"] = guild_channels
    source.config = cfg
    await db.commit()
    await db.refresh(source)
    return DiscordGuildChannelOut(guild_id=body.guild_id, channel_ids=channels)


@protected.delete(
    "/sources/{source_id}/discord-channels/{guild_id}/{channel_id}",
    response_model=DiscordChannelRemoveOut,
)
async def remove_discord_channel(
    source_id: str,
    guild_id: str,
    channel_id: str,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    if source.source_type != SourceType.discord:
        raise HTTPException(400, "Discord channel management is only available for Discord sources")

    cfg = dict(source.config or {})
    guild_channels: dict[str, list[str]] = dict(cfg.get("guild_channels", {}))

    channels = list(guild_channels.get(guild_id, []))
    if channel_id not in channels:
        raise HTTPException(404, f"Channel not configured: {guild_id}/{channel_id}")

    channels.remove(channel_id)
    if channels:
        guild_channels[guild_id] = channels
    else:
        guild_channels.pop(guild_id, None)

    # Clean up high-water mark
    hwm: dict = dict(cfg.get("last_message_ids", {}))
    hwm.pop(channel_id, None)
    cfg["last_message_ids"] = hwm
    cfg["guild_channels"] = guild_channels
    source.config = cfg
    await db.commit()
    return DiscordChannelRemoveOut(guild_id=guild_id, removed_channel=channel_id)


# ---- Source Poll Trigger & Findings ----------------------------------------

@protected.post("/sources/{source_id}/poll")
async def trigger_poll(
    source_id: str,
    db: AsyncSession = Depends(get_session),
):
    """Manually trigger a poll for a source."""
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    if not source.enabled:
        raise HTTPException(400, "Source is disabled")

    from darkdisco.pipeline.worker import poll_source

    task = poll_source.delay(source_id)
    return {"status": "dispatched", "task_id": task.id, "source_id": source_id}


@protected.get("/sources/{source_id}/findings", response_model=list[FindingOut])
async def list_source_findings(
    source_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    """List findings for a specific source."""
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")

    stmt = (
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .where(Finding.source_id == source_id)
        .order_by(Finding.discovered_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.get("/sources/{source_id}/findings/trend")
async def source_findings_trend(
    source_id: str,
    days: int = Query(14, ge=1, le=90),
    db: AsyncSession = Depends(get_session),
):
    """Get daily finding counts for a source over the last N days."""
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")

    since = datetime.now(timezone.utc) - timedelta(days=days)
    stmt = (
        select(
            func.date(Finding.discovered_at).label("date"),
            func.count(Finding.id).label("count"),
        )
        .where(Finding.source_id == source_id, Finding.discovered_at >= since)
        .group_by(func.date(Finding.discovered_at))
        .order_by(func.date(Finding.discovered_at))
    )
    result = await db.execute(stmt)
    rows = result.all()
    return [{"date": str(r.date), "count": r.count} for r in rows]


# ---- Raw Mentions ----------------------------------------------------------

@protected.get("/mentions", response_model=list[RawMentionOut])
async def list_mentions(
    source_id: str | None = None,
    source_type: str | None = None,
    promoted: bool | None = None,
    q: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    """Browse raw collected mentions. Filter by source, promotion status, or search content."""
    stmt = (
        select(RawMention)
        .options(selectinload(RawMention.source))
        .order_by(RawMention.collected_at.desc())
    )
    if source_id is not None:
        stmt = stmt.where(RawMention.source_id == source_id)
    if source_type is not None:
        stmt = stmt.join(Source).where(Source.source_type == source_type)
    if promoted is not None:
        if promoted:
            stmt = stmt.where(RawMention.promoted_to_finding_id.isnot(None))
        else:
            stmt = stmt.where(RawMention.promoted_to_finding_id.is_(None))
    if q:
        stmt = stmt.where(RawMention.content.ilike(f"%{q}%"))
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.get("/mentions/{mention_id}", response_model=RawMentionOut)
async def get_mention(
    mention_id: str,
    db: AsyncSession = Depends(get_session),
):
    result = await db.execute(
        select(RawMention)
        .options(selectinload(RawMention.source))
        .where(RawMention.id == mention_id)
    )
    mention = result.scalar_one_or_none()
    if not mention:
        raise HTTPException(404, "Mention not found")
    return mention


@protected.get("/mentions/{mention_id}/archive-contents")
async def mention_archive_contents(
    mention_id: str,
    q: str | None = None,
    db: AsyncSession = Depends(get_session),
):
    """Return per-file extracted archive contents for a mention.

    Queries the extracted_files table first (with FTS for search), falling back
    to legacy JSONB metadata if no ExtractedFile rows exist.
    """
    result = await db.execute(
        select(RawMention).where(RawMention.id == mention_id)
    )
    mention = result.scalar_one_or_none()
    if not mention:
        raise HTTPException(404, "Mention not found")

    # Try normalized ExtractedFile table first
    stmt = select(ExtractedFile).where(ExtractedFile.mention_id == mention_id)
    if q:
        stmt = stmt.where(
            or_(
                ExtractedFile.filename.ilike(f"%{q}%"),
                ExtractedFile.content_tsvector.match(q),
            )
        )
    ef_result = await db.execute(stmt)
    extracted_rows = ef_result.scalars().all()

    if extracted_rows:
        files = [
            {
                "filename": ef.filename,
                "size": ef.size or 0,
                "preview": (ef.text_content or "")[:500],
                "content": ef.text_content or "",
                "s3_key": ef.s3_key,
                "sha256": ef.sha256,
                "extension": ef.extension,
                "is_text": ef.is_text,
            }
            for ef in extracted_rows
        ]
    else:
        # Fallback to legacy JSONB metadata
        files = _extract_archive_file_list(mention.metadata_, q)

    return {"mention_id": mention_id, "files": files, "total": len(files)}


@protected.get("/extracted-files/{file_id}/preview")
async def extracted_file_preview(
    file_id: str,
    db: AsyncSession = Depends(get_session),
):
    """Return text content preview for a single extracted file."""
    ef = await db.get(ExtractedFile, file_id)
    if not ef:
        raise HTTPException(404, "Extracted file not found")
    return {
        "id": ef.id,
        "mention_id": ef.mention_id,
        "filename": ef.filename,
        "size": ef.size or 0,
        "extension": ef.extension,
        "is_text": ef.is_text,
        "sha256": ef.sha256,
        "content": ef.text_content or "",
    }


@protected.get("/extracted-files/search")
async def search_extracted_files(
    q: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_session),
):
    """Full-text search across all extracted file contents using PostgreSQL tsquery."""
    stmt = (
        select(ExtractedFile)
        .where(ExtractedFile.content_tsvector.match(q))
        .order_by(ExtractedFile.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.scalars().all()

    count_stmt = (
        select(func.count())
        .select_from(ExtractedFile)
        .where(ExtractedFile.content_tsvector.match(q))
    )
    total = (await db.execute(count_stmt)).scalar() or 0

    return {
        "query": q,
        "total": total,
        "files": [
            {
                "id": ef.id,
                "mention_id": ef.mention_id,
                "filename": ef.filename,
                "size": ef.size or 0,
                "extension": ef.extension,
                "is_text": ef.is_text,
                "preview": (ef.text_content or "")[:500],
                "s3_key": ef.s3_key,
            }
            for ef in rows
        ],
    }


@protected.post("/mentions/{mention_id}/promote", response_model=FindingOut)
async def promote_mention(
    mention_id: str,
    body: RawMentionPromote,
    db: AsyncSession = Depends(get_session),
):
    """Promote a raw mention to a finding for analyst workflow."""
    result = await db.execute(
        select(RawMention)
        .options(selectinload(RawMention.source))
        .where(RawMention.id == mention_id)
    )
    mention = result.scalar_one_or_none()
    if not mention:
        raise HTTPException(404, "Mention not found")
    if mention.promoted_to_finding_id:
        raise HTTPException(409, "Mention already promoted to a finding")

    parent = await db.get(Institution, body.institution_id)
    if not parent:
        raise HTTPException(400, "Institution not found")

    finding = Finding(
        institution_id=body.institution_id,
        source_id=mention.source_id,
        severity=body.severity,
        title=body.title,
        summary=body.summary,
        raw_content=mention.content,
        content_hash=mention.content_hash,
        source_url=mention.source_url,
        tags=body.tags,
        metadata_=mention.metadata_,
    )
    db.add(finding)
    await db.flush()

    mention.promoted_to_finding_id = finding.id
    await db.commit()

    result = await db.execute(
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .where(Finding.id == finding.id)
    )
    return result.scalar_one()


# ---- Findings --------------------------------------------------------------

@protected.get("/findings", response_model=list[FindingOut])
async def list_findings(
    institution_id: str | None = None,
    severity: Severity | None = None,
    status: FindingStatus | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    stmt = (
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .order_by(Finding.discovered_at.desc())
    )
    if institution_id is not None:
        stmt = stmt.where(Finding.institution_id == institution_id)
    if severity is not None:
        stmt = stmt.where(Finding.severity == severity)
    if status is not None:
        stmt = stmt.where(Finding.status == status)
    if date_from is not None:
        stmt = stmt.where(Finding.discovered_at >= date_from)
    if date_to is not None:
        stmt = stmt.where(Finding.discovered_at <= date_to)
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.post("/findings", response_model=FindingOut, status_code=201)
async def create_finding(
    body: FindingCreate,
    db: AsyncSession = Depends(get_session),
):
    parent = await db.get(Institution, body.institution_id)
    if not parent:
        raise HTTPException(400, "Institution not found")
    finding = Finding(**body.model_dump(by_alias=False))
    db.add(finding)
    await db.commit()
    # Re-fetch with relationships loaded
    result = await db.execute(
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .where(Finding.id == finding.id)
    )
    return result.scalar_one()


@protected.get("/findings/search", response_model=list[FindingOut])
async def search_findings(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    """Full-text-ish search across finding title, summary, and raw_content."""
    like_pattern = f"%{q}%"
    stmt = (
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .where(
            or_(
                Finding.title.ilike(like_pattern),
                Finding.summary.ilike(like_pattern),
                Finding.raw_content.ilike(like_pattern),
            )
        )
        .order_by(Finding.discovered_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.get("/findings/{finding_id}/archive-contents")
async def finding_archive_contents(
    finding_id: str,
    q: str | None = None,
    db: AsyncSession = Depends(get_session),
):
    """Return per-file extracted archive contents for a finding.

    Looks up the linked raw_mention via promoted_to_finding_id and queries
    ExtractedFile rows, falling back to legacy JSONB metadata.
    """
    finding = await db.get(Finding, finding_id)
    if not finding:
        raise HTTPException(404, "Finding not found")

    # Find the raw_mention that was promoted to this finding
    mention_result = await db.execute(
        select(RawMention).where(RawMention.promoted_to_finding_id == finding_id)
    )
    linked_mention = mention_result.scalar_one_or_none()

    if linked_mention:
        stmt = select(ExtractedFile).where(ExtractedFile.mention_id == linked_mention.id)
        if q:
            stmt = stmt.where(
                or_(
                    ExtractedFile.filename.ilike(f"%{q}%"),
                    ExtractedFile.content_tsvector.match(q),
                )
            )
        ef_result = await db.execute(stmt)
        extracted_rows = ef_result.scalars().all()

        if extracted_rows:
            files = [
                {
                    "filename": ef.filename,
                    "size": ef.size or 0,
                    "preview": (ef.text_content or "")[:500],
                    "content": ef.text_content or "",
                    "s3_key": ef.s3_key,
                    "sha256": ef.sha256,
                    "extension": ef.extension,
                    "is_text": ef.is_text,
                }
                for ef in extracted_rows
            ]
            return {"finding_id": finding_id, "files": files, "total": len(files)}

    # Fallback to legacy JSONB metadata
    files = _extract_archive_file_list(finding.metadata_, q)
    return {"finding_id": finding_id, "files": files, "total": len(files)}


@protected.get("/findings/{finding_id}", response_model=FindingOut)
async def get_finding(
    finding_id: str,
    db: AsyncSession = Depends(get_session),
):
    result = await db.execute(
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .where(Finding.id == finding_id)
    )
    finding = result.scalar_one_or_none()
    if not finding:
        raise HTTPException(404, "Finding not found")
    return finding


@protected.put("/findings/{finding_id}", response_model=FindingOut)
async def update_finding(
    finding_id: str,
    body: FindingUpdate,
    db: AsyncSession = Depends(get_session),
):
    finding = await db.get(Finding, finding_id)
    if not finding:
        raise HTTPException(404, "Finding not found")
    updates = body.model_dump(exclude_unset=True, by_alias=False)
    # If status is being changed, validate transition
    if "status" in updates and updates["status"] != finding.status:
        _check_transition(finding.status, updates["status"])
    for key, val in updates.items():
        setattr(finding, key, val)
    await db.commit()
    # Re-fetch with relationships loaded
    result = await db.execute(
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .where(Finding.id == finding_id)
    )
    return result.scalar_one()


@protected.post("/findings/{finding_id}/transition", response_model=FindingOut)
async def transition_finding_status(
    finding_id: str,
    body: FindingStatusTransition,
    db: AsyncSession = Depends(get_session),
):
    """Explicit status transition with validation."""
    finding = await db.get(Finding, finding_id)
    if not finding:
        raise HTTPException(404, "Finding not found")
    _check_transition(finding.status, body.status)
    finding.status = body.status
    if body.notes:
        existing = finding.analyst_notes or ""
        separator = "\n---\n" if existing else ""
        finding.analyst_notes = existing + separator + body.notes
    await db.commit()
    # Re-fetch with relationships loaded
    result = await db.execute(
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .where(Finding.id == finding_id)
    )
    return result.scalar_one()


@protected.delete("/findings/{finding_id}", status_code=204)
async def delete_finding(
    finding_id: str,
    db: AsyncSession = Depends(get_session),
):
    finding = await db.get(Finding, finding_id)
    if not finding:
        raise HTTPException(404, "Finding not found")
    await db.delete(finding)
    await db.commit()


def _check_transition(current: FindingStatus, target: FindingStatus) -> None:
    allowed = _VALID_TRANSITIONS.get(current, set())
    if target not in allowed:
        raise HTTPException(
            409,
            f"Invalid status transition: {current.value} -> {target.value}. "
            f"Allowed: {', '.join(s.value for s in allowed)}",
        )


# ---- Dashboard Stats -------------------------------------------------------

@protected.get("/dashboard/stats", response_model=DashboardStats)
async def dashboard_stats(
    institution_id: str | None = None,
    db: AsyncSession = Depends(get_session),
):
    base = select(Finding).options(
        selectinload(Finding.institution), selectinload(Finding.source)
    )
    if institution_id:
        base = base.where(Finding.institution_id == institution_id)

    # Total count
    total_q = select(func.count(Finding.id))
    if institution_id:
        total_q = total_q.where(Finding.institution_id == institution_id)
    total = (await db.execute(total_q)).scalar() or 0

    # By severity
    sev_q = (
        select(Finding.severity, func.count(Finding.id))
        .group_by(Finding.severity)
    )
    if institution_id:
        sev_q = sev_q.where(Finding.institution_id == institution_id)
    sev_rows = (await db.execute(sev_q)).all()
    by_severity = [SeverityCount(severity=s, count=c) for s, c in sev_rows]

    # By status
    stat_q = (
        select(Finding.status, func.count(Finding.id))
        .group_by(Finding.status)
    )
    if institution_id:
        stat_q = stat_q.where(Finding.institution_id == institution_id)
    stat_rows = (await db.execute(stat_q)).all()
    by_status = [StatusCount(status=s, count=c) for s, c in stat_rows]

    # Recent findings (last 10)
    recent_q = (
        base.order_by(Finding.discovered_at.desc()).limit(10)
    )
    recent_rows = (await db.execute(recent_q)).scalars().all()

    # findings_by_severity as a dict keyed by severity name
    sev_dict: dict[str, int] = {s: 0 for s in ("critical", "high", "medium", "low", "info")}
    for sc in by_severity:
        sev_dict[sc.severity] = sc.count

    # New today
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    new_today_q = select(func.count(Finding.id)).where(Finding.discovered_at >= today_start)
    if institution_id:
        new_today_q = new_today_q.where(Finding.institution_id == institution_id)
    new_today = (await db.execute(new_today_q)).scalar() or 0

    # Monitored institutions
    inst_q = select(func.count(Institution.id))
    monitored_institutions = (await db.execute(inst_q)).scalar() or 0

    # Active sources
    src_q = select(func.count(Source.id)).where(Source.enabled.is_(True))
    active_sources = (await db.execute(src_q)).scalar() or 0

    # Findings trend (last 14 days)
    trend = []
    for i in range(13, -1, -1):
        day = today_start - timedelta(days=i)
        day_end = day + timedelta(days=1)
        day_q = select(func.count(Finding.id)).where(
            Finding.discovered_at >= day,
            Finding.discovered_at < day_end,
        )
        if institution_id:
            day_q = day_q.where(Finding.institution_id == institution_id)
        cnt = (await db.execute(day_q)).scalar() or 0
        trend.append({"date": day.strftime("%Y-%m-%d"), "count": cnt})

    return DashboardStats(
        total_findings=total,
        findings_by_severity=sev_dict,
        new_today=new_today,
        monitored_institutions=monitored_institutions,
        active_sources=active_sources,
        findings_trend=trend,
        by_severity=by_severity,
        by_status=by_status,
        recent_findings=recent_rows,
    )


# ---- Alert Rules -----------------------------------------------------------

@protected.get("/alert-rules", response_model=list[AlertRuleOut])
async def list_alert_rules(
    owner_id: str | None = None,
    enabled: bool | None = None,
    db: AsyncSession = Depends(get_session),
):
    stmt = select(AlertRule).order_by(AlertRule.created_at.desc())
    if owner_id is not None:
        stmt = stmt.where(AlertRule.owner_id == owner_id)
    if enabled is not None:
        stmt = stmt.where(AlertRule.enabled == enabled)
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.post("/alert-rules", response_model=AlertRuleOut, status_code=201)
async def create_alert_rule(
    body: AlertRuleCreate,
    db: AsyncSession = Depends(get_session),
):
    rule = AlertRule(**body.model_dump())
    db.add(rule)
    await db.commit()
    await db.refresh(rule)
    return rule


@protected.get("/alert-rules/{rule_id}", response_model=AlertRuleOut)
async def get_alert_rule(
    rule_id: str,
    db: AsyncSession = Depends(get_session),
):
    rule = await db.get(AlertRule, rule_id)
    if not rule:
        raise HTTPException(404, "Alert rule not found")
    return rule


@protected.put("/alert-rules/{rule_id}", response_model=AlertRuleOut)
async def update_alert_rule(
    rule_id: str,
    body: AlertRuleUpdate,
    db: AsyncSession = Depends(get_session),
):
    rule = await db.get(AlertRule, rule_id)
    if not rule:
        raise HTTPException(404, "Alert rule not found")
    for key, val in body.model_dump(exclude_unset=True).items():
        setattr(rule, key, val)
    await db.commit()
    await db.refresh(rule)
    return rule


@protected.delete("/alert-rules/{rule_id}", status_code=204)
async def delete_alert_rule(
    rule_id: str,
    db: AsyncSession = Depends(get_session),
):
    rule = await db.get(AlertRule, rule_id)
    if not rule:
        raise HTTPException(404, "Alert rule not found")
    await db.delete(rule)
    await db.commit()


# ---- Pipeline Diagnostics --------------------------------------------------

@protected.get("/pipeline/status", response_model=PipelineStatus)
async def pipeline_status(
    db: AsyncSession = Depends(get_session),
):
    """Show pipeline health: sources, watch terms, matching coverage."""
    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)

    # Sources overview
    all_sources = (await db.execute(select(Source))).scalars().all()
    enabled_count = sum(1 for s in all_sources if s.enabled)

    source_details = []
    for s in all_sources:
        health = "offline"
        if s.enabled and s.last_polled_at:
            age_min = (now - s.last_polled_at).total_seconds() / 60
            if s.last_error:
                health = "degraded"
            elif age_min < 30:
                health = "healthy"
            else:
                health = "stale"
        elif s.enabled:
            health = "never_polled"

        source_details.append({
            "id": s.id,
            "name": s.name,
            "type": s.source_type.value,
            "enabled": s.enabled,
            "health": health,
            "last_polled": s.last_polled_at.isoformat() if s.last_polled_at else None,
            "last_error": s.last_error,
            "poll_interval_seconds": s.poll_interval_seconds,
        })

    # Watch term coverage
    terms = (await db.execute(
        select(WatchTerm).where(WatchTerm.enabled.is_(True))
    )).scalars().all()
    term_coverage: dict[str, int] = {}
    for t in terms:
        ttype = t.term_type.value
        term_coverage[ttype] = term_coverage.get(ttype, 0) + 1

    # Finding counts
    total_findings = (await db.execute(
        select(func.count(Finding.id))
    )).scalar() or 0
    recent_findings = (await db.execute(
        select(func.count(Finding.id)).where(Finding.discovered_at >= cutoff_24h)
    )).scalar() or 0

    return PipelineStatus(
        enabled_sources=enabled_count,
        total_sources=len(all_sources),
        active_watch_terms=len(terms),
        total_findings=total_findings,
        recent_findings_24h=recent_findings,
        sources=source_details,
        watch_term_coverage=term_coverage,
    )


@protected.post("/pipeline/dry-run", response_model=DryRunResult)
async def pipeline_dry_run(
    body: DryRunRequest,
    db: AsyncSession = Depends(get_session),
):
    """Test matching against sample content without creating findings.

    Useful for verifying watch terms match expected content.
    """
    from darkdisco.discovery.connectors.base import RawMention
    from darkdisco.discovery.matcher import match_mention
    from darkdisco.enrichment.false_positive import check_false_positive

    # Load active watch terms
    terms = (await db.execute(
        select(WatchTerm).where(WatchTerm.enabled.is_(True))
    )).scalars().all()

    mention = RawMention(
        source_name=body.source_name,
        title=body.title,
        content=body.content,
        metadata={},
    )

    match_results = match_mention(mention, list(terms))

    # Build response with institution names
    dry_matches = []
    for result in match_results:
        inst = await db.get(Institution, result.institution_id)
        dry_matches.append(DryRunMatch(
            institution_id=result.institution_id,
            institution_name=inst.name if inst else None,
            matched_terms=result.matched_terms,
            severity_hint=result.severity_hint,
        ))

    # Run FP analysis on first match (if any)
    fp_analysis = None
    would_create = len(dry_matches) > 0
    if dry_matches:
        candidate = {
            "title": body.title,
            "raw_content": body.content,
            "matched_terms": dry_matches[0].matched_terms,
            "metadata": {},
        }
        fp_result = check_false_positive(candidate)
        fp_analysis = {
            "fp_score": fp_result.fp_score,
            "is_likely_fp": fp_result.is_likely_fp,
            "recommendation": fp_result.recommendation,
            "signals": [
                {"rule": s.rule, "description": s.description, "weight": s.weight}
                for s in fp_result.signals
            ],
        }
        if fp_result.recommendation == "auto_dismiss":
            would_create = False

    return DryRunResult(
        matches=dry_matches,
        fp_analysis=fp_analysis,
        would_create_finding=would_create,
    )


# ---- Notifications ---------------------------------------------------------

@protected.get("/notifications", response_model=list[NotificationOut])
async def list_notifications(
    user_id: str | None = None,
    unread_only: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    stmt = select(Notification).order_by(Notification.created_at.desc())
    if user_id is not None:
        stmt = stmt.where(Notification.user_id == user_id)
    if unread_only:
        stmt = stmt.where(Notification.read.is_(False))
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.get("/notifications/{notification_id}", response_model=NotificationOut)
async def get_notification(
    notification_id: str,
    db: AsyncSession = Depends(get_session),
):
    notif = await db.get(Notification, notification_id)
    if not notif:
        raise HTTPException(404, "Notification not found")
    return notif


@protected.put("/notifications/{notification_id}/read", response_model=NotificationOut)
async def mark_notification_read(
    notification_id: str,
    body: NotificationMarkRead,
    db: AsyncSession = Depends(get_session),
):
    notif = await db.get(Notification, notification_id)
    if not notif:
        raise HTTPException(404, "Notification not found")
    notif.read = body.read
    await db.commit()
    await db.refresh(notif)
    return notif


@protected.post("/notifications/mark-all-read", status_code=204)
async def mark_all_notifications_read(
    user_id: str = Query(...),
    db: AsyncSession = Depends(get_session),
):
    from sqlalchemy import update

    await db.execute(
        update(Notification)
        .where(Notification.user_id == user_id, Notification.read.is_(False))
        .values(read=True)
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Stored archive extraction backfill
# ---------------------------------------------------------------------------


@protected.post("/pipeline/backfill-stored-archives")
async def trigger_backfill_stored_archives(
    batch_size: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_session),
):
    """Trigger extraction of stored-but-unextracted archives from S3.

    Finds mentions with archives uploaded to S3 that were never extracted
    (typically because they were too large for in-memory processing) and
    dispatches streaming extraction tasks for each.
    """
    from darkdisco.pipeline.worker import backfill_stored_archives

    # Count eligible mentions for context
    from darkdisco.common.models import ExtractedFile

    already_extracted = select(ExtractedFile.mention_id).distinct().scalar_subquery()
    total_stored = (await db.execute(
        select(func.count(RawMention.id)).where(
            RawMention.metadata_.isnot(None),
            RawMention.id.notin_(already_extracted),
        )
    )).scalar() or 0

    task = backfill_stored_archives.delay(batch_size)

    return {
        "status": "dispatched",
        "task_id": task.id,
        "batch_size": batch_size,
        "estimated_eligible": total_stored,
    }


@protected.post("/pipeline/extract-mention-archive/{mention_id}")
async def trigger_extract_mention_archive(
    mention_id: str,
    db: AsyncSession = Depends(get_session),
):
    """Trigger streaming extraction for a single mention's stored archive."""
    mention = await db.get(RawMention, mention_id)
    if not mention:
        raise HTTPException(404, "Mention not found")

    meta = mention.metadata_ or {}
    if not meta.get("s3_key"):
        raise HTTPException(400, "Mention has no stored archive (no s3_key)")

    from darkdisco.pipeline.worker import extract_stored_archive

    task = extract_stored_archive.delay(mention_id)

    return {
        "status": "dispatched",
        "task_id": task.id,
        "mention_id": mention_id,
        "archive_filename": meta.get("file_name", "unknown"),
    }


# ---- Download Queue Status -------------------------------------------------


@protected.get("/pipeline/download-status", response_model=DownloadQueueStatus)
async def download_status(
    recent_limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_session),
):
    """Show download queue: active extraction, pending archives, recent completions."""
    from darkdisco.common.models import ExtractedFile
    from darkdisco.pipeline.worker import app as celery_app

    # --- Query Celery for active/reserved extraction tasks ---
    current_task: DownloadTaskInfo | None = None
    pending_tasks: list[DownloadTaskInfo] = []

    try:
        inspect = celery_app.control.inspect(timeout=2.0)
        active = inspect.active() or {}
        reserved = inspect.reserved() or {}

        # Extract extraction tasks from active workers
        for _worker, tasks in active.items():
            for t in tasks:
                task_name = t.get("name", "")
                if "extract_stored_archive" not in task_name:
                    continue
                args = t.get("args", [])
                mention_id = args[0] if args else None
                info = DownloadTaskInfo(
                    task_id=t.get("id", ""),
                    mention_id=mention_id,
                    filename=None,
                    status="active",
                    started_at=None,
                )
                if current_task is None:
                    current_task = info
                else:
                    pending_tasks.append(info)

        # Reserved = queued but not yet started
        for _worker, tasks in reserved.items():
            for t in tasks:
                task_name = t.get("name", "")
                if "extract_stored_archive" not in task_name:
                    continue
                args = t.get("args", [])
                mention_id = args[0] if args else None
                pending_tasks.append(DownloadTaskInfo(
                    task_id=t.get("id", ""),
                    mention_id=mention_id,
                    filename=None,
                    status="pending",
                ))
    except Exception:
        # Celery inspect may fail if broker is down — continue with DB data
        logger.warning("Failed to inspect Celery workers for download status")

    # Enrich tasks with mention filenames if we have mention_ids
    all_mention_ids = []
    if current_task and current_task.mention_id:
        all_mention_ids.append(current_task.mention_id)
    for t in pending_tasks:
        if t.mention_id:
            all_mention_ids.append(t.mention_id)

    if all_mention_ids:
        mentions_rows = (await db.execute(
            select(RawMention.id, RawMention.metadata_).where(
                RawMention.id.in_(all_mention_ids)
            )
        )).all()
        mention_filenames = {
            r.id: (r.metadata_ or {}).get("file_name", "unknown")
            for r in mentions_rows
        }
        if current_task and current_task.mention_id:
            current_task.filename = mention_filenames.get(current_task.mention_id)
        for t in pending_tasks:
            if t.mention_id:
                t.filename = mention_filenames.get(t.mention_id)

    # --- Recent completions: mentions that have extracted files ---
    recent_extracted = (await db.execute(
        select(
            RawMention.id,
            RawMention.metadata_,
            RawMention.collected_at,
            func.count(ExtractedFile.id).label("file_count"),
        )
        .outerjoin(ExtractedFile, ExtractedFile.mention_id == RawMention.id)
        .where(RawMention.metadata_["file_analysis"].isnot(None))
        .group_by(RawMention.id)
        .order_by(RawMention.collected_at.desc())
        .limit(recent_limit)
    )).all()

    recent: list[DownloadTaskInfo] = []
    for row in recent_extracted:
        meta = row.metadata_ or {}
        analysis = meta.get("file_analysis", {})
        has_error = analysis.get("extraction_attempted") and analysis.get("total_files", 0) == 0
        recent.append(DownloadTaskInfo(
            task_id="",
            mention_id=row.id,
            filename=meta.get("file_name", "unknown"),
            status="error" if has_error else "success",
            completed_at=row.collected_at,
            files_extracted=row.file_count,
        ))

    # --- Stats ---
    # Total pending (have s3_key but no file_analysis)
    already_extracted_sq = select(ExtractedFile.mention_id).distinct().scalar_subquery()
    total_pending = (await db.execute(
        select(func.count(RawMention.id)).where(
            RawMention.metadata_.isnot(None),
            RawMention.id.notin_(already_extracted_sq),
        )
    )).scalar() or 0

    # Total stored files
    total_stored = (await db.execute(
        select(func.count(ExtractedFile.id))
    )).scalar() or 0

    # Total with errors (extraction attempted but 0 files)
    total_errors = sum(1 for r in recent if r.status == "error")

    # Total extractions done
    total_extracted = (await db.execute(
        select(func.count(RawMention.id)).where(
            RawMention.metadata_["file_analysis"].isnot(None)
        )
    )).scalar() or 0

    return DownloadQueueStatus(
        current=current_task,
        pending=pending_tasks,
        recent=recent,
        stats={
            "total_pending": total_pending,
            "total_stored": total_stored,
            "total_errors": total_errors,
            "total_extracted": total_extracted,
        },
    )


# ---- Discovered Channels ---------------------------------------------------

@protected.get(
    "/discovered-channels",
    response_model=list[DiscoveredChannelOut],
)
async def list_discovered_channels(
    status: DiscoveryStatus | None = None,
    source_id: str | None = None,
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_session),
):
    """List discovered channel links, optionally filtered by status or source."""
    q = select(DiscoveredChannel).order_by(DiscoveredChannel.discovered_at.desc())
    if status is not None:
        q = q.where(DiscoveredChannel.status == status)
    if source_id is not None:
        q = q.where(DiscoveredChannel.source_id == source_id)
    q = q.offset(offset).limit(limit)
    rows = (await db.execute(q)).scalars().all()
    return [DiscoveredChannelOut.model_validate(r) for r in rows]


@protected.get(
    "/discovered-channels/{channel_id}",
    response_model=DiscoveredChannelOut,
)
async def get_discovered_channel(
    channel_id: str,
    db: AsyncSession = Depends(get_session),
):
    ch = await db.get(DiscoveredChannel, channel_id)
    if not ch:
        raise HTTPException(404, "Discovered channel not found")
    return DiscoveredChannelOut.model_validate(ch)


@protected.put(
    "/discovered-channels/{channel_id}",
    response_model=DiscoveredChannelOut,
)
async def update_discovered_channel(
    channel_id: str,
    body: DiscoveredChannelUpdate,
    db: AsyncSession = Depends(get_session),
):
    """Update a discovered channel's status (approve, ignore, etc.).

    When status is set to 'approved', the channel will be picked up by the
    periodic process_channel_discoveries task for auto-joining.
    """
    ch = await db.get(DiscoveredChannel, channel_id)
    if not ch:
        raise HTTPException(404, "Discovered channel not found")

    ch.status = body.status
    if body.notes is not None:
        ch.notes = body.notes
    if body.target_source_id is not None:
        ch.added_to_source_id = body.target_source_id

    await db.commit()
    await db.refresh(ch)
    return DiscoveredChannelOut.model_validate(ch)


@protected.post(
    "/discovered-channels/{channel_id}/join",
    response_model=DiscoveredChannelOut,
)
async def join_discovered_channel(
    channel_id: str,
    target_source_id: str | None = Query(default=None),
    db: AsyncSession = Depends(get_session),
):
    """Immediately join a discovered channel and add it to a source."""
    ch = await db.get(DiscoveredChannel, channel_id)
    if not ch:
        raise HTTPException(404, "Discovered channel not found")

    if ch.status == DiscoveryStatus.joined:
        raise HTTPException(409, "Channel already joined")

    # Determine target source
    target_sid = target_source_id or ch.added_to_source_id or ch.source_id
    target_source = await db.get(Source, target_sid)
    if not target_source:
        raise HTTPException(404, "Target source not found")
    if target_source.source_type not in (SourceType.telegram, SourceType.telegram_intel):
        raise HTTPException(400, "Target source must be a Telegram source")

    # Join via Telegram API
    from darkdisco.discovery.connectors.telegram import TelegramConnector

    cfg = dict(target_source.config or {})
    connector = TelegramConnector(cfg)
    try:
        await connector.setup()
        joined = await connector.join_channel(ch.url)
    finally:
        await connector.teardown()

    if not joined:
        ch.status = DiscoveryStatus.failed
        ch.notes = (ch.notes or "") + "\nJoin failed"
        await db.commit()
        await db.refresh(ch)
        raise HTTPException(502, "Failed to join channel")

    # Add to source config
    channels: list[str] = list(cfg.get("channels", []))
    if ch.url not in channels:
        channels.append(ch.url)
        cfg["channels"] = channels
        target_source.config = cfg
    ch.status = DiscoveryStatus.joined
    ch.added_to_source_id = target_sid
    ch.joined_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(ch)
    return DiscoveredChannelOut.model_validate(ch)


@protected.delete("/discovered-channels/{channel_id}")
async def delete_discovered_channel(
    channel_id: str,
    db: AsyncSession = Depends(get_session),
):
    ch = await db.get(DiscoveredChannel, channel_id)
    if not ch:
        raise HTTPException(404, "Discovered channel not found")
    await db.delete(ch)
    await db.commit()
    return {"deleted": channel_id}

