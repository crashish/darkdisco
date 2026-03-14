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
    PaginatedMentionsOut,
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
    WatchTermCreate,
    WatchTermOut,
    WatchTermUpdate,
)
from darkdisco.common.database import get_session
from darkdisco.common.models import (
    AlertRule,
    Client,
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
    metadata: dict | None, search: str | None = None, content_blob: str = ""
) -> list[dict]:
    """Pull file inventory from metadata, optionally filter by search term.

    Legacy fallback: used when ExtractedFile rows don't exist for a mention.
    Checks extracted_file_contents first, then file_analysis.files.
    """
    if not metadata:
        return []

    # Try new-style extracted_file_contents (has content)
    files = metadata.get("extracted_file_contents")
    if files and isinstance(files, list):
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

    # Fallback to file_analysis.files with content parsed from mention text
    analysis = metadata.get("file_analysis")
    if analysis and isinstance(analysis, dict):
        file_list = analysis.get("files", [])
        if not isinstance(file_list, list):
            return []
        result = []
        needle = search.lower() if search else None
        file_sha256 = metadata.get("file_sha256", "")[:8]

        # Parse per-file content from the concatenated mention content
        content_by_file = _parse_extracted_sections(content_blob) if content_blob else {}

        text_exts = {".txt", ".csv", ".log", ".json", ".xml", ".html", ".sql", ".cfg", ".conf", ".ini", ".env", ".yml", ".yaml"}
        for f in file_list:
            filename = f.get("filename", "")
            file_content = content_by_file.get(filename, "")
            if needle:
                if needle not in filename.lower() and needle not in file_content.lower():
                    continue
            s3_key = f"files/{file_sha256}/extracted/{f.get('sha256', '')[:8]}/{filename}" if file_sha256 else None
            result.append({
                "filename": filename,
                "size": f.get("size", 0),
                "preview": file_content[:500] if file_content else "",
                "content": file_content,
                "s3_key": s3_key,
                "sha256": f.get("sha256", ""),
                "extension": f.get("extension", ""),
                "is_text": f.get("extension", "") in text_exts,
            })
        return result

    return []


def _list_s3_extracted_files(s3_key: str) -> list[dict]:
    """List extracted files in S3 under the archive's prefix."""
    import boto3
    from darkdisco.config import settings

    # s3_key is like "files/804e57ef/@Trident_Cloud.zip"
    # extracted files are at "files/804e57ef/extracted/..."
    prefix = s3_key.rsplit("/", 1)[0] + "/extracted/"

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )

    text_exts = {".txt", ".csv", ".log", ".json", ".xml", ".html", ".sql", ".cfg", ".conf", ".ini", ".env", ".yml", ".yaml"}
    result = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Preserve path relative to extracted/ prefix
                rel_path = key[len(prefix):] if key.startswith(prefix) else key
                # rel_path is like "sha256prefix/BR_i4iAB/passwords.txt"
                # Skip the sha256 prefix dir, show the inner archive path
                parts = rel_path.split("/", 1)
                filename = parts[1] if len(parts) > 1 else parts[0]
                ext = "." + filename.rsplit(".", 1)[1] if "." in filename else ""
                result.append({
                    "filename": filename,
                    "size": obj.get("Size", 0),
                    "preview": "",
                    "content": "",
                    "s3_key": key,
                    "sha256": "",
                    "extension": ext,
                    "is_text": ext in text_exts,
                })
    except Exception:
        pass

    return result


def _parse_extracted_sections(content: str) -> dict[str, str]:
    """Parse '--- filename ---' delimited sections from concatenated mention content."""
    import re
    sections: dict[str, str] = {}
    # Find all section headers and their positions
    header_pattern = re.compile(r'^--- (.+?) ---\s*$', re.MULTILINE)
    headers = list(header_pattern.finditer(content))
    for i, match in enumerate(headers):
        filename = match.group(1).strip()
        if filename == "Extracted archive content":
            continue
        # Content runs from end of this header to start of next header (or end of string)
        start = match.end()
        end = headers[i + 1].start() if i + 1 < len(headers) else len(content)
        text = content[start:end].strip()
        if filename:
            sections[filename] = text
    return sections


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
            age_sec = (now - s.last_polled_at).total_seconds()
            stale_threshold = max(s.poll_interval_seconds * 2, 1800)
            if s.last_error:
                health = "degraded"
            elif age_sec < stale_threshold:
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
        age_sec = (now - source.last_polled_at).total_seconds()
        stale_threshold = max(source.poll_interval_seconds * 2, 1800)
        if source.last_error:
            health = "degraded"
        elif age_sec < stale_threshold:
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
        age_sec = (now - source.last_polled_at).total_seconds()
        stale_threshold = max(source.poll_interval_seconds * 2, 1800)
        if source.last_error:
            health = "degraded"
        elif age_sec < stale_threshold:
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

@protected.get("/mentions", response_model=PaginatedMentionsOut)
async def list_mentions(
    source_id: str | None = None,
    source_type: str | None = None,
    promoted: bool | None = None,
    channel: str | None = None,
    has_media: bool | None = None,
    q: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    """Browse raw collected mentions. Filter by source, channel, media, promotion status, or search content."""
    base = select(RawMention)
    if source_id is not None:
        base = base.where(RawMention.source_id == source_id)
    if source_type is not None:
        base = base.join(Source).where(Source.source_type == source_type)
    if promoted is not None:
        if promoted:
            base = base.where(RawMention.promoted_to_finding_id.isnot(None))
        else:
            base = base.where(RawMention.promoted_to_finding_id.is_(None))
    if channel:
        base = base.where(RawMention.metadata_["channel_ref"].astext == channel)
    if has_media is not None:
        if has_media:
            base = base.where(RawMention.metadata_["has_media"].astext == "true")
        else:
            base = base.where(
                RawMention.metadata_["has_media"].is_(None)
                | (RawMention.metadata_["has_media"].astext != "true")
            )
    if q:
        base = base.where(RawMention.content.ilike(f"%{q}%"))

    # Total count
    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar() or 0

    # Paginated query
    stmt = (
        base.options(selectinload(RawMention.source))
        .order_by(RawMention.collected_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)
    mentions = result.scalars().all()
    return PaginatedMentionsOut(items=mentions, total=total, page=page, page_size=page_size)


@protected.get("/mentions/channels")
async def list_mention_channels(db: AsyncSession = Depends(get_session)):
    """Return distinct channel_ref values from raw_mentions metadata."""
    stmt = select(
        RawMention.metadata_["channel_ref"].astext.label("channel"),
        func.count(RawMention.id).label("count"),
    ).where(
        RawMention.metadata_["channel_ref"].isnot(None)
    ).group_by("channel").order_by(func.count(RawMention.id).desc())
    rows = (await db.execute(stmt)).all()
    return [{"channel": r.channel, "count": r.count} for r in rows]


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
        # Fallback to legacy JSONB metadata (parse content sections)
        files = _extract_archive_file_list(mention.metadata_, q, content_blob=mention.content or "")

    # Always supplement with S3 listing for stored archives
    # This catches files beyond the 100-item inventory cap and
    # mentions that were stored but never extracted in-memory
    meta = mention.metadata_ or {}
    if meta.get("download_status") == "stored" and meta.get("s3_key"):
        try:
            import asyncio
            s3_files = await asyncio.get_event_loop().run_in_executor(
                None, _list_s3_extracted_files, meta["s3_key"]
            )
            # Merge: add S3 files not already in the list (by filename)
            existing_names = {f["filename"] for f in files}
            for sf in s3_files:
                if sf["filename"] not in existing_names:
                    if q and q.lower() not in sf["filename"].lower():
                        continue
                    files.append(sf)
        except Exception:
            logger.debug("Failed to list S3 extracted files", exc_info=True)

    return {"mention_id": mention_id, "files": files, "total": len(files)}


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


# ---- Mention Files ---------------------------------------------------------

@protected.get("/mentions/{mention_id}/file")
async def get_mention_file(
    mention_id: str,
    db: AsyncSession = Depends(get_session),
):
    """Serve the original file attached to a mention from S3."""
    import boto3
    from botocore.config import Config
    from fastapi.responses import StreamingResponse
    from darkdisco.config import settings

    mention = (await db.execute(
        select(RawMention).where(RawMention.id == mention_id)
    )).scalar_one_or_none()
    if not mention:
        raise HTTPException(404, "Mention not found")

    meta = mention.metadata_ or {}
    s3_key = meta.get("s3_key")
    if not s3_key:
        raise HTTPException(404, "No file stored for this mention")

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4"),
    )

    try:
        obj = s3.get_object(Bucket=settings.s3_bucket, Key=s3_key)
    except Exception:
        raise HTTPException(404, "File not found in storage")

    content_type = meta.get("file_mime", "application/octet-stream")
    filename = meta.get("file_name") or s3_key.rsplit("/", 1)[-1]

    return StreamingResponse(
        obj["Body"],
        media_type=content_type,
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Content-Length": str(obj.get("ContentLength", "")),
        },
    )


@protected.get("/mentions/{mention_id}/files")
async def list_mention_files(
    mention_id: str,
    db: AsyncSession = Depends(get_session),
):
    """List all files associated with a mention — original + extracted."""
    import boto3
    from botocore.config import Config
    from darkdisco.config import settings

    mention = (await db.execute(
        select(RawMention).where(RawMention.id == mention_id)
    )).scalar_one_or_none()
    if not mention:
        raise HTTPException(404, "Mention not found")

    meta = mention.metadata_ or {}
    files = []

    # Original file
    if meta.get("s3_key"):
        files.append({
            "type": "original",
            "filename": meta.get("file_name") or "unnamed",
            "size": meta.get("file_size"),
            "mime": meta.get("file_mime"),
            "sha256": meta.get("file_sha256"),
            "s3_key": meta["s3_key"],
            "download_url": f"/api/mentions/{mention_id}/file",
        })

    # Extracted files from archive analysis
    analysis = meta.get("file_analysis", {})
    if analysis.get("files"):
        s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=Config(signature_version="s3v4"),
        )

        file_sha_prefix = meta.get("file_sha256", "")[:8]
        for ef in analysis["files"]:
            ef_s3_key = f"files/{file_sha_prefix}/extracted/{ef['sha256'][:8]}/{ef['filename']}"
            # Check if extracted file exists in S3
            exists = True
            try:
                s3.head_object(Bucket=settings.s3_bucket, Key=ef_s3_key)
            except Exception:
                exists = False

            files.append({
                "type": "extracted",
                "filename": ef["filename"],
                "size": ef.get("size"),
                "extension": ef.get("extension"),
                "sha256": ef.get("sha256"),
                "s3_key": ef_s3_key if exists else None,
                "download_url": f"/api/files/{ef_s3_key}" if exists else None,
            })

    return {
        "mention_id": mention_id,
        "original_file": meta.get("file_name"),
        "download_status": meta.get("download_status"),
        "passwords": meta.get("extracted_passwords", []),
        "has_credentials": meta.get("has_credentials", False),
        "credential_count": meta.get("credential_count", 0),
        "credential_samples": analysis.get("credential_samples", []),
        "files": files,
    }


@protected.get("/files/{s3_key:path}")
async def serve_s3_file(
    s3_key: str,
):
    """Serve any file from S3 by key. Used for extracted archive contents."""
    import boto3
    import mimetypes
    from botocore.config import Config
    from fastapi.responses import StreamingResponse
    from darkdisco.config import settings

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4"),
    )

    try:
        obj = s3.get_object(Bucket=settings.s3_bucket, Key=s3_key)
    except Exception:
        raise HTTPException(404, "File not found in storage")

    filename = s3_key.rsplit("/", 1)[-1]
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    return StreamingResponse(
        obj["Body"],
        media_type=content_type,
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Content-Length": str(obj.get("ContentLength", "")),
        },
    )


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
    files = _extract_archive_file_list(finding.metadata_, q, content_blob=finding.raw_content or "")
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

