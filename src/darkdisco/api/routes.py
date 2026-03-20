"""DarkDisco API routes — full CRUD for all core entities."""

from __future__ import annotations

import hashlib
import hmac
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from darkdisco.api.auth import (
    create_access_token,
    get_current_user,
    get_current_user_or_token_param,
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
    FindingAuditLogOut,
    GeneratedReportOut,
    FindingCreate,
    FindingNoteAdd,
    FindingOut,
    FindingStatusTransition,
    FindingUpdate,
    PaginatedFindingsOut,
    InstitutionCreate,
    InstitutionDomainExport,
    InstitutionOut,
    InstitutionUpdate,
    TraplineWebhookPayload,
    TraplineWebhookResponse,
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
    BinRoutingEntry,
    BinRoutingResult,
    BinRoutingSummary,
    InstitutionExportRow,
    InstitutionImportResult,
    DiscoveredChannelOut,
    DiscoveredChannelUpdate,
    ReportRequest,
    ReportScheduleCreate,
    ReportScheduleOut,
    ReportScheduleUpdate,
    ReportTemplateCreate,
    ReportTemplateOut,
    ReportTemplateUpdate,
    BINRecordOut,
    BINLookupResponse,
    BINImportResponse,
    BINStatsResponse,
    WatchTermCreate,
    WatchTermOut,
    WatchTermUpdate,
)
from darkdisco.common.database import get_session
from darkdisco.config import settings
from darkdisco.common.models import (
    AlertRule,
    BINRecord,
    Client,
    DiscoveredChannel,
    DiscoveryStatus,
    ExtractedFile,
    Finding,
    GeneratedReport,
    FindingAuditLog,
    FindingStatus,
    ImageOCRCache,
    Institution,
    ReportSchedule,
    ReportTemplate,
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
    FindingStatus.new: {FindingStatus.reviewing, FindingStatus.confirmed, FindingStatus.dismissed, FindingStatus.false_positive},
    FindingStatus.reviewing: {
        FindingStatus.escalated,
        FindingStatus.confirmed,
        FindingStatus.dismissed,
        FindingStatus.resolved,
        FindingStatus.false_positive,
    },
    FindingStatus.escalated: {FindingStatus.resolved, FindingStatus.confirmed, FindingStatus.dismissed, FindingStatus.false_positive, FindingStatus.reviewing},
    FindingStatus.confirmed: {FindingStatus.resolved, FindingStatus.escalated, FindingStatus.dismissed, FindingStatus.false_positive, FindingStatus.reviewing},
    FindingStatus.dismissed: {FindingStatus.reviewing, FindingStatus.confirmed, FindingStatus.false_positive, FindingStatus.resolved},
    FindingStatus.resolved: {FindingStatus.reviewing, FindingStatus.confirmed, FindingStatus.false_positive, FindingStatus.dismissed},
    FindingStatus.false_positive: {FindingStatus.reviewing, FindingStatus.confirmed, FindingStatus.dismissed},
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
    MAX_FILES = 500  # Cap to prevent slow responses on huge archives
    result = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix, PaginationConfig={"MaxItems": MAX_FILES}):
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


# ---- Trapline webhook receiver ---------------------------------------------


def _build_trapline_metadata(payload: TraplineWebhookPayload, match_type: str | None) -> dict:
    """Build the trapline metadata dict from a webhook payload."""
    meta = {
        "score": payload.score,
        "brands": payload.brands,
        "artifacts": payload.artifacts,
        "screenshot_url": payload.screenshot_url,
        "finding_id": payload.finding_id,
        "completed_at": payload.completed_at,
        "match_type": match_type,
    }
    if payload.dns_records is not None:
        meta["dns_records"] = payload.dns_records
    if payload.whois is not None:
        meta["whois"] = payload.whois
    if payload.tls_certificate is not None:
        meta["tls_certificate"] = payload.tls_certificate
    if payload.network_log is not None:
        meta["network_log"] = payload.network_log
    if payload.score_breakdown is not None:
        meta["score_breakdown"] = payload.score_breakdown
    return meta


def _verify_trapline_signature(body: bytes, signature: str) -> bool:
    """Verify HMAC-SHA256 signature from trapline's X-Trapline-Signature header."""
    if not settings.trapline_webhook_secret:
        return False
    expected = hmac.new(
        settings.trapline_webhook_secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


@router.post(
    "/integration/trapline-webhook",
    response_model=TraplineWebhookResponse,
    status_code=202,
)
async def receive_trapline_webhook(
    request: Request,
    x_trapline_signature: str = Header(...),
    db: AsyncSession = Depends(get_session),
):
    """Receive finding.completed webhooks from trapline.

    Verifies HMAC-SHA256 signature, matches the finding to a darkdisco
    institution by domain/brand, and creates or enriches a Finding record.
    """
    body = await request.body()

    if not _verify_trapline_signature(body, x_trapline_signature):
        raise HTTPException(401, "Invalid signature")

    payload = TraplineWebhookPayload.model_validate_json(body)

    if payload.event != "finding.completed":
        return TraplineWebhookResponse(status="ignored", message=f"unhandled event: {payload.event}")

    # Match domain to an institution
    domain_lower = payload.domain.lower().strip()
    stmt = select(Institution).where(Institution.active.is_(True))
    rows = (await db.execute(stmt)).scalars().all()

    matched_institution = None
    match_type = None

    for inst in rows:
        # Exact primary domain
        if inst.primary_domain and inst.primary_domain.lower() == domain_lower:
            matched_institution = inst
            match_type = "exact_primary"
            break
        # Additional domains
        additional = inst.additional_domains or []
        if any(d.lower() == domain_lower for d in additional if isinstance(d, str)):
            matched_institution = inst
            match_type = "exact_additional"
            break
        # Brand name match
        if inst.name and inst.name.lower() in [b.lower() for b in payload.brands]:
            matched_institution = inst
            match_type = "brand"
            break

    if not matched_institution:
        logger.info("Trapline webhook: no institution match for domain=%s brands=%s", payload.domain, payload.brands)
        return TraplineWebhookResponse(status="skipped", message="no matching institution")

    # Build content hash for dedup
    content_hash = hashlib.sha256(
        f"trapline:{payload.finding_id or payload.domain}".encode()
    ).hexdigest()

    # Check for existing finding with same content_hash (idempotency)
    existing = (
        await db.execute(
            select(Finding).where(Finding.content_hash == content_hash)
        )
    ).scalar_one_or_none()

    if existing:
        # Enrich existing finding with updated trapline data
        meta = existing.metadata_ or {}
        meta["trapline"] = _build_trapline_metadata(payload, match_type)
        existing.metadata_ = meta
        await db.commit()
        logger.info("Trapline webhook: enriched existing finding %s for %s", existing.id, payload.domain)
        return TraplineWebhookResponse(
            status="enriched",
            finding_id=existing.id,
            institution_id=matched_institution.id,
        )

    # Create new Finding
    severity = Severity.high if payload.score >= 70 else Severity.medium if payload.score >= 40 else Severity.low
    finding = Finding(
        institution_id=matched_institution.id,
        severity=severity,
        status=FindingStatus.new,
        title=f"Phishing site detected: {payload.domain}",
        summary=f"Trapline detected phishing targeting {matched_institution.name} at {payload.domain} (score: {payload.score})",
        source_url=payload.screenshot_url,
        content_hash=content_hash,
        matched_terms=[{"source": "trapline", "match_type": match_type, "domain": payload.domain}],
        tags=["trapline", "phishing"],
        metadata_={
            "trapline": _build_trapline_metadata(payload, match_type),
        },
    )
    db.add(finding)
    await db.commit()
    await db.refresh(finding)

    logger.info("Trapline webhook: created finding %s for %s (institution=%s)", finding.id, payload.domain, matched_institution.name)
    return TraplineWebhookResponse(
        status="created",
        finding_id=finding.id,
        institution_id=matched_institution.id,
    )


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


@protected.get("/institutions/export")
async def export_institutions(
    format: str = Query("json", pattern="^(json|csv)$"),
    client_id: str | None = None,
    db: AsyncSession = Depends(get_session),
):
    """Export all institutions with their domains, BINs, routing numbers, and watch terms."""
    import csv
    import io
    import json as json_lib

    stmt = (
        select(Institution)
        .options(selectinload(Institution.watch_terms))
        .order_by(Institution.name)
    )
    if client_id is not None:
        stmt = stmt.where(Institution.client_id == client_id)
    result = await db.execute(stmt)
    institutions = result.scalars().all()

    rows = []
    for inst in institutions:
        terms = [
            {"term_type": t.term_type.value, "value": t.value, "enabled": t.enabled,
             "case_sensitive": t.case_sensitive, "notes": t.notes}
            for t in (inst.watch_terms or [])
        ]
        rows.append(InstitutionExportRow(
            name=inst.name,
            short_name=inst.short_name,
            charter_type=inst.charter_type,
            state=inst.state,
            primary_domain=inst.primary_domain,
            additional_domains=inst.additional_domains,
            bin_ranges=inst.bin_ranges,
            routing_numbers=inst.routing_numbers,
            active=inst.active,
            watch_terms=terms or None,
        ))

    if format == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "name", "short_name", "charter_type", "state", "primary_domain",
            "additional_domains", "bin_ranges", "routing_numbers", "active", "watch_terms",
        ])
        for r in rows:
            writer.writerow([
                r.name, r.short_name or "", r.charter_type or "", r.state or "",
                r.primary_domain or "",
                json_lib.dumps(r.additional_domains) if r.additional_domains else "",
                json_lib.dumps(r.bin_ranges) if r.bin_ranges else "",
                json_lib.dumps(r.routing_numbers) if r.routing_numbers else "",
                r.active,
                json_lib.dumps(r.watch_terms) if r.watch_terms else "",
            ])
        content = buf.getvalue()
        return StreamingResponse(
            iter([content]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=institutions.csv"},
        )
    else:
        content = json_lib.dumps(
            [r.model_dump() for r in rows], indent=2, default=str
        )
        return StreamingResponse(
            iter([content]),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=institutions.json"},
        )


@protected.post("/institutions/import", response_model=InstitutionImportResult)
async def import_institutions(
    file: UploadFile = File(...),
    client_id: str = Query(...),
    db: AsyncSession = Depends(get_session),
):
    """Import institutions from JSON or CSV file. Skips duplicates by name within the client."""
    import csv
    import io
    import json as json_lib

    # Verify client exists
    parent = await db.get(Client, client_id)
    if not parent:
        raise HTTPException(400, "Client not found")

    raw = await file.read()
    text = raw.decode("utf-8-sig")

    # Parse input
    entries: list[dict] = []
    filename = (file.filename or "").lower()
    if filename.endswith(".csv") or file.content_type == "text/csv":
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            entry: dict = {}
            entry["name"] = row.get("name", "").strip()
            entry["short_name"] = row.get("short_name", "").strip() or None
            entry["charter_type"] = row.get("charter_type", "").strip() or None
            entry["state"] = row.get("state", "").strip() or None
            entry["primary_domain"] = row.get("primary_domain", "").strip() or None
            for json_field in ("additional_domains", "bin_ranges", "routing_numbers", "watch_terms"):
                val = row.get(json_field, "").strip()
                entry[json_field] = json_lib.loads(val) if val else None
            active_val = row.get("active", "true").strip().lower()
            entry["active"] = active_val not in ("false", "0", "no")
            entries.append(entry)
    else:
        try:
            entries = json_lib.loads(text)
        except json_lib.JSONDecodeError as e:
            raise HTTPException(400, f"Invalid JSON: {e}")
        if not isinstance(entries, list):
            raise HTTPException(400, "JSON must be an array of institution objects")

    # Get existing institution names for dedup
    existing_stmt = select(Institution.name).where(Institution.client_id == client_id)
    existing_result = await db.execute(existing_stmt)
    existing_names = {row[0].lower() for row in existing_result.all()}

    imported = 0
    skipped = 0
    errors: list[str] = []

    for i, entry in enumerate(entries):
        name = (entry.get("name") or "").strip()
        if not name:
            errors.append(f"Row {i + 1}: missing name")
            continue
        if name.lower() in existing_names:
            skipped += 1
            continue

        inst = Institution(
            client_id=client_id,
            name=name,
            short_name=entry.get("short_name"),
            charter_type=entry.get("charter_type"),
            state=entry.get("state"),
            primary_domain=entry.get("primary_domain"),
            additional_domains=entry.get("additional_domains"),
            bin_ranges=entry.get("bin_ranges"),
            routing_numbers=entry.get("routing_numbers"),
            active=entry.get("active", True),
        )
        db.add(inst)
        await db.flush()

        # Import watch terms if present
        watch_terms = entry.get("watch_terms") or []
        for wt in watch_terms:
            if isinstance(wt, dict) and wt.get("value"):
                term = WatchTerm(
                    institution_id=inst.id,
                    term_type=wt.get("term_type", "keyword"),
                    value=wt["value"],
                    enabled=wt.get("enabled", True),
                    case_sensitive=wt.get("case_sensitive", False),
                    notes=wt.get("notes"),
                )
                db.add(term)

        existing_names.add(name.lower())
        imported += 1

    await db.commit()
    return InstitutionImportResult(imported=imported, skipped=skipped, errors=errors)


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
    sort_by: str | None = None,
    sort_dir: str = Query("desc", pattern="^(asc|desc)$"),
    date_from: str | None = None,
    date_to: str | None = None,
    source_ids: str | None = None,
    channels: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    """Browse raw collected mentions. Filter by source, channel, media, promotion status, or search content."""
    base = select(RawMention)
    # Multi-select source filter (comma-separated IDs)
    if source_ids:
        ids = [s.strip() for s in source_ids.split(",") if s.strip()]
        if ids:
            base = base.where(RawMention.source_id.in_(ids))
    elif source_id is not None:
        base = base.where(RawMention.source_id == source_id)
    if source_type is not None:
        base = base.join(Source).where(Source.source_type == source_type)
    if promoted is not None:
        if promoted:
            base = base.where(RawMention.promoted_to_finding_id.isnot(None))
        else:
            base = base.where(RawMention.promoted_to_finding_id.is_(None))
    # Multi-select channel filter (comma-separated)
    if channels:
        ch_list = [c.strip() for c in channels.split(",") if c.strip()]
        if ch_list:
            base = base.where(RawMention.metadata_["channel_ref"].astext.in_(ch_list))
    elif channel:
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
    # Date range filtering on collected_at
    if date_from:
        try:
            dt_from = datetime.fromisoformat(date_from)
            base = base.where(RawMention.collected_at >= dt_from)
        except ValueError:
            pass
    if date_to:
        try:
            dt_to = datetime.fromisoformat(date_to)
            # Include the entire end day
            if len(date_to) <= 10:  # date only, no time component
                dt_to = dt_to.replace(hour=23, minute=59, second=59)
            base = base.where(RawMention.collected_at <= dt_to)
        except ValueError:
            pass

    # Total count
    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar() or 0

    # Sorting
    sortable_columns = {
        "collected_at": RawMention.collected_at,
        "content": RawMention.content,
        "source_id": RawMention.source_id,
    }
    sort_col = sortable_columns.get(sort_by, RawMention.collected_at)
    order = sort_col.asc() if sort_dir == "asc" else sort_col.desc()

    # Paginated query
    stmt = (
        base.options(selectinload(RawMention.source))
        .order_by(order)
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
        from darkdisco.pipeline.files import detect_mime_type
        # Bulk-fetch OCR cache entries for all sha256 hashes in this batch
        sha_list = [ef.sha256 for ef in extracted_rows if ef.sha256]
        ocr_map: dict[str, ImageOCRCache] = {}
        if sha_list:
            ocr_result = await db.execute(
                select(ImageOCRCache).where(ImageOCRCache.sha256.in_(sha_list))
            )
            ocr_map = {r.sha256: r for r in ocr_result.scalars().all()}
        files = []
        for ef in extracted_rows:
            ocr = ocr_map.get(ef.sha256) if ef.sha256 else None
            entry: dict = {
                "filename": ef.filename,
                "size": ef.size or 0,
                "preview": (ef.text_content or "")[:200] if q else "",
                "content": "",  # Content loaded on-demand via /files/{s3_key}
                "s3_key": ef.s3_key,
                "sha256": ef.sha256,
                "extension": ef.extension,
                "is_text": ef.is_text,
                "mime_type": detect_mime_type(ef.filename),
            }
            if ocr:
                entry["ocr_text"] = ocr.ocr_text or ""
                entry["ocr_confidence"] = round(ocr.confidence, 3)
                entry["ocr_engine"] = ocr.engine
            files.append(entry)
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


@protected.get("/extracted-files")
async def list_extracted_files(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    extension: str | None = None,
    sort: str = Query("newest", regex="^(newest|oldest)$"),
    db: AsyncSession = Depends(get_session),
):
    """List all extracted files with pagination."""
    order = ExtractedFile.created_at.asc() if sort == "oldest" else ExtractedFile.created_at.desc()
    stmt = (
        select(ExtractedFile, RawMention.metadata_.label("mention_meta"))
        .join(RawMention, RawMention.id == ExtractedFile.mention_id)
        .order_by(order)
    )
    count_stmt = select(func.count()).select_from(ExtractedFile)

    if extension:
        stmt = stmt.where(ExtractedFile.extension == extension)
        count_stmt = count_stmt.where(ExtractedFile.extension == extension)

    total = (await db.execute(count_stmt)).scalar() or 0
    result = await db.execute(stmt.offset((page - 1) * page_size).limit(page_size))
    rows = result.all()

    files = []
    for row in rows:
        ef = row[0]
        mention_meta = row[1] or {}
        files.append({
            "id": ef.id,
            "mention_id": ef.mention_id,
            "filename": ef.filename,
            "size": ef.size or 0,
            "extension": ef.extension,
            "is_text": ef.is_text,
            "s3_key": ef.s3_key,
            "sha256": ef.sha256,
            "archive_name": mention_meta.get("file_name", ""),
            "created_at": ef.created_at.isoformat() if ef.created_at else None,
        })

    return {"items": files, "total": total, "page": page, "page_size": page_size}


@protected.get("/extracted-files/search")
async def search_extracted_files(
    q: str = Query(..., min_length=1),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_session),
):
    """Global search across all extracted file contents and filenames.

    Searches both the ExtractedFile table (FTS + filename ILIKE) and
    falls back to raw_mentions content for archives without ExtractedFile rows.
    Results include the parent mention's archive info for grouping.
    """
    files_result: list[dict] = []
    total = 0

    # 1. Search ExtractedFile table (FTS on content + ILIKE on filename)
    content_match = ExtractedFile.content_tsvector.match(q)
    name_match = ExtractedFile.filename.ilike(f"%{q}%")
    stmt = (
        select(ExtractedFile, RawMention.metadata_.label("mention_meta"))
        .join(RawMention, RawMention.id == ExtractedFile.mention_id)
        .where(or_(content_match, name_match))
        .order_by(ExtractedFile.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.all()

    count_stmt = (
        select(func.count())
        .select_from(ExtractedFile)
        .where(or_(content_match, name_match))
    )
    total = (await db.execute(count_stmt)).scalar() or 0

    # Bulk-fetch OCR cache for search results
    search_sha_list = [row[0].sha256 for row in rows if row[0].sha256]
    search_ocr_map: dict[str, ImageOCRCache] = {}
    if search_sha_list:
        ocr_r = await db.execute(
            select(ImageOCRCache).where(ImageOCRCache.sha256.in_(search_sha_list))
        )
        search_ocr_map = {r.sha256: r for r in ocr_r.scalars().all()}

    for row in rows:
        ef = row[0]
        mention_meta = row[1] or {}
        entry = {
            "id": ef.id,
            "mention_id": ef.mention_id,
            "filename": ef.filename,
            "size": ef.size or 0,
            "extension": ef.extension,
            "is_text": ef.is_text,
            "preview": (ef.text_content or "")[:500],
            "s3_key": ef.s3_key,
            "archive_name": mention_meta.get("file_name", ""),
            "source": "extracted_files",
        }
        ocr = search_ocr_map.get(ef.sha256) if ef.sha256 else None
        if ocr:
            entry["ocr_text"] = ocr.ocr_text or ""
            entry["ocr_confidence"] = round(ocr.confidence, 3)
            entry["ocr_engine"] = ocr.engine
        files_result.append(entry)

    # 2. Fallback: search raw_mentions content for archives without ExtractedFile rows
    # (only if we haven't hit the limit from ExtractedFile results)
    if len(files_result) < limit:
        remaining = limit - len(files_result)
        mention_ids_with_ef = select(ExtractedFile.mention_id).distinct().scalar_subquery()
        mention_stmt = (
            select(RawMention)
            .where(
                RawMention.content.ilike(f"%{q}%"),
                RawMention.metadata_["s3_key"].astext.isnot(None),
                RawMention.id.notin_(mention_ids_with_ef),
            )
            .order_by(RawMention.collected_at.desc())
            .limit(remaining)
        )
        mention_result = await db.execute(mention_stmt)
        for mention in mention_result.scalars().all():
            meta = mention.metadata_ or {}
            files_result.append({
                "id": mention.id,
                "mention_id": mention.id,
                "filename": meta.get("file_name", "unknown"),
                "size": meta.get("file_size", 0),
                "extension": "",
                "is_text": True,
                "preview": _extract_context(mention.content or "", q, 250),
                "s3_key": meta.get("s3_key", ""),
                "archive_name": meta.get("file_name", ""),
                "source": "mention_content",
            })
            total += 1

    return {
        "query": q,
        "total": total,
        "files": files_result,
    }


def _extract_context(text: str, query: str, context_chars: int = 250) -> str:
    """Extract a snippet around the first match of query in text."""
    idx = text.lower().find(query.lower())
    if idx == -1:
        return text[:context_chars]
    start = max(0, idx - context_chars // 2)
    end = min(len(text), idx + len(query) + context_chars // 2)
    snippet = text[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(text):
        snippet = snippet + "..."
    return snippet


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
        subtitle=getattr(body, 'subtitle', 'Dark Web Threat Intelligence Report'),
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

@router.get("/mentions/{mention_id}/file")
async def get_mention_file(
    mention_id: str,
    _user=Depends(get_current_user_or_token_param),
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


@router.get("/files/{s3_key:path}")
async def serve_s3_file(
    s3_key: str,
    _user=Depends(get_current_user_or_token_param),
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


# ---- Hex Dump --------------------------------------------------------------


@protected.get("/hex-dump")
async def hex_dump_file(
    s3_key: str = Query(..., description="S3 key of the file"),
    limit: int = Query(4096, ge=16, le=65536),
):
    """Return hex dump of a file from S3, limited to first `limit` bytes."""
    import boto3
    from botocore.config import Config
    from darkdisco.config import settings
    from darkdisco.pipeline.files import detect_mime_type, hex_dump

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4"),
    )

    try:
        # Only fetch the bytes we need
        obj = s3.get_object(
            Bucket=settings.s3_bucket,
            Key=s3_key,
            Range=f"bytes=0-{limit - 1}",
        )
        data = obj["Body"].read()
    except Exception:
        raise HTTPException(404, "File not found in storage")

    # Get total size from a HEAD request
    try:
        head = s3.head_object(Bucket=settings.s3_bucket, Key=s3_key)
        total_size = head.get("ContentLength", len(data))
    except Exception:
        total_size = len(data)

    filename = s3_key.rsplit("/", 1)[-1]
    mime = detect_mime_type(filename, data)

    return {
        "s3_key": s3_key,
        "filename": filename,
        "mime_type": mime,
        "total_size": total_size,
        "dump_size": min(limit, total_size),
        "hex_dump": hex_dump(data, limit),
    }


# ---- Findings --------------------------------------------------------------

@protected.get("/findings", response_model=PaginatedFindingsOut)
async def list_findings(
    institution_id: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    q: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    base_filters = []
    if institution_id is not None:
        inst_ids = [v.strip() for v in institution_id.split(",") if v.strip()]
        if len(inst_ids) == 1:
            base_filters.append(Finding.institution_id == inst_ids[0])
        elif inst_ids:
            base_filters.append(Finding.institution_id.in_(inst_ids))
    if severity is not None:
        sev_vals = [v.strip() for v in severity.split(",") if v.strip()]
        if len(sev_vals) == 1:
            base_filters.append(Finding.severity == sev_vals[0])
        elif sev_vals:
            base_filters.append(Finding.severity.in_(sev_vals))
    if status is not None:
        stat_vals = [v.strip() for v in status.split(",") if v.strip()]
        if len(stat_vals) == 1:
            base_filters.append(Finding.status == stat_vals[0])
        elif stat_vals:
            base_filters.append(Finding.status.in_(stat_vals))
    if date_from is not None:
        base_filters.append(Finding.discovered_at >= date_from)
    if date_to is not None:
        base_filters.append(Finding.discovered_at <= date_to)
    if q:
        like_pattern = f"%{q}%"
        base_filters.append(
            or_(
                Finding.title.ilike(like_pattern),
                Finding.summary.ilike(like_pattern),
                Finding.raw_content.ilike(like_pattern),
            )
        )

    # Total count
    count_stmt = select(func.count(Finding.id))
    for f in base_filters:
        count_stmt = count_stmt.where(f)
    total = (await db.execute(count_stmt)).scalar() or 0

    # Paginated results
    stmt = (
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .order_by(Finding.discovered_at.desc())
    )
    for f in base_filters:
        stmt = stmt.where(f)
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(stmt)

    return PaginatedFindingsOut(
        items=result.scalars().all(),
        total=total,
        page=page,
        page_size=page_size,
    )


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


@protected.get("/findings/search", response_model=PaginatedFindingsOut)
async def search_findings(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_session),
):
    """Full-text-ish search across finding title, summary, and raw_content."""
    like_pattern = f"%{q}%"
    search_filter = or_(
        Finding.title.ilike(like_pattern),
        Finding.summary.ilike(like_pattern),
        Finding.raw_content.ilike(like_pattern),
    )

    total = (await db.execute(
        select(func.count(Finding.id)).where(search_filter)
    )).scalar() or 0

    stmt = (
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .where(search_filter)
        .order_by(Finding.discovered_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)

    return PaginatedFindingsOut(
        items=result.scalars().all(),
        total=total,
        page=page,
        page_size=page_size,
    )


@protected.get("/findings/classifications")
async def list_classifications(
    db: AsyncSession = Depends(get_session),
):
    """Return distinct classification values for autocomplete suggestions."""
    result = await db.execute(
        select(Finding.classification)
        .where(Finding.classification.isnot(None))
        .where(Finding.classification != "")
        .distinct()
        .order_by(Finding.classification)
    )
    return [row[0] for row in result.all()]


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
            # Bulk-fetch OCR cache entries
            sha_list = [ef.sha256 for ef in extracted_rows if ef.sha256]
            ocr_map: dict[str, ImageOCRCache] = {}
            if sha_list:
                ocr_res = await db.execute(
                    select(ImageOCRCache).where(ImageOCRCache.sha256.in_(sha_list))
                )
                ocr_map = {r.sha256: r for r in ocr_res.scalars().all()}
            files = []
            for ef in extracted_rows:
                ocr = ocr_map.get(ef.sha256) if ef.sha256 else None
                entry: dict = {
                    "filename": ef.filename,
                    "size": ef.size or 0,
                    "preview": (ef.text_content or "")[:500],
                    "content": ef.text_content or "",
                    "s3_key": ef.s3_key,
                    "sha256": ef.sha256,
                    "extension": ef.extension,
                    "is_text": ef.is_text,
                }
                if ocr:
                    entry["ocr_text"] = ocr.ocr_text or ""
                    entry["ocr_confidence"] = round(ocr.confidence, 3)
                    entry["ocr_engine"] = ocr.engine
                files.append(entry)
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
    current_user: User = Depends(get_current_user),
):
    finding = await db.get(Finding, finding_id)
    if not finding:
        raise HTTPException(404, "Finding not found")
    updates = body.model_dump(exclude_unset=True, by_alias=False)
    # If status is being changed, validate transition
    if "status" in updates and updates["status"] != finding.status:
        _check_transition(finding.status, updates["status"])

    username = current_user.username

    # Audit tracked fields
    audit_fields = {
        "status": ("status_change", lambda v: v.value if hasattr(v, "value") else str(v)),
        "severity": ("severity_change", lambda v: v.value if hasattr(v, "value") else str(v)),
        "classification": ("classification_change", str),
    }
    for field_name, (action, fmt) in audit_fields.items():
        if field_name in updates:
            old_val = getattr(finding, field_name)
            new_val = updates[field_name]
            if old_val != new_val:
                db.add(FindingAuditLog(
                    finding_id=finding_id,
                    action=action,
                    username=username,
                    field=field_name,
                    old_value=fmt(old_val) if old_val is not None else None,
                    new_value=fmt(new_val) if new_val is not None else None,
                ))

    # Analyst notes: append-only behavior
    if "analyst_notes" in updates and updates["analyst_notes"]:
        new_note = updates["analyst_notes"]
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        formatted = f"[{timestamp}] {username}: {new_note}"
        existing = finding.analyst_notes or ""
        separator = "\n---\n" if existing else ""
        updates["analyst_notes"] = existing + separator + formatted
        db.add(FindingAuditLog(
            finding_id=finding_id,
            action="note_added",
            username=username,
            new_value=new_note,
        ))

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
    current_user: User = Depends(get_current_user),
):
    """Explicit status transition with validation."""
    finding = await db.get(Finding, finding_id)
    if not finding:
        raise HTTPException(404, "Finding not found")
    _check_transition(finding.status, body.status)
    username = current_user.username
    old_status = finding.status.value
    finding.status = body.status
    db.add(FindingAuditLog(
        finding_id=finding_id,
        action="status_change",
        username=username,
        field="status",
        old_value=old_status,
        new_value=body.status.value,
    ))
    if body.notes:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        formatted = f"[{timestamp}] {username}: {body.notes}"
        existing = finding.analyst_notes or ""
        separator = "\n---\n" if existing else ""
        finding.analyst_notes = existing + separator + formatted
        db.add(FindingAuditLog(
            finding_id=finding_id,
            action="note_added",
            username=username,
            new_value=body.notes,
        ))
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


@protected.get("/findings/{finding_id}/audit-log", response_model=list[FindingAuditLogOut])
async def get_finding_audit_log(
    finding_id: str,
    db: AsyncSession = Depends(get_session),
):
    """Return audit log entries for a finding, ordered chronologically."""
    finding = await db.get(Finding, finding_id)
    if not finding:
        raise HTTPException(404, "Finding not found")
    result = await db.execute(
        select(FindingAuditLog)
        .where(FindingAuditLog.finding_id == finding_id)
        .order_by(FindingAuditLog.created_at.asc())
    )
    return result.scalars().all()


@protected.post("/findings/{finding_id}/notes", response_model=FindingOut)
async def add_finding_note(
    finding_id: str,
    body: FindingNoteAdd,
    db: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """Append a note to a finding's analyst_notes (append-only thread)."""
    finding = await db.get(Finding, finding_id)
    if not finding:
        raise HTTPException(404, "Finding not found")
    username = current_user.username
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    formatted = f"[{timestamp}] {username}: {body.content}"
    existing = finding.analyst_notes or ""
    separator = "\n---\n" if existing else ""
    finding.analyst_notes = existing + separator + formatted
    db.add(FindingAuditLog(
        finding_id=finding_id,
        action="note_added",
        username=username,
        new_value=body.content,
    ))
    await db.commit()
    result = await db.execute(
        select(Finding)
        .options(selectinload(Finding.institution), selectinload(Finding.source))
        .where(Finding.id == finding_id)
    )
    return result.scalar_one()


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
        subtitle=getattr(body, 'subtitle', 'Dark Web Threat Intelligence Report'),
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


# ---------------------------------------------------------------------------
# OCR Stats
# ---------------------------------------------------------------------------


@protected.get("/ocr-stats")
async def get_ocr_stats(
    db: AsyncSession = Depends(get_session),
):
    """Return OCR processing statistics: total cached, avg confidence, recent results."""
    # Total cached images
    total_cached = (await db.execute(
        select(func.count()).select_from(ImageOCRCache)
    )).scalar() or 0

    # Average confidence
    avg_confidence = (await db.execute(
        select(func.avg(ImageOCRCache.confidence))
    )).scalar() or 0.0

    # Count mentions with OCR text (cache hits = mentions that matched a cached hash)
    mentions_with_ocr = (await db.execute(
        select(func.count()).select_from(RawMention).where(
            RawMention.metadata_["ocr_text"].astext.isnot(None),
            RawMention.metadata_.has_key("ocr_text"),
        )
    )).scalar() or 0

    # Recent OCR cache entries
    recent_stmt = (
        select(ImageOCRCache)
        .order_by(ImageOCRCache.created_at.desc())
        .limit(10)
    )
    recent_result = await db.execute(recent_stmt)
    recent = [
        {
            "sha256": r.sha256[:12] + "...",
            "text_preview": (r.ocr_text or "")[:120],
            "confidence": round(r.confidence, 3),
            "engine": r.engine,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in recent_result.scalars().all()
    ]

    return {
        "total_cached": total_cached,
        "mentions_with_ocr": mentions_with_ocr,
        "avg_confidence": round(float(avg_confidence), 3),
        "recent": recent,
    }


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

@protected.post("/reports/generate")
async def generate_report_pdf(
    body: ReportRequest,
    db: AsyncSession = Depends(get_session),
):
    """Generate a PDF report and return it as a download."""
    from darkdisco.reporting.engine import generate_pdf

    pdf_bytes = await generate_pdf(
        db,
        title=body.title,
        subtitle=getattr(body, 'subtitle', 'Dark Web Threat Intelligence Report'),
        date_from=body.date_from,
        date_to=body.date_to,
        client_id=body.client_id,
        institution_id=body.institution_id,
        severities=body.severities,
        statuses=body.statuses,
        sections=body.sections.model_dump(),
        chart_options=body.charts.model_dump(),
        truncate_content=body.truncate_content,
    )

    filename = f"darkdisco-report-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')}.pdf"
    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@protected.post("/reports/preview")
async def preview_report_html(
    body: ReportRequest,
    db: AsyncSession = Depends(get_session),
):
    """Generate an HTML preview of the report."""
    from darkdisco.reporting.engine import render_report_html

    html = await render_report_html(
        db,
        title=body.title,
        subtitle=getattr(body, 'subtitle', 'Dark Web Threat Intelligence Report'),
        date_from=body.date_from,
        date_to=body.date_to,
        client_id=body.client_id,
        institution_id=body.institution_id,
        severities=body.severities,
        statuses=body.statuses,
        sections=body.sections.model_dump(),
        chart_options=body.charts.model_dump(),
        truncate_content=body.truncate_content,
    )

    return StreamingResponse(
        iter([html.encode()]),
        media_type="text/html",
    )


# ---------------------------------------------------------------------------
# Report Templates
# ---------------------------------------------------------------------------

@protected.get("/reports/templates", response_model=list[ReportTemplateOut])
async def list_report_templates(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """List report templates owned by the current user."""
    result = await db.execute(
        select(ReportTemplate)
        .where(ReportTemplate.owner_id == user.id)
        .order_by(ReportTemplate.updated_at.desc())
    )
    return result.scalars().all()


@protected.post("/reports/templates", response_model=ReportTemplateOut, status_code=201)
async def create_report_template(
    body: ReportTemplateCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """Save current report configuration as a named template."""
    template = ReportTemplate(
        name=body.name,
        description=body.description,
        owner_id=user.id,
        config=body.config.model_dump(),
    )
    db.add(template)
    await db.commit()
    await db.refresh(template)
    return template


@protected.get("/reports/templates/{template_id}", response_model=ReportTemplateOut)
async def get_report_template(
    template_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """Get a specific report template."""
    template = await db.get(ReportTemplate, template_id)
    if not template or template.owner_id != user.id:
        raise HTTPException(status_code=404, detail="Template not found")
    return template


@protected.put("/reports/templates/{template_id}", response_model=ReportTemplateOut)
async def update_report_template(
    template_id: str,
    body: ReportTemplateUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """Update a report template."""
    template = await db.get(ReportTemplate, template_id)
    if not template or template.owner_id != user.id:
        raise HTTPException(status_code=404, detail="Template not found")
    if body.name is not None:
        template.name = body.name
    if body.description is not None:
        template.description = body.description
    if body.config is not None:
        template.config = body.config.model_dump()
    await db.commit()
    await db.refresh(template)
    return template


@protected.delete("/reports/templates/{template_id}", status_code=204)
async def delete_report_template(
    template_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """Delete a report template."""
    template = await db.get(ReportTemplate, template_id)
    if not template or template.owner_id != user.id:
        raise HTTPException(status_code=404, detail="Template not found")
    await db.delete(template)
    await db.commit()


# ---------------------------------------------------------------------------
# Report Schedules
# ---------------------------------------------------------------------------

@protected.get("/reports/schedules", response_model=list[ReportScheduleOut])
async def list_report_schedules(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """List report schedules owned by the current user."""
    result = await db.execute(
        select(ReportSchedule)
        .where(ReportSchedule.owner_id == user.id)
        .order_by(ReportSchedule.created_at.desc())
    )
    return result.scalars().all()


@protected.post("/reports/schedules", response_model=ReportScheduleOut, status_code=201)
async def create_report_schedule(
    body: ReportScheduleCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """Create a new report schedule."""
    # Verify template exists and belongs to user
    template = await db.get(ReportTemplate, body.template_id)
    if not template or template.owner_id != user.id:
        raise HTTPException(status_code=404, detail="Template not found")

    if not body.cron_expression and not body.interval_seconds:
        raise HTTPException(
            status_code=422,
            detail="Either cron_expression or interval_seconds must be provided",
        )

    # Compute initial next_run_at
    now = datetime.now(timezone.utc)
    next_run: datetime | None = None
    if body.interval_seconds:
        next_run = now + timedelta(seconds=body.interval_seconds)
    elif body.cron_expression:
        try:
            from croniter import croniter
            cron = croniter(body.cron_expression, now)
            next_run = cron.get_next(datetime)
        except Exception:
            raise HTTPException(status_code=422, detail="Invalid cron expression")

    schedule = ReportSchedule(
        template_id=body.template_id,
        owner_id=user.id,
        name=body.name,
        cron_expression=body.cron_expression,
        interval_seconds=body.interval_seconds,
        date_range_mode=body.date_range_mode,
        enabled=body.enabled,
        delivery_method=body.delivery_method,
        recipients=body.recipients,
        next_run_at=next_run if body.enabled else None,
    )
    db.add(schedule)
    await db.commit()
    await db.refresh(schedule)
    return schedule


@protected.get("/reports/schedules/{schedule_id}", response_model=ReportScheduleOut)
async def get_report_schedule(
    schedule_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """Get a specific report schedule."""
    schedule = await db.get(ReportSchedule, schedule_id)
    if not schedule or schedule.owner_id != user.id:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return schedule


@protected.put("/reports/schedules/{schedule_id}", response_model=ReportScheduleOut)
async def update_report_schedule(
    schedule_id: str,
    body: ReportScheduleUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """Update a report schedule."""
    schedule = await db.get(ReportSchedule, schedule_id)
    if not schedule or schedule.owner_id != user.id:
        raise HTTPException(status_code=404, detail="Schedule not found")

    if body.name is not None:
        schedule.name = body.name
    if body.template_id is not None:
        template = await db.get(ReportTemplate, body.template_id)
        if not template or template.owner_id != user.id:
            raise HTTPException(status_code=404, detail="Template not found")
        schedule.template_id = body.template_id
    if body.cron_expression is not None:
        schedule.cron_expression = body.cron_expression
    if body.interval_seconds is not None:
        schedule.interval_seconds = body.interval_seconds
    if body.date_range_mode is not None:
        schedule.date_range_mode = body.date_range_mode
    if body.delivery_method is not None:
        schedule.delivery_method = body.delivery_method
    if body.recipients is not None:
        schedule.recipients = body.recipients
    if body.enabled is not None:
        schedule.enabled = body.enabled
        if body.enabled and not schedule.next_run_at:
            # Re-compute next run when re-enabling
            now = datetime.now(timezone.utc)
            if schedule.interval_seconds:
                schedule.next_run_at = now + timedelta(seconds=schedule.interval_seconds)
            elif schedule.cron_expression:
                try:
                    from croniter import croniter
                    cron = croniter(schedule.cron_expression, now)
                    schedule.next_run_at = cron.get_next(datetime)
                except Exception:
                    pass
        elif not body.enabled:
            schedule.next_run_at = None

    await db.commit()
    await db.refresh(schedule)
    return schedule


@protected.delete("/reports/schedules/{schedule_id}", status_code=204)
async def delete_report_schedule(
    schedule_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """Delete a report schedule."""
    schedule = await db.get(ReportSchedule, schedule_id)
    if not schedule or schedule.owner_id != user.id:
        raise HTTPException(status_code=404, detail="Schedule not found")
    await db.delete(schedule)
    await db.commit()


# ---------------------------------------------------------------------------
# Generated Reports
# ---------------------------------------------------------------------------

@protected.get("/reports/generated", response_model=list[GeneratedReportOut])
async def list_generated_reports(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
    schedule_id: str | None = Query(None),
    limit: int = Query(50, le=200),
):
    """List generated reports for the current user."""
    stmt = (
        select(GeneratedReport)
        .where(GeneratedReport.owner_id == user.id)
        .order_by(GeneratedReport.created_at.desc())
        .limit(limit)
    )
    if schedule_id:
        stmt = stmt.where(GeneratedReport.schedule_id == schedule_id)
    result = await db.execute(stmt)
    return result.scalars().all()


@protected.get("/reports/generated/{report_id}/download")
async def download_generated_report(
    report_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_session),
):
    """Download a generated report PDF from S3."""
    import boto3

    report = await db.get(GeneratedReport, report_id)
    if not report or report.owner_id != user.id:
        raise HTTPException(status_code=404, detail="Report not found")
    if report.status != "completed" or not report.s3_key:
        raise HTTPException(status_code=404, detail="Report not available")

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )
    try:
        obj = s3.get_object(Bucket=settings.s3_bucket, Key=report.s3_key)
        pdf_bytes = obj["Body"].read()
    except Exception:
        raise HTTPException(status_code=404, detail="Report file not found in storage")

    filename = f"report-{report.created_at.strftime('%Y%m%d-%H%M')}.pdf"
    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# BIN Database
# ---------------------------------------------------------------------------


@protected.get("/bins/lookup/{prefix}", response_model=BINLookupResponse)
async def bin_lookup(prefix: str, db: AsyncSession = Depends(get_session)):
    """Look up a BIN prefix (6-8 digits) and return issuer information."""
    prefix = prefix.strip()
    if not prefix.isdigit() or len(prefix) < 6 or len(prefix) > 8:
        raise HTTPException(status_code=400, detail="BIN prefix must be 6-8 digits")

    # Try exact prefix match
    record = (await db.execute(
        select(BINRecord).where(BINRecord.bin_prefix == prefix).limit(1)
    )).scalar_one_or_none()

    # Fall back to 6-digit if 8-digit not found
    if not record and len(prefix) == 8:
        record = (await db.execute(
            select(BINRecord).where(BINRecord.bin_prefix == prefix[:6]).limit(1)
        )).scalar_one_or_none()

    # Try range-based lookup
    if not record:
        record = (await db.execute(
            select(BINRecord).where(
                BINRecord.bin_range_start.isnot(None),
                BINRecord.bin_range_end.isnot(None),
                BINRecord.bin_range_start <= prefix,
                BINRecord.bin_range_end >= prefix,
            ).limit(1)
        )).scalar_one_or_none()

    if not record:
        return BINLookupResponse(bin_prefix=prefix, found=False)

    return BINLookupResponse(
        bin_prefix=prefix,
        found=True,
        issuer_name=record.issuer_name,
        card_brand=record.card_brand.value if record.card_brand else None,
        card_type=record.card_type.value if record.card_type else None,
        card_level=record.card_level,
        country_code=record.country_code,
        country_name=record.country_name,
        bank_url=record.bank_url,
        bank_phone=record.bank_phone,
    )


@protected.get("/bins/search", response_model=list[BINRecordOut])
async def bin_search(
    q: str = Query(default="", description="Search by issuer name, prefix, or country"),
    brand: str | None = Query(default=None),
    country: str | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_session),
):
    """Search the BIN database with filters."""
    query = select(BINRecord)

    if q:
        query = query.where(
            or_(
                BINRecord.bin_prefix.startswith(q),
                BINRecord.issuer_name.ilike(f"%{q}%"),
                BINRecord.country_name.ilike(f"%{q}%"),
            )
        )

    if brand:
        query = query.where(BINRecord.card_brand == brand)

    if country:
        query = query.where(
            or_(
                BINRecord.country_code == country.upper(),
                BINRecord.country_name.ilike(f"%{country}%"),
            )
        )

    query = query.order_by(BINRecord.bin_prefix).offset(offset).limit(limit)
    results = (await db.execute(query)).scalars().all()

    return [BINRecordOut.model_validate(r) for r in results]


@protected.get("/bins/stats", response_model=BINStatsResponse)
async def bin_stats(db: AsyncSession = Depends(get_session)):
    """Get BIN database statistics."""
    total = (await db.execute(select(func.count(BINRecord.id)))).scalar() or 0

    # By brand
    brand_rows = (await db.execute(
        select(BINRecord.card_brand, func.count(BINRecord.id))
        .group_by(BINRecord.card_brand)
        .order_by(func.count(BINRecord.id).desc())
    )).all()
    by_brand = {(r[0].value if r[0] else "unknown"): r[1] for r in brand_rows}

    # By source
    source_rows = (await db.execute(
        select(BINRecord.source, func.count(BINRecord.id))
        .group_by(BINRecord.source)
        .order_by(func.count(BINRecord.id).desc())
    )).all()
    by_source = {(r[0] or "unknown"): r[1] for r in source_rows}

    # Top countries
    country_rows = (await db.execute(
        select(BINRecord.country_name, BINRecord.country_code, func.count(BINRecord.id))
        .where(BINRecord.country_name.isnot(None))
        .group_by(BINRecord.country_name, BINRecord.country_code)
        .order_by(func.count(BINRecord.id).desc())
        .limit(20)
    )).all()
    by_country = [
        {"name": r[0], "code": r[1], "count": r[2]}
        for r in country_rows
    ]

    # Top issuers
    issuer_rows = (await db.execute(
        select(BINRecord.issuer_name, func.count(BINRecord.id))
        .where(BINRecord.issuer_name.isnot(None))
        .group_by(BINRecord.issuer_name)
        .order_by(func.count(BINRecord.id).desc())
        .limit(20)
    )).all()
    top_issuers = [
        {"name": r[0], "count": r[1]}
        for r in issuer_rows
    ]

    return BINStatsResponse(
        total_records=total,
        by_brand=by_brand,
        by_source=by_source,
        by_country=by_country,
        top_issuers=top_issuers,
    )


@protected.post("/bins/import", response_model=BINImportResponse)
async def bin_import(
    file: UploadFile = File(...),
    source_label: str = Query(default="csv", description="Source label for tracking"),
):
    """Import BIN records from CSV or PDF file."""
    content = await file.read()
    filename = file.filename or ""

    if filename.lower().endswith(".pdf"):
        from darkdisco.pipeline.bin_import import import_pdf
        result = import_pdf(content, source_label=source_label)
    elif filename.lower().endswith(".csv") or filename.lower().endswith(".tsv"):
        from darkdisco.pipeline.bin_import import import_csv
        result = import_csv(content, source_label=source_label)
    else:
        # Try CSV by default
        from darkdisco.pipeline.bin_import import import_csv
        result = import_csv(content, source_label=source_label)

    return BINImportResponse(
        imported=result.imported,
        updated=result.updated,
        skipped=result.skipped,
        errors=result.errors[:50],  # cap error list
        source=result.source,
    )

