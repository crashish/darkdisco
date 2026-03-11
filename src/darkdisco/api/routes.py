"""DarkDisco API routes — full CRUD for all core entities."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from darkdisco.api.schemas import (
    ClientCreate,
    ClientOut,
    ClientUpdate,
    DashboardStats,
    FindingCreate,
    FindingOut,
    FindingStatusTransition,
    FindingUpdate,
    InstitutionCreate,
    InstitutionOut,
    InstitutionUpdate,
    SeverityCount,
    SourceCreate,
    SourceOut,
    SourceUpdate,
    StatusCount,
    WatchTermCreate,
    WatchTermOut,
    WatchTermUpdate,
)
from darkdisco.common.database import get_session
from darkdisco.common.models import (
    Client,
    Finding,
    FindingStatus,
    Institution,
    Severity,
    Source,
    WatchTerm,
)

router = APIRouter()

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


# ---- Health ----------------------------------------------------------------

@router.get("/health")
async def health():
    return {"status": "ok"}


# ---- Clients ---------------------------------------------------------------

@router.get("/clients", response_model=list[ClientOut])
async def list_clients(
    active: bool | None = None,
    db: AsyncSession = Depends(get_session),
):
    stmt = select(Client).order_by(Client.name)
    if active is not None:
        stmt = stmt.where(Client.active == active)
    result = await db.execute(stmt)
    return result.scalars().all()


@router.post("/clients", response_model=ClientOut, status_code=201)
async def create_client(
    body: ClientCreate,
    db: AsyncSession = Depends(get_session),
):
    client = Client(**body.model_dump())
    db.add(client)
    await db.commit()
    await db.refresh(client)
    return client


@router.get("/clients/{client_id}", response_model=ClientOut)
async def get_client(
    client_id: str,
    db: AsyncSession = Depends(get_session),
):
    client = await db.get(Client, client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    return client


@router.put("/clients/{client_id}", response_model=ClientOut)
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


@router.delete("/clients/{client_id}", status_code=204)
async def delete_client(
    client_id: str,
    db: AsyncSession = Depends(get_session),
):
    client = await db.get(Client, client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    await db.delete(client)
    await db.commit()


# ---- Institutions ----------------------------------------------------------

@router.get("/institutions", response_model=list[InstitutionOut])
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


@router.post("/institutions", response_model=InstitutionOut, status_code=201)
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
    return inst


@router.get("/institutions/{institution_id}", response_model=InstitutionOut)
async def get_institution(
    institution_id: str,
    db: AsyncSession = Depends(get_session),
):
    inst = await db.get(Institution, institution_id)
    if not inst:
        raise HTTPException(404, "Institution not found")
    return inst


@router.put("/institutions/{institution_id}", response_model=InstitutionOut)
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
    return inst


@router.delete("/institutions/{institution_id}", status_code=204)
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

@router.get("/watch-terms", response_model=list[WatchTermOut])
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


@router.post("/watch-terms", response_model=WatchTermOut, status_code=201)
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


@router.get("/watch-terms/{term_id}", response_model=WatchTermOut)
async def get_watch_term(
    term_id: str,
    db: AsyncSession = Depends(get_session),
):
    wt = await db.get(WatchTerm, term_id)
    if not wt:
        raise HTTPException(404, "Watch term not found")
    return wt


@router.put("/watch-terms/{term_id}", response_model=WatchTermOut)
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


@router.delete("/watch-terms/{term_id}", status_code=204)
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

@router.get("/sources", response_model=list[SourceOut])
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
    return result.scalars().all()


@router.post("/sources", response_model=SourceOut, status_code=201)
async def create_source(
    body: SourceCreate,
    db: AsyncSession = Depends(get_session),
):
    source = Source(**body.model_dump())
    db.add(source)
    await db.commit()
    await db.refresh(source)
    return source


@router.get("/sources/{source_id}", response_model=SourceOut)
async def get_source(
    source_id: str,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    return source


@router.put("/sources/{source_id}", response_model=SourceOut)
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
    return source


@router.delete("/sources/{source_id}", status_code=204)
async def delete_source(
    source_id: str,
    db: AsyncSession = Depends(get_session),
):
    source = await db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    await db.delete(source)
    await db.commit()


# ---- Findings --------------------------------------------------------------

@router.get("/findings", response_model=list[FindingOut])
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
    stmt = select(Finding).order_by(Finding.discovered_at.desc())
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


@router.post("/findings", response_model=FindingOut, status_code=201)
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
    await db.refresh(finding)
    return finding


@router.get("/findings/search", response_model=list[FindingOut])
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


@router.get("/findings/{finding_id}", response_model=FindingOut)
async def get_finding(
    finding_id: str,
    db: AsyncSession = Depends(get_session),
):
    finding = await db.get(Finding, finding_id)
    if not finding:
        raise HTTPException(404, "Finding not found")
    return finding


@router.put("/findings/{finding_id}", response_model=FindingOut)
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
    await db.refresh(finding)
    return finding


@router.post("/findings/{finding_id}/transition", response_model=FindingOut)
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
    await db.refresh(finding)
    return finding


@router.delete("/findings/{finding_id}", status_code=204)
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

@router.get("/dashboard/stats", response_model=DashboardStats)
async def dashboard_stats(
    institution_id: str | None = None,
    db: AsyncSession = Depends(get_session),
):
    base = select(Finding)
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

    return DashboardStats(
        total_findings=total,
        by_severity=by_severity,
        by_status=by_status,
        recent_findings=recent_rows,
    )
