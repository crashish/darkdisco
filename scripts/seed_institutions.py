#!/usr/bin/env python3
"""Seed DarkDisco database with top 100 US community/regional banks and credit unions.

Usage:
    python scripts/seed_institutions.py

Idempotent — checks for existing records before inserting.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

# Allow running from repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from darkdisco.common.models import (
    Base,
    Client,
    Finding,
    FindingStatus,
    Institution,
    Severity,
    Source,
    SourceType,
    WatchTerm,
    WatchTermType,
)
from darkdisco.config import settings

# ---------------------------------------------------------------------------
# Institution data
# ---------------------------------------------------------------------------

# Each entry: (name, short_name, charter_type, state, domain, [additional_domains])
# charter_type: "credit_union" or "bank"

CREDIT_UNIONS: list[tuple[str, str, str, str, list[str]]] = [
    ("Navy Federal Credit Union", "Navy Federal", "VA", "navyfcu.org", []),
    ("State Employees' Credit Union", "SECU", "NC", "ncsecu.org", []),
    ("Pentagon Federal Credit Union", "PenFed", "VA", "penfed.org", []),
    ("SchoolsFirst Federal Credit Union", "SchoolsFirst", "CA", "schoolsfirstfcu.org", []),
    ("Boeing Employees Credit Union", "BECU", "WA", "becu.org", []),
    ("America First Credit Union", "America First", "UT", "americafirst.com", []),
    ("Alliant Credit Union", "Alliant", "IL", "alliantcreditunion.org", ["alliantcu.com"]),
    ("Mountain America Credit Union", "Mountain America", "UT", "macu.com", []),
    ("Golden 1 Credit Union", "Golden 1", "CA", "golden1.com", []),
    ("Suncoast Credit Union", "Suncoast", "FL", "suncoastcreditunion.com", []),
    ("Randolph-Brooks Federal Credit Union", "RBFCU", "TX", "rbfcu.org", []),
    ("First Technology Federal Credit Union", "First Tech", "CA", "firsttechfed.com", []),
    ("VyStar Credit Union", "VyStar", "FL", "vystarcu.org", []),
    ("Lake Michigan Credit Union", "LMCU", "MI", "lmcu.org", []),
    ("Security Service Federal Credit Union", "SSFCU", "TX", "ssfcu.org", []),
    ("Bethpage Federal Credit Union", "Bethpage", "NY", "bethpagefcu.com", []),
    ("Digital Federal Credit Union", "DCU", "MA", "dcu.org", []),
    ("Idaho Central Credit Union", "ICCU", "ID", "iccu.com", []),
    ("Global Credit Union", "Global CU", "AK", "globalcu.org", []),
    ("GreenState Credit Union", "GreenState", "IA", "greenstate.org", []),
    ("San Diego County Credit Union", "SDCCU", "CA", "sdccu.com", []),
    ("Ent Credit Union", "Ent", "CO", "ent.com", []),
    ("Logix Federal Credit Union", "Logix", "CA", "logixbanking.com", []),
    ("Teachers Federal Credit Union", "Teachers FCU", "NY", "teachersfcu.org", []),
    ("Star One Credit Union", "Star One", "CA", "starone.org", []),
    ("OnPoint Community Credit Union", "OnPoint", "OR", "onpointcu.com", []),
    ("ESL Federal Credit Union", "ESL", "NY", "esl.org", []),
    ("United Nations Federal Credit Union", "UNFCU", "NY", "unfcu.com", []),
    ("Patelco Credit Union", "Patelco", "CA", "patelco.org", []),
    ("Wings Financial Credit Union", "Wings Financial", "MN", "wingsfinancial.com", []),
    ("Police and Fire Federal Credit Union", "PFFCU", "PA", "pffcu.org", []),
    ("American Airlines Federal Credit Union", "AAFCU", "TX", "aaborcu.com", []),
    ("Broadview Federal Credit Union", "Broadview", "NY", "broadviewfcu.com", []),
    ("Eastman Credit Union", "Eastman CU", "TN", "eastmancu.org", []),
    ("Desert Financial Credit Union", "Desert Financial", "AZ", "desertfinancial.com", []),
    ("Wright-Patt Credit Union", "WPCU", "OH", "wpcu.coop", []),
    ("Redwood Credit Union", "Redwood CU", "CA", "redwoodcu.org", []),
    ("Space Coast Credit Union", "Space Coast", "FL", "sccu.com", []),
    ("Delta Community Credit Union", "Delta Community", "GA", "deltacommunitycu.com", []),
    ("Pennsylvania State Employees Credit Union", "PSECU", "PA", "psecu.com", []),
    ("Bellco Credit Union", "Bellco", "CO", "bellco.org", []),
    ("Michigan State University Federal Credit Union", "MSUFCU", "MI", "msufcu.org", []),
    ("MIDFLORIDA Credit Union", "MIDFLORIDA", "FL", "midflorida.com", []),
    ("Members 1st Federal Credit Union", "Members 1st", "PA", "members1st.org", []),
    ("Redstone Federal Credit Union", "Redstone", "AL", "redfcu.org", []),
    ("Citizens Equity First Credit Union", "CEFCU", "IL", "cefcu.com", []),
    ("Veridian Credit Union", "Veridian", "IA", "veridiancu.org", []),
    ("Summit Credit Union", "Summit CU", "WI", "summitcreditunion.com", []),
    ("Hudson Valley Federal Credit Union", "HVFCU", "NY", "hvfcu.org", []),
    ("Virginia Credit Union", "VACU", "VA", "vacu.org", []),
]

COMMUNITY_BANKS: list[tuple[str, str, str, str, list[str]]] = [
    # Large regional banks ($10B-$50B assets)
    ("Bank OZK", "OZK", "AR", "ozk.com", []),
    ("Prosperity Bank", "Prosperity", "TX", "prosperitybankusa.com", []),
    ("Hancock Whitney Bank", "Hancock Whitney", "MS", "hancockwhitney.com", []),
    ("Commerce Bank", "Commerce", "MO", "commercebank.com", []),
    ("MidFirst Bank", "MidFirst", "OK", "midfirst.com", []),
    ("BankUnited", "BankUnited", "FL", "bankunited.com", []),
    ("United Bank", "United Bank", "WV", "bankwithunited.com", []),
    ("Cadence Bank", "Cadence", "MS", "cadencebank.com", []),
    ("First National Bank of Pennsylvania", "FNB", "PA", "fnb-online.com", []),
    ("Associated Bank", "Associated", "WI", "associatedbank.com", []),
    ("South State Bank", "South State", "SC", "southstatebank.com", []),
    ("Pinnacle Financial Partners", "Pinnacle Bank", "TN", "pnfp.com", ["pinnaclebank.com"]),
    ("Glacier Bank", "Glacier", "MT", "glacierbank.com", []),
    ("First Interstate Bank", "First Interstate", "MT", "firstinterstatebank.com", []),
    ("Old National Bank", "Old National", "IN", "oldnational.com", []),
    ("UMB Bank", "UMB", "MO", "umb.com", []),
    ("Simmons Bank", "Simmons", "AR", "simmonsbank.com", []),
    ("Banner Bank", "Banner", "WA", "bannerbank.com", []),
    ("Pacific Premier Bank", "Pacific Premier", "CA", "ppbi.com", ["ppbi.com"]),
    ("Fulton Bank", "Fulton", "PA", "fultonbank.com", []),
    # Mid-size community banks ($3B-$10B assets)
    ("Trustmark National Bank", "Trustmark", "MS", "trustmark.com", []),
    ("WesBanco Bank", "WesBanco", "WV", "wesbanco.com", []),
    ("Berkshire Hills Bancorp", "Berkshire Bank", "MA", "berkshirebank.com", []),
    ("Valley National Bank", "Valley National", "NJ", "valley.com", []),
    ("Independent Bank", "Independent", "MI", "independentbank.com", []),
    ("Renasant Bank", "Renasant", "MS", "renasantbank.com", []),
    ("First Financial Bankshares", "First Financial", "TX", "ffin.com", []),
    ("International Bancshares", "IBC Bank", "TX", "ibc.com", []),
    ("Columbia Banking System", "Columbia Bank", "WA", "columbiabank.com", []),
    ("Sandy Spring Bank", "Sandy Spring", "MD", "sandyspringbank.com", []),
    ("Centier Bank", "Centier", "IN", "centier.com", []),
    ("S&T Bank", "S&T", "PA", "stbank.com", []),
    ("NBT Bank", "NBT", "NY", "nbtbank.com", []),
    ("Community Bank N.A.", "Community Bank", "NY", "cbna.com", []),
    ("CVB Financial Corp", "Citizens Business Bank", "CA", "cbbank.com", []),
    ("Lakeland Bank", "Lakeland", "NJ", "lakelandbank.com", []),
    ("Heartland BancCorp", "Heartland Bank", "OH", "heartlandbank.com", []),
    ("First Busey Bank", "Busey Bank", "IL", "busey.com", []),
    ("Glacier Hills Credit Union", "Glacier Hills", "WI", "glacierhillscu.org", []),
    ("Home Federal Savings Bank", "Home Federal", "LA", "homefederal.com", []),
    # Smaller community banks ($1B-$3B assets)
    ("Glacier Bancorp Mountain West", "Mountain West Bank", "ID", "mountainwestbank.com", []),
    ("TowneBank", "TowneBank", "VA", "townebank.com", []),
    ("Brookline Bank", "Brookline", "MA", "brooklinebank.com", []),
    ("First Savings Financial Group", "First Savings", "IN", "fsbbank.net", []),
    ("Carter Bank and Trust", "Carter Bank", "VA", "carterbankandtrust.com", []),
    ("Seacoast Banking Corp", "Seacoast Bank", "FL", "seacoastbanking.com", []),
    ("Enterprise Bancorp", "Enterprise Bank", "MA", "enterprisebanking.com", []),
    ("First Bancshares", "The First Bank", "MS", "thefirstbank.com", []),
    ("Heritage Financial Corp", "Heritage Bank", "WA", "heritagebanknw.com", []),
    ("Veritex Community Bank", "Veritex", "TX", "veritexbank.com", []),
]


def _generate_watch_terms(
    inst_id: str,
    name: str,
    short_name: str,
    domain: str,
    additional_domains: list[str],
) -> list[WatchTerm]:
    """Generate watch terms for an institution."""
    terms: list[WatchTerm] = []

    # Full legal name
    terms.append(
        WatchTerm(
            id=str(uuid4()),
            institution_id=inst_id,
            term_type=WatchTermType.institution_name,
            value=name,
            enabled=True,
            case_sensitive=False,
            notes="Full legal name",
        )
    )

    # Short / common name (if different from full name)
    if short_name and short_name.lower() != name.lower():
        terms.append(
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst_id,
                term_type=WatchTermType.institution_name,
                value=short_name,
                enabled=True,
                case_sensitive=False,
                notes="Common short name",
            )
        )

    # Primary domain
    terms.append(
        WatchTerm(
            id=str(uuid4()),
            institution_id=inst_id,
            term_type=WatchTermType.domain,
            value=domain,
            enabled=True,
            case_sensitive=False,
            notes="Primary domain",
        )
    )

    # Additional domains
    for d in additional_domains:
        terms.append(
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst_id,
                term_type=WatchTermType.domain,
                value=d,
                enabled=True,
                case_sensitive=False,
                notes="Additional domain",
            )
        )

    # Keyword variations — strip common suffixes for broader matching
    for suffix in ("Credit Union", "Federal Credit Union", "Bank", "National Bank"):
        if name.endswith(suffix) and name != suffix:
            base = name[: -len(suffix)].strip()
            if base and base.lower() != short_name.lower() and len(base) > 3:
                terms.append(
                    WatchTerm(
                        id=str(uuid4()),
                        institution_id=inst_id,
                        term_type=WatchTermType.keyword,
                        value=base,
                        enabled=True,
                        case_sensitive=False,
                        notes=f"Name without '{suffix}' suffix",
                    )
                )
                break  # Only the first match

    return terms


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

DEFAULT_SOURCES = [
    {
        "name": "BreachForums Tor Mirror",
        "source_type": SourceType.forum,
        "url": "http://breachforums.example.onion",
        "connector_class": "darkdisco.connectors.tor_forum.TorForumConnector",
        "poll_interval_seconds": 1800,
        "config": {"forum_engine": "xenforo", "sections": ["databases", "combos"]},
    },
    {
        "name": "Dread Market Forum",
        "source_type": SourceType.forum,
        "url": "http://dread.example.onion",
        "connector_class": "darkdisco.connectors.tor_forum.TorForumConnector",
        "poll_interval_seconds": 3600,
        "config": {"forum_engine": "custom", "sections": ["fraud", "banking"]},
    },
    {
        "name": "Pastebin Scraper",
        "source_type": SourceType.paste_site,
        "url": "https://pastebin.com",
        "connector_class": "darkdisco.connectors.paste_site.PasteSiteConnector",
        "poll_interval_seconds": 600,
        "config": {"api_key_env": "PASTEBIN_API_KEY"},
    },
    {
        "name": "Rentry Paste Monitor",
        "source_type": SourceType.paste_site,
        "url": "https://rentry.co",
        "connector_class": "darkdisco.connectors.paste_site.PasteSiteConnector",
        "poll_interval_seconds": 900,
        "config": {},
    },
    {
        "name": "Telegram Channel Monitor",
        "source_type": SourceType.telegram,
        "url": None,
        "connector_class": "darkdisco.connectors.telegram.TelegramConnector",
        "poll_interval_seconds": 300,
        "config": {
            "channels": [
                "bank_leaks_demo",
                "combolist_channel_demo",
                "stealer_logs_demo",
            ]
        },
    },
    {
        "name": "DeHashed Breach Database",
        "source_type": SourceType.breach_db,
        "url": "https://dehashed.com",
        "connector_class": "darkdisco.connectors.breach_db.DehashedConnector",
        "poll_interval_seconds": 7200,
        "config": {"api_key_env": "DEHASHED_API_KEY"},
    },
    {
        "name": "Have I Been Pwned Monitor",
        "source_type": SourceType.breach_db,
        "url": "https://haveibeenpwned.com",
        "connector_class": "darkdisco.connectors.breach_db.HIBPConnector",
        "poll_interval_seconds": 3600,
        "config": {"api_key_env": "HIBP_API_KEY"},
    },
    {
        "name": "LockBit Ransomware Blog",
        "source_type": SourceType.ransomware_blog,
        "url": "http://lockbit.example.onion",
        "connector_class": "darkdisco.connectors.ransomware_blog.RansomwareBlogConnector",
        "poll_interval_seconds": 1800,
        "config": {"group": "lockbit"},
    },
    {
        "name": "ALPHV/BlackCat Blog",
        "source_type": SourceType.ransomware_blog,
        "url": "http://alphv.example.onion",
        "connector_class": "darkdisco.connectors.ransomware_blog.RansomwareBlogConnector",
        "poll_interval_seconds": 1800,
        "config": {"group": "alphv"},
    },
    {
        "name": "Stealer Log Aggregator",
        "source_type": SourceType.stealer_log,
        "url": None,
        "connector_class": "darkdisco.connectors.stealer_log.StealerLogConnector",
        "poll_interval_seconds": 3600,
        "config": {"parsers": ["redline", "raccoon", "vidar"]},
    },
]


# ---------------------------------------------------------------------------
# Sample findings for demo
# ---------------------------------------------------------------------------


def _build_sample_findings(
    institutions: dict[str, str],
    sources: dict[str, str],
) -> list[dict]:
    """Return sample finding dicts.  institutions = {name: id}, sources = {name: id}."""
    now = datetime.now(timezone.utc)
    return [
        {
            "institution_name": "Navy Federal Credit Union",
            "source_name": "BreachForums Tor Mirror",
            "severity": Severity.critical,
            "status": FindingStatus.new,
            "title": "Alleged Navy Federal member database for sale (250K records)",
            "summary": (
                "Threat actor 'CreditPhantom' posted on BreachForums offering "
                "a database allegedly containing 250,000 Navy Federal member records "
                "including names, SSNs, and account numbers. Price: $15,000 XMR. "
                "Sample of 500 records provided as proof."
            ),
            "raw_content": "[SALE] Navy Federal CU Full DB - 250K records\n\nFields: name, ssn, dob, address, phone, email, account_no, balance\nProof: https://anonfiles.example/sample_500.csv\nPrice: $15,000 XMR\nContact: Telegram @CreditPhantom",
            "matched_terms": ["Navy Federal Credit Union", "navyfcu.org", "Navy Federal"],
            "tags": ["pii", "database_sale", "financial_data"],
            "discovered_at": now - timedelta(hours=2),
        },
        {
            "institution_name": "BECU",
            "source_name": "Stealer Log Aggregator",
            "severity": Severity.high,
            "status": FindingStatus.reviewing,
            "title": "1,200+ BECU online banking credentials in Redline stealer logs",
            "summary": (
                "Batch of Redline stealer logs from Jan 2026 contains 1,247 unique "
                "credential pairs for becu.org online banking portal. Logs include "
                "saved passwords, cookies, and autofill data."
            ),
            "raw_content": "Redline batch 2026-01-15\nDomain: becu.org\nUnique creds: 1,247\nLog source: Telegram distribution",
            "matched_terms": ["becu.org", "BECU"],
            "tags": ["credentials", "stealer_log", "online_banking"],
            "discovered_at": now - timedelta(hours=18),
        },
        {
            "institution_name": "Prosperity Bank",
            "source_name": "LockBit Ransomware Blog",
            "severity": Severity.critical,
            "status": FindingStatus.escalated,
            "title": "Prosperity Bank listed on LockBit ransomware blog",
            "summary": (
                "LockBit 3.0 blog lists Prosperity Bank as a victim with a countdown "
                "timer of 9 days. Threat actors claim to have exfiltrated 85 GB of data "
                "including customer PII, loan documents, and internal communications."
            ),
            "raw_content": "LockBit 3.0 Blog Entry\nVictim: Prosperity Bank (prosperitybankusa.com)\nData: 85 GB\nDeadline: 9 days\nSample files: internal_memo_2025.pdf, customer_export_q4.xlsx",
            "matched_terms": ["Prosperity Bank", "prosperitybankusa.com"],
            "tags": ["ransomware", "lockbit", "data_exfiltration"],
            "discovered_at": now - timedelta(days=1),
        },
        {
            "institution_name": "Golden 1 Credit Union",
            "source_name": "Pastebin Scraper",
            "severity": Severity.medium,
            "status": FindingStatus.new,
            "title": "Golden 1 employee email/password combo list on Pastebin",
            "summary": (
                "Paste containing 47 email/password combinations with @golden1.com "
                "domain. Appears to be from a third-party breach rather than direct "
                "compromise. Passwords are in plaintext."
            ),
            "raw_content": "=== golden1.com corporate emails ===\nj.smith@golden1.com:Summer2025!\nm.jones@golden1.com:G0lden1CU#\n... (47 total entries)",
            "matched_terms": ["golden1.com", "Golden 1"],
            "tags": ["credentials", "employee_data", "combo_list"],
            "discovered_at": now - timedelta(days=2),
        },
        {
            "institution_name": "Suncoast Credit Union",
            "source_name": "Telegram Channel Monitor",
            "severity": Severity.high,
            "status": FindingStatus.new,
            "title": "Suncoast CU phishing kit being distributed on Telegram",
            "summary": (
                "A phishing kit mimicking the Suncoast Credit Union login portal is "
                "being sold on Telegram for $200. Kit includes responsive HTML, "
                "credential harvesting backend, and SMS 2FA interception module."
            ),
            "raw_content": "NEW KIT: Suncoast Credit Union\nPanel: PHP + MySQL\nFeatures: OTP grab, real-time relay, antibot\nScreens: login, verify identity, security questions\nPrice: $200 USDT\nDemo: suncoast-demo.example.com",
            "matched_terms": ["Suncoast Credit Union", "suncoastcreditunion.com"],
            "tags": ["phishing_kit", "brand_abuse", "telegram"],
            "discovered_at": now - timedelta(hours=6),
        },
        {
            "institution_name": "Fulton Bank",
            "source_name": "DeHashed Breach Database",
            "severity": Severity.medium,
            "status": FindingStatus.resolved,
            "title": "312 Fulton Bank employee records in third-party breach",
            "summary": (
                "DeHashed query for fultonbank.com returned 312 records from the "
                "2024 MOVEit breach dataset. Records contain employee emails, "
                "hashed passwords, and job titles."
            ),
            "raw_content": "DeHashed results for fultonbank.com: 312 records\nSource breach: MOVEit_2024\nFields: email, password_hash, name, title",
            "matched_terms": ["fultonbank.com", "Fulton Bank"],
            "tags": ["breach_data", "employee_data", "third_party"],
            "discovered_at": now - timedelta(days=5),
        },
        {
            "institution_name": "PenFed",
            "source_name": "BreachForums Tor Mirror",
            "severity": Severity.low,
            "status": FindingStatus.false_positive,
            "title": "Mention of PenFed in generic fraud discussion thread",
            "summary": (
                "Forum thread discussing general carding techniques mentions PenFed "
                "in passing. No specific data or targeted attack. Marked as false positive."
            ),
            "raw_content": "Thread: Best CU cards for cashing out?\n> penfed cards are harder now, they added step-up auth\n> try smaller CUs instead",
            "matched_terms": ["PenFed", "penfed.org"],
            "tags": ["discussion", "carding"],
            "discovered_at": now - timedelta(days=3),
        },
        {
            "institution_name": "Simmons Bank",
            "source_name": "ALPHV/BlackCat Blog",
            "severity": Severity.high,
            "status": FindingStatus.new,
            "title": "Simmons Bank vendor listed on ALPHV ransomware blog",
            "summary": (
                "A third-party IT vendor serving Simmons Bank appears on the ALPHV blog. "
                "Threat actors claim access to managed banking infrastructure. "
                "Potential supply chain risk."
            ),
            "raw_content": "ALPHV Blog\nVictim: TechServe Solutions (vendor for Simmons Bank, Banner Bank)\nData: 40 GB internal + client data\nDeadline: 12 days",
            "matched_terms": ["Simmons Bank", "simmonsbank.com"],
            "tags": ["ransomware", "supply_chain", "vendor_compromise"],
            "discovered_at": now - timedelta(hours=36),
        },
    ]


# ---------------------------------------------------------------------------
# Main seeding logic
# ---------------------------------------------------------------------------


async def seed(db_url: str | None = None) -> None:
    url = db_url or settings.database_url
    engine = create_async_engine(url, echo=False, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Ensure tables exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        # ---- 1. Default client ----
        client_id = await _ensure_client(session)

        # ---- 2. Institutions + watch terms ----
        institution_ids: dict[str, str] = {}  # name -> id

        print(f"\n--- Seeding {len(CREDIT_UNIONS)} credit unions ---")
        for name, short, state, domain, extra_domains in CREDIT_UNIONS:
            inst_id = await _ensure_institution(
                session,
                client_id=client_id,
                name=name,
                short_name=short,
                charter_type="credit_union",
                state=state,
                domain=domain,
                additional_domains=extra_domains,
            )
            institution_ids[name] = inst_id
            # Also register by short name for finding lookup
            institution_ids[short] = inst_id

        print(f"\n--- Seeding {len(COMMUNITY_BANKS)} community/regional banks ---")
        for name, short, state, domain, extra_domains in COMMUNITY_BANKS:
            inst_id = await _ensure_institution(
                session,
                client_id=client_id,
                name=name,
                short_name=short,
                charter_type="bank",
                state=state,
                domain=domain,
                additional_domains=extra_domains,
            )
            institution_ids[name] = inst_id
            institution_ids[short] = inst_id

        # ---- 3. Sources ----
        print(f"\n--- Seeding {len(DEFAULT_SOURCES)} sources ---")
        source_ids: dict[str, str] = {}
        for src in DEFAULT_SOURCES:
            src_id = await _ensure_source(session, **src)
            source_ids[src["name"]] = src_id

        # ---- 4. Sample findings ----
        sample_findings = _build_sample_findings(institution_ids, source_ids)
        print(f"\n--- Seeding {len(sample_findings)} sample findings ---")
        for f_data in sample_findings:
            await _ensure_finding(session, institution_ids, source_ids, f_data)

        await session.commit()

    await engine.dispose()
    print("\nSeed complete.")


async def _ensure_client(session: AsyncSession) -> str:
    name = "DarkDisco Monitoring Service"
    result = await session.execute(select(Client).where(Client.name == name))
    client = result.scalars().first()
    if client:
        print(f"  [exists] Client: {name}")
        return client.id
    client = Client(
        id=str(uuid4()),
        name=name,
        contract_ref="DD-SEED-001",
        active=True,
        notes="Default seed client for demo/development",
    )
    session.add(client)
    await session.flush()
    print(f"  [created] Client: {name}")
    return client.id


async def _ensure_institution(
    session: AsyncSession,
    *,
    client_id: str,
    name: str,
    short_name: str,
    charter_type: str,
    state: str,
    domain: str,
    additional_domains: list[str],
) -> str:
    result = await session.execute(
        select(Institution).where(
            Institution.client_id == client_id,
            Institution.name == name,
        )
    )
    inst = result.scalars().first()
    if inst:
        print(f"  [exists] {charter_type:12s} | {name}")
        return inst.id

    inst_id = str(uuid4())
    inst = Institution(
        id=inst_id,
        client_id=client_id,
        name=name,
        short_name=short_name,
        charter_type=charter_type,
        state=state,
        primary_domain=domain,
        additional_domains=additional_domains if additional_domains else None,
        active=True,
    )
    session.add(inst)

    # Generate and add watch terms
    terms = _generate_watch_terms(inst_id, name, short_name, domain, additional_domains)
    for t in terms:
        session.add(t)

    await session.flush()
    print(f"  [created] {charter_type:12s} | {name} ({len(terms)} watch terms)")
    return inst_id


async def _ensure_source(
    session: AsyncSession,
    *,
    name: str,
    source_type: SourceType,
    url: str | None,
    connector_class: str | None,
    poll_interval_seconds: int,
    config: dict | None,
) -> str:
    result = await session.execute(select(Source).where(Source.name == name))
    src = result.scalars().first()
    if src:
        print(f"  [exists] Source: {name}")
        return src.id

    src = Source(
        id=str(uuid4()),
        name=name,
        source_type=source_type,
        url=url,
        connector_class=connector_class,
        enabled=True,
        poll_interval_seconds=poll_interval_seconds,
        config=config,
    )
    session.add(src)
    await session.flush()
    print(f"  [created] Source: {name} ({source_type.value})")
    return src.id


async def _ensure_finding(
    session: AsyncSession,
    institution_ids: dict[str, str],
    source_ids: dict[str, str],
    data: dict,
) -> None:
    # Compute a content hash for dedup
    content_hash = hashlib.sha256(
        (data["title"] + (data.get("raw_content") or "")).encode()
    ).hexdigest()

    result = await session.execute(
        select(Finding).where(Finding.content_hash == content_hash)
    )
    if result.scalars().first():
        print(f"  [exists] Finding: {data['title'][:60]}...")
        return

    inst_name = data.pop("institution_name")
    source_name = data.pop("source_name")
    inst_id = institution_ids.get(inst_name)
    source_id = source_ids.get(source_name)

    if not inst_id:
        print(f"  [skip] Finding references unknown institution: {inst_name}")
        return

    finding = Finding(
        id=str(uuid4()),
        institution_id=inst_id,
        source_id=source_id,
        severity=data["severity"],
        status=data["status"],
        title=data["title"],
        summary=data.get("summary"),
        raw_content=data.get("raw_content"),
        content_hash=content_hash,
        matched_terms=data.get("matched_terms"),
        tags=data.get("tags"),
        discovered_at=data.get("discovered_at"),
    )
    session.add(finding)
    await session.flush()
    print(f"  [created] Finding: {data['title'][:60]}...")


if __name__ == "__main__":
    asyncio.run(seed())
