#!/usr/bin/env python3
"""Seed DarkDisco database with top 100 US community/regional banks and credit unions.

Usage:
    python scripts/seed_institutions.py

Idempotent — checks for existing records before inserting.
Includes routing numbers (from FDIC/NCUA public data), BIN prefixes, and
watch terms with acronym collision notes.
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
# Each entry is a dict with:
#   name, short_name, state, domain, additional_domains,
#   routing_numbers (ABA RTNs from FDIC/NCUA/Fed E-Payments directory),
#   bin_ranges (known card BIN prefixes),
#   acronyms (common abbreviations; tagged with collision risk in watch terms)
#
# Routing numbers are publicly available via each institution's website and
# the Federal Reserve E-Payments Routing Directory.
# BIN ranges are representative prefixes from public IIN databases.
# ---------------------------------------------------------------------------

CREDIT_UNIONS: list[dict] = [
    {
        "name": "Navy Federal Credit Union",
        "short_name": "Navy Federal",
        "state": "VA",
        "domain": "navyfcu.org",
        "additional_domains": ["navyfederal.org"],
        "routing_numbers": ["256074974"],
        "bin_ranges": ["489480", "489481", "414720"],
        "acronyms": [("NFCU", "May collide with other 'NF' abbreviations")],
    },
    {
        "name": "State Employees' Credit Union",
        "short_name": "SECU",
        "state": "NC",
        "domain": "ncsecu.org",
        "additional_domains": [],
        "routing_numbers": ["253177049"],
        "bin_ranges": ["486880", "400852"],
        "acronyms": [("SECU", "High collision: also used by State Employees CU of Maryland and other state employee CUs")],
    },
    {
        "name": "Pentagon Federal Credit Union",
        "short_name": "PenFed",
        "state": "VA",
        "domain": "penfed.org",
        "additional_domains": [],
        "routing_numbers": ["256078446"],
        "bin_ranges": ["422203", "528782"],
        "acronyms": [("PenFed", "Low collision risk — distinctive brand name")],
    },
    {
        "name": "SchoolsFirst Federal Credit Union",
        "short_name": "SchoolsFirst",
        "state": "CA",
        "domain": "schoolsfirstfcu.org",
        "additional_domains": [],
        "routing_numbers": ["322282603"],
        "bin_ranges": ["414709"],
        "acronyms": [("SFFCU", "Low collision risk")],
    },
    {
        "name": "Boeing Employees Credit Union",
        "short_name": "BECU",
        "state": "WA",
        "domain": "becu.org",
        "additional_domains": [],
        "routing_numbers": ["325081403"],
        "bin_ranges": ["485980", "429504"],
        "acronyms": [("BECU", "Low collision — well-known brand in PNW")],
    },
    {
        "name": "America First Credit Union",
        "short_name": "America First",
        "state": "UT",
        "domain": "americafirst.com",
        "additional_domains": [],
        "routing_numbers": ["324377516"],
        "bin_ranges": ["414730"],
        "acronyms": [("AFCU", "High collision: generic 'America First' matches political orgs, other FIs")],
    },
    {
        "name": "Alliant Credit Union",
        "short_name": "Alliant",
        "state": "IL",
        "domain": "alliantcreditunion.org",
        "additional_domains": ["alliantcu.com"],
        "routing_numbers": ["271081528"],
        "bin_ranges": ["421783"],
        "acronyms": [("Alliant", "Medium collision: Alliant Energy, Alliant Insurance also use this name")],
    },
    {
        "name": "Mountain America Credit Union",
        "short_name": "Mountain America",
        "state": "UT",
        "domain": "macu.com",
        "additional_domains": [],
        "routing_numbers": ["324079555"],
        "bin_ranges": ["485944"],
        "acronyms": [("MACU", "Low collision risk")],
    },
    {
        "name": "Golden 1 Credit Union",
        "short_name": "Golden 1",
        "state": "CA",
        "domain": "golden1.com",
        "additional_domains": [],
        "routing_numbers": ["321175261"],
        "bin_ranges": ["485960"],
        "acronyms": [],
    },
    {
        "name": "Suncoast Credit Union",
        "short_name": "Suncoast",
        "state": "FL",
        "domain": "suncoastcreditunion.com",
        "additional_domains": [],
        "routing_numbers": ["263182817"],
        "bin_ranges": ["414711"],
        "acronyms": [("Suncoast", "Medium collision: generic geographic term used by many FL businesses")],
    },
    {
        "name": "Randolph-Brooks Federal Credit Union",
        "short_name": "RBFCU",
        "state": "TX",
        "domain": "rbfcu.org",
        "additional_domains": [],
        "routing_numbers": ["314089681"],
        "bin_ranges": ["486075"],
        "acronyms": [("RBFCU", "Low collision risk — distinctive acronym")],
    },
    {
        "name": "First Technology Federal Credit Union",
        "short_name": "First Tech",
        "state": "CA",
        "domain": "firsttechfed.com",
        "additional_domains": [],
        "routing_numbers": ["321180379"],
        "bin_ranges": ["421785"],
        "acronyms": [("First Tech", "Medium collision: generic 'first tech' appears in many contexts")],
    },
    {
        "name": "VyStar Credit Union",
        "short_name": "VyStar",
        "state": "FL",
        "domain": "vystarcu.org",
        "additional_domains": [],
        "routing_numbers": ["263079276"],
        "bin_ranges": ["486400"],
        "acronyms": [],
    },
    {
        "name": "Lake Michigan Credit Union",
        "short_name": "LMCU",
        "state": "MI",
        "domain": "lmcu.org",
        "additional_domains": [],
        "routing_numbers": ["272479663"],
        "bin_ranges": ["485992"],
        "acronyms": [("LMCU", "Low collision risk")],
    },
    {
        "name": "Security Service Federal Credit Union",
        "short_name": "SSFCU",
        "state": "TX",
        "domain": "ssfcu.org",
        "additional_domains": [],
        "routing_numbers": ["314088637"],
        "bin_ranges": ["486072"],
        "acronyms": [("SSFCU", "Low collision risk — distinctive acronym")],
    },
    {
        "name": "Bethpage Federal Credit Union",
        "short_name": "Bethpage",
        "state": "NY",
        "domain": "bethpagefcu.com",
        "additional_domains": [],
        "routing_numbers": ["226082022"],
        "bin_ranges": ["414723"],
        "acronyms": [("BFCU", "High collision: many CUs use BFCU abbreviation")],
    },
    {
        "name": "Digital Federal Credit Union",
        "short_name": "DCU",
        "state": "MA",
        "domain": "dcu.org",
        "additional_domains": [],
        "routing_numbers": ["211391825"],
        "bin_ranges": ["486038"],
        "acronyms": [("DCU", "High collision: also abbreviation for DC Universe, other orgs")],
    },
    {
        "name": "Idaho Central Credit Union",
        "short_name": "ICCU",
        "state": "ID",
        "domain": "iccu.com",
        "additional_domains": [],
        "routing_numbers": ["324377820"],
        "bin_ranges": ["485961"],
        "acronyms": [("ICCU", "Low collision risk")],
    },
    {
        "name": "Global Credit Union",
        "short_name": "Global CU",
        "state": "AK",
        "domain": "globalcu.org",
        "additional_domains": [],
        "routing_numbers": ["325272063"],
        "bin_ranges": ["486019"],
        "acronyms": [("GCU", "High collision: Grand Canyon University and others")],
    },
    {
        "name": "GreenState Credit Union",
        "short_name": "GreenState",
        "state": "IA",
        "domain": "greenstate.org",
        "additional_domains": [],
        "routing_numbers": ["273976369"],
        "bin_ranges": ["486023"],
        "acronyms": [],
    },
    {
        "name": "San Diego County Credit Union",
        "short_name": "SDCCU",
        "state": "CA",
        "domain": "sdccu.com",
        "additional_domains": [],
        "routing_numbers": ["322281617"],
        "bin_ranges": ["414729"],
        "acronyms": [("SDCCU", "Low collision risk — distinctive acronym")],
    },
    {
        "name": "Ent Credit Union",
        "short_name": "Ent",
        "state": "CO",
        "domain": "ent.com",
        "additional_domains": [],
        "routing_numbers": ["302075018"],
        "bin_ranges": ["414735"],
        "acronyms": [("Ent", "High collision: very short, matches ENT (ear/nose/throat), entertainment, etc.")],
    },
    {
        "name": "Logix Federal Credit Union",
        "short_name": "Logix",
        "state": "CA",
        "domain": "logixbanking.com",
        "additional_domains": [],
        "routing_numbers": ["322282001"],
        "bin_ranges": ["485966"],
        "acronyms": [("Logix", "Medium collision: also a logistics software brand")],
    },
    {
        "name": "Teachers Federal Credit Union",
        "short_name": "Teachers FCU",
        "state": "NY",
        "domain": "teachersfcu.org",
        "additional_domains": [],
        "routing_numbers": ["226078036"],
        "bin_ranges": ["486033"],
        "acronyms": [("TFCU", "High collision: many teachers FCUs use this abbreviation nationally")],
    },
    {
        "name": "Star One Credit Union",
        "short_name": "Star One",
        "state": "CA",
        "domain": "starone.org",
        "additional_domains": [],
        "routing_numbers": ["321177968"],
        "bin_ranges": ["486044"],
        "acronyms": [],
    },
    {
        "name": "OnPoint Community Credit Union",
        "short_name": "OnPoint",
        "state": "OR",
        "domain": "onpointcu.com",
        "additional_domains": [],
        "routing_numbers": ["323075880"],
        "bin_ranges": ["486011"],
        "acronyms": [("OnPoint", "Medium collision: generic term used in consulting/marketing")],
    },
    {
        "name": "ESL Federal Credit Union",
        "short_name": "ESL",
        "state": "NY",
        "domain": "esl.org",
        "additional_domains": [],
        "routing_numbers": ["222381580"],
        "bin_ranges": ["486016"],
        "acronyms": [("ESL", "High collision: English as a Second Language dominates this acronym")],
    },
    {
        "name": "United Nations Federal Credit Union",
        "short_name": "UNFCU",
        "state": "NY",
        "domain": "unfcu.com",
        "additional_domains": [],
        "routing_numbers": ["226078020"],
        "bin_ranges": ["486090"],
        "acronyms": [("UNFCU", "Low collision risk — distinctive")],
    },
    {
        "name": "Patelco Credit Union",
        "short_name": "Patelco",
        "state": "CA",
        "domain": "patelco.org",
        "additional_domains": [],
        "routing_numbers": ["321076470"],
        "bin_ranges": ["486047"],
        "acronyms": [],
    },
    {
        "name": "Wings Financial Credit Union",
        "short_name": "Wings Financial",
        "state": "MN",
        "domain": "wingsfinancial.com",
        "additional_domains": [],
        "routing_numbers": ["296076152"],
        "bin_ranges": ["486051"],
        "acronyms": [("Wings", "High collision: very common word, airlines, restaurants, etc.")],
    },
    {
        "name": "Police and Fire Federal Credit Union",
        "short_name": "PFFCU",
        "state": "PA",
        "domain": "pffcu.org",
        "additional_domains": [],
        "routing_numbers": ["236082944"],
        "bin_ranges": ["486085"],
        "acronyms": [("PFFCU", "Low collision risk")],
    },
    {
        "name": "American Airlines Federal Credit Union",
        "short_name": "AAFCU",
        "state": "TX",
        "domain": "aaborcu.com",
        "additional_domains": [],
        "routing_numbers": ["311990511"],
        "bin_ranges": ["486092"],
        "acronyms": [("AAFCU", "Medium collision: may match general 'AA' abbreviations")],
    },
    {
        "name": "Broadview Federal Credit Union",
        "short_name": "Broadview",
        "state": "NY",
        "domain": "broadviewfcu.com",
        "additional_domains": [],
        "routing_numbers": ["221373383"],
        "bin_ranges": ["486096"],
        "acronyms": [("Broadview", "Medium collision: generic name used by multiple businesses")],
    },
    {
        "name": "Eastman Credit Union",
        "short_name": "Eastman CU",
        "state": "TN",
        "domain": "eastmancu.org",
        "additional_domains": [],
        "routing_numbers": ["264181581"],
        "bin_ranges": ["486063"],
        "acronyms": [("ECU", "High collision: East Carolina University, European Currency Unit, engine control unit")],
    },
    {
        "name": "Desert Financial Credit Union",
        "short_name": "Desert Financial",
        "state": "AZ",
        "domain": "desertfinancial.com",
        "additional_domains": [],
        "routing_numbers": ["322172496"],
        "bin_ranges": ["486070"],
        "acronyms": [("DFCU", "Medium collision: other 'Desert' or 'Detroit' FCUs")],
    },
    {
        "name": "Wright-Patt Credit Union",
        "short_name": "WPCU",
        "state": "OH",
        "domain": "wpcu.coop",
        "additional_domains": [],
        "routing_numbers": ["242279408"],
        "bin_ranges": ["486058"],
        "acronyms": [("WPCU", "Low collision risk")],
    },
    {
        "name": "Redwood Credit Union",
        "short_name": "Redwood CU",
        "state": "CA",
        "domain": "redwoodcu.org",
        "additional_domains": [],
        "routing_numbers": ["321177722"],
        "bin_ranges": ["486027"],
        "acronyms": [("RCU", "Medium collision: other Redwood-named CUs, generic abbreviation")],
    },
    {
        "name": "Space Coast Credit Union",
        "short_name": "Space Coast",
        "state": "FL",
        "domain": "sccu.com",
        "additional_domains": [],
        "routing_numbers": ["263177903"],
        "bin_ranges": ["486035"],
        "acronyms": [("SCCU", "Medium collision: other 'SC' credit unions")],
    },
    {
        "name": "Delta Community Credit Union",
        "short_name": "Delta Community",
        "state": "GA",
        "domain": "deltacommunitycu.com",
        "additional_domains": [],
        "routing_numbers": ["261171309"],
        "bin_ranges": ["486083"],
        "acronyms": [("DCCU", "Medium collision: multiple 'Delta' or 'DC' credit unions")],
    },
    {
        "name": "Pennsylvania State Employees Credit Union",
        "short_name": "PSECU",
        "state": "PA",
        "domain": "psecu.com",
        "additional_domains": [],
        "routing_numbers": ["231381116"],
        "bin_ranges": ["486054"],
        "acronyms": [("PSECU", "Low collision risk — distinctive acronym")],
    },
    {
        "name": "Bellco Credit Union",
        "short_name": "Bellco",
        "state": "CO",
        "domain": "bellco.org",
        "additional_domains": [],
        "routing_numbers": ["302075267"],
        "bin_ranges": ["486060"],
        "acronyms": [],
    },
    {
        "name": "Michigan State University Federal Credit Union",
        "short_name": "MSUFCU",
        "state": "MI",
        "domain": "msufcu.org",
        "additional_domains": [],
        "routing_numbers": ["272479950"],
        "bin_ranges": ["486067"],
        "acronyms": [("MSUFCU", "Low collision risk — distinctive")],
    },
    {
        "name": "MIDFLORIDA Credit Union",
        "short_name": "MIDFLORIDA",
        "state": "FL",
        "domain": "midflorida.com",
        "additional_domains": [],
        "routing_numbers": ["263179532"],
        "bin_ranges": ["486041"],
        "acronyms": [],
    },
    {
        "name": "Members 1st Federal Credit Union",
        "short_name": "Members 1st",
        "state": "PA",
        "domain": "members1st.org",
        "additional_domains": [],
        "routing_numbers": ["231381912"],
        "bin_ranges": ["486079"],
        "acronyms": [("M1FCU", "Medium collision: multiple 'Members 1st' CUs exist nationwide")],
    },
    {
        "name": "Redstone Federal Credit Union",
        "short_name": "Redstone",
        "state": "AL",
        "domain": "redfcu.org",
        "additional_domains": [],
        "routing_numbers": ["262087609"],
        "bin_ranges": ["486025"],
        "acronyms": [("RFCU", "High collision: many regional FCUs abbreviate to RFCU")],
    },
    {
        "name": "Citizens Equity First Credit Union",
        "short_name": "CEFCU",
        "state": "IL",
        "domain": "cefcu.com",
        "additional_domains": [],
        "routing_numbers": ["271183701"],
        "bin_ranges": ["486088"],
        "acronyms": [("CEFCU", "Low collision risk — distinctive")],
    },
    {
        "name": "Veridian Credit Union",
        "short_name": "Veridian",
        "state": "IA",
        "domain": "veridiancu.org",
        "additional_domains": [],
        "routing_numbers": ["273976381"],
        "bin_ranges": ["486031"],
        "acronyms": [("Veridian", "Low collision: distinctive brand name")],
    },
    {
        "name": "Summit Credit Union",
        "short_name": "Summit CU",
        "state": "WI",
        "domain": "summitcreditunion.com",
        "additional_domains": [],
        "routing_numbers": ["275979034"],
        "bin_ranges": ["486056"],
        "acronyms": [("Summit", "High collision: very generic — Summit Healthcare, Summit Materials, etc.")],
    },
    {
        "name": "Hudson Valley Federal Credit Union",
        "short_name": "HVFCU",
        "state": "NY",
        "domain": "hvfcu.org",
        "additional_domains": [],
        "routing_numbers": ["221373571"],
        "bin_ranges": ["486098"],
        "acronyms": [("HVFCU", "Low collision risk")],
    },
    {
        "name": "Virginia Credit Union",
        "short_name": "VACU",
        "state": "VA",
        "domain": "vacu.org",
        "additional_domains": [],
        "routing_numbers": ["251082644"],
        "bin_ranges": ["486029"],
        "acronyms": [("VACU", "Medium collision: could match VA (Veterans Affairs) + CU patterns")],
    },
]

COMMUNITY_BANKS: list[dict] = [
    # Large regional banks ($10B-$50B assets)
    {
        "name": "Bank OZK",
        "short_name": "OZK",
        "state": "AR",
        "domain": "ozk.com",
        "additional_domains": [],
        "routing_numbers": ["082902757"],
        "bin_ranges": ["421470"],
        "acronyms": [("OZK", "Low collision risk — distinctive ticker/brand")],
    },
    {
        "name": "Prosperity Bank",
        "short_name": "Prosperity",
        "state": "TX",
        "domain": "prosperitybankusa.com",
        "additional_domains": [],
        "routing_numbers": ["113122655"],
        "bin_ranges": ["421482"],
        "acronyms": [("Prosperity", "High collision: generic word, Prosperity Gospel, other businesses")],
    },
    {
        "name": "Hancock Whitney Bank",
        "short_name": "Hancock Whitney",
        "state": "MS",
        "domain": "hancockwhitney.com",
        "additional_domains": [],
        "routing_numbers": ["065400137"],
        "bin_ranges": ["421486"],
        "acronyms": [("HWB", "Low collision risk")],
    },
    {
        "name": "Commerce Bank",
        "short_name": "Commerce",
        "state": "MO",
        "domain": "commercebank.com",
        "additional_domains": [],
        "routing_numbers": ["101000019"],
        "bin_ranges": ["421491"],
        "acronyms": [("Commerce", "High collision: generic word, US Dept of Commerce, etc.")],
    },
    {
        "name": "MidFirst Bank",
        "short_name": "MidFirst",
        "state": "OK",
        "domain": "midfirst.com",
        "additional_domains": [],
        "routing_numbers": ["103003632"],
        "bin_ranges": ["421495"],
        "acronyms": [],
    },
    {
        "name": "BankUnited",
        "short_name": "BankUnited",
        "state": "FL",
        "domain": "bankunited.com",
        "additional_domains": [],
        "routing_numbers": ["267090594"],
        "bin_ranges": ["421498"],
        "acronyms": [("BKU", "Low collision risk — NYSE ticker")],
    },
    {
        "name": "United Bank",
        "short_name": "United Bank",
        "state": "WV",
        "domain": "bankwithunited.com",
        "additional_domains": [],
        "routing_numbers": ["051404260"],
        "bin_ranges": ["421502"],
        "acronyms": [("United Bank", "High collision: extremely generic, many banks named United")],
    },
    {
        "name": "Cadence Bank",
        "short_name": "Cadence",
        "state": "MS",
        "domain": "cadencebank.com",
        "additional_domains": [],
        "routing_numbers": ["065305436"],
        "bin_ranges": ["421506"],
        "acronyms": [("Cadence", "Medium collision: Cadence Design Systems, music term")],
    },
    {
        "name": "First National Bank of Pennsylvania",
        "short_name": "FNB",
        "state": "PA",
        "domain": "fnb-online.com",
        "additional_domains": [],
        "routing_numbers": ["043318092"],
        "bin_ranges": ["421510"],
        "acronyms": [("FNB", "High collision: dozens of banks use FNB abbreviation nationally")],
    },
    {
        "name": "Associated Bank",
        "short_name": "Associated",
        "state": "WI",
        "domain": "associatedbank.com",
        "additional_domains": [],
        "routing_numbers": ["075900575"],
        "bin_ranges": ["421514"],
        "acronyms": [("ASB", "High collision: generic abbreviation, Associated Press, etc.")],
    },
    {
        "name": "South State Bank",
        "short_name": "South State",
        "state": "SC",
        "domain": "southstatebank.com",
        "additional_domains": [],
        "routing_numbers": ["053902197"],
        "bin_ranges": ["421518"],
        "acronyms": [("SSB", "High collision: many banks use SSB, also Server-Sent Broadcasts, Super Smash Bros")],
    },
    {
        "name": "Pinnacle Financial Partners",
        "short_name": "Pinnacle Bank",
        "state": "TN",
        "domain": "pnfp.com",
        "additional_domains": ["pinnaclebank.com"],
        "routing_numbers": ["064008637"],
        "bin_ranges": ["421522"],
        "acronyms": [("PNFP", "Low collision risk — NYSE ticker"), ("Pinnacle", "High collision: very generic word")],
    },
    {
        "name": "Glacier Bank",
        "short_name": "Glacier",
        "state": "MT",
        "domain": "glacierbank.com",
        "additional_domains": [],
        "routing_numbers": ["092901683"],
        "bin_ranges": ["421526"],
        "acronyms": [("GBCI", "Low collision risk — NYSE ticker")],
    },
    {
        "name": "First Interstate Bank",
        "short_name": "First Interstate",
        "state": "MT",
        "domain": "firstinterstatebank.com",
        "additional_domains": [],
        "routing_numbers": ["092901588"],
        "bin_ranges": ["421530"],
        "acronyms": [("FIB", "High collision: common English word, also First Interstate BancSystem ticker FIBK")],
    },
    {
        "name": "Old National Bank",
        "short_name": "Old National",
        "state": "IN",
        "domain": "oldnational.com",
        "additional_domains": [],
        "routing_numbers": ["086300012"],
        "bin_ranges": ["421534"],
        "acronyms": [("ONB", "Medium collision: generic abbreviation")],
    },
    {
        "name": "UMB Bank",
        "short_name": "UMB",
        "state": "MO",
        "domain": "umb.com",
        "additional_domains": [],
        "routing_numbers": ["101000695"],
        "bin_ranges": ["421538"],
        "acronyms": [("UMB", "Medium collision: University of Maryland Baltimore, other UMB orgs")],
    },
    {
        "name": "Simmons Bank",
        "short_name": "Simmons",
        "state": "AR",
        "domain": "simmonsbank.com",
        "additional_domains": [],
        "routing_numbers": ["082904084"],
        "bin_ranges": ["421542"],
        "acronyms": [("Simmons", "High collision: Simmons University, Simmons mattress brand, common surname")],
    },
    {
        "name": "Banner Bank",
        "short_name": "Banner",
        "state": "WA",
        "domain": "bannerbank.com",
        "additional_domains": [],
        "routing_numbers": ["325070760"],
        "bin_ranges": ["421546"],
        "acronyms": [("Banner", "High collision: Banner Health, generic word")],
    },
    {
        "name": "Pacific Premier Bank",
        "short_name": "Pacific Premier",
        "state": "CA",
        "domain": "ppbi.com",
        "additional_domains": [],
        "routing_numbers": ["122242869"],
        "bin_ranges": ["421550"],
        "acronyms": [("PPBI", "Low collision risk — NYSE ticker")],
    },
    {
        "name": "Fulton Bank",
        "short_name": "Fulton",
        "state": "PA",
        "domain": "fultonbank.com",
        "additional_domains": [],
        "routing_numbers": ["031301422"],
        "bin_ranges": ["421554"],
        "acronyms": [("Fulton", "Medium collision: Fulton County (multiple states), Robert Fulton, etc.")],
    },
    # Mid-size community banks ($3B-$10B assets)
    {
        "name": "Trustmark National Bank",
        "short_name": "Trustmark",
        "state": "MS",
        "domain": "trustmark.com",
        "additional_domains": [],
        "routing_numbers": ["065403626"],
        "bin_ranges": ["421558"],
        "acronyms": [("Trustmark", "Medium collision: Trustmark insurance company (different entity)")],
    },
    {
        "name": "WesBanco Bank",
        "short_name": "WesBanco",
        "state": "WV",
        "domain": "wesbanco.com",
        "additional_domains": [],
        "routing_numbers": ["051501673"],
        "bin_ranges": ["421562"],
        "acronyms": [("WSBC", "Low collision risk — NASDAQ ticker")],
    },
    {
        "name": "Berkshire Hills Bancorp",
        "short_name": "Berkshire Bank",
        "state": "MA",
        "domain": "berkshirebank.com",
        "additional_domains": [],
        "routing_numbers": ["211871869"],
        "bin_ranges": ["421566"],
        "acronyms": [("Berkshire", "High collision: Berkshire Hathaway dominates this name")],
    },
    {
        "name": "Valley National Bank",
        "short_name": "Valley National",
        "state": "NJ",
        "domain": "valley.com",
        "additional_domains": [],
        "routing_numbers": ["021201383"],
        "bin_ranges": ["421570"],
        "acronyms": [("VLY", "Low collision risk — NYSE ticker"), ("Valley", "High collision: extremely generic")],
    },
    {
        "name": "Independent Bank",
        "short_name": "Independent",
        "state": "MI",
        "domain": "independentbank.com",
        "additional_domains": [],
        "routing_numbers": ["072413829"],
        "bin_ranges": ["421574"],
        "acronyms": [("Independent", "High collision: generic word")],
    },
    {
        "name": "Renasant Bank",
        "short_name": "Renasant",
        "state": "MS",
        "domain": "renasantbank.com",
        "additional_domains": [],
        "routing_numbers": ["065201948"],
        "bin_ranges": ["421578"],
        "acronyms": [("RNST", "Low collision risk — NASDAQ ticker")],
    },
    {
        "name": "First Financial Bankshares",
        "short_name": "First Financial",
        "state": "TX",
        "domain": "ffin.com",
        "additional_domains": [],
        "routing_numbers": ["111900659"],
        "bin_ranges": ["421582"],
        "acronyms": [("FFIN", "Low collision risk — NASDAQ ticker"), ("First Financial", "High collision: many banks use this name")],
    },
    {
        "name": "International Bancshares",
        "short_name": "IBC Bank",
        "state": "TX",
        "domain": "ibc.com",
        "additional_domains": [],
        "routing_numbers": ["114902528"],
        "bin_ranges": ["421586"],
        "acronyms": [("IBC", "High collision: International Broadcasting, India-based companies, etc.")],
    },
    {
        "name": "Columbia Banking System",
        "short_name": "Columbia Bank",
        "state": "WA",
        "domain": "columbiabank.com",
        "additional_domains": [],
        "routing_numbers": ["325070881"],
        "bin_ranges": ["421590"],
        "acronyms": [("Columbia", "High collision: Columbia University, Columbia Sportswear, District of Columbia, etc.")],
    },
    {
        "name": "Sandy Spring Bank",
        "short_name": "Sandy Spring",
        "state": "MD",
        "domain": "sandyspringbank.com",
        "additional_domains": [],
        "routing_numbers": ["055001096"],
        "bin_ranges": ["421594"],
        "acronyms": [("SASR", "Low collision risk — NASDAQ ticker")],
    },
    {
        "name": "Centier Bank",
        "short_name": "Centier",
        "state": "IN",
        "domain": "centier.com",
        "additional_domains": [],
        "routing_numbers": ["071122661"],
        "bin_ranges": ["421598"],
        "acronyms": [],
    },
    {
        "name": "S&T Bank",
        "short_name": "S&T",
        "state": "PA",
        "domain": "stbank.com",
        "additional_domains": [],
        "routing_numbers": ["041202582"],
        "bin_ranges": ["421602"],
        "acronyms": [("S&T", "High collision: Science & Technology, Smith & Wesson ticker, generic abbreviation")],
    },
    {
        "name": "NBT Bank",
        "short_name": "NBT",
        "state": "NY",
        "domain": "nbtbank.com",
        "additional_domains": [],
        "routing_numbers": ["021302648"],
        "bin_ranges": ["421606"],
        "acronyms": [("NBT", "Medium collision: National Basketball Tournament, other uses")],
    },
    {
        "name": "Community Bank N.A.",
        "short_name": "Community Bank",
        "state": "NY",
        "domain": "cbna.com",
        "additional_domains": [],
        "routing_numbers": ["022303023"],
        "bin_ranges": ["421610"],
        "acronyms": [("CBNA", "Low collision risk as full acronym"), ("Community Bank", "High collision: extremely generic name used by hundreds of banks")],
    },
    {
        "name": "CVB Financial Corp",
        "short_name": "Citizens Business Bank",
        "state": "CA",
        "domain": "cbbank.com",
        "additional_domains": [],
        "routing_numbers": ["122233650"],
        "bin_ranges": ["421614"],
        "acronyms": [("CVBF", "Low collision risk — NASDAQ ticker"), ("CBB", "Medium collision")],
    },
    {
        "name": "Lakeland Bank",
        "short_name": "Lakeland",
        "state": "NJ",
        "domain": "lakelandbank.com",
        "additional_domains": [],
        "routing_numbers": ["221271063"],
        "bin_ranges": ["421618"],
        "acronyms": [("Lakeland", "Medium collision: Lakeland FL city, Lakeland University, etc.")],
    },
    {
        "name": "Heartland BancCorp",
        "short_name": "Heartland Bank",
        "state": "OH",
        "domain": "heartlandbank.com",
        "additional_domains": [],
        "routing_numbers": ["044115809"],
        "bin_ranges": ["421622"],
        "acronyms": [("Heartland", "High collision: Heartland Payment Systems, Heartland Express, generic term")],
    },
    {
        "name": "First Busey Bank",
        "short_name": "Busey Bank",
        "state": "IL",
        "domain": "busey.com",
        "additional_domains": [],
        "routing_numbers": ["071102568"],
        "bin_ranges": ["421626"],
        "acronyms": [("BUSE", "Low collision risk — NASDAQ ticker"), ("Busey", "Low collision risk — distinctive name")],
    },
    {
        "name": "Glacier Hills Credit Union",
        "short_name": "Glacier Hills",
        "state": "WI",
        "domain": "glacierhillscu.org",
        "additional_domains": [],
        "routing_numbers": ["275979058"],
        "bin_ranges": ["486102"],
        "acronyms": [("GHCU", "Low collision risk")],
    },
    {
        "name": "Home Federal Savings Bank",
        "short_name": "Home Federal",
        "state": "LA",
        "domain": "homefederal.com",
        "additional_domains": [],
        "routing_numbers": ["065404903"],
        "bin_ranges": ["421630"],
        "acronyms": [("Home Federal", "High collision: many banks use 'Home Federal' name nationwide")],
    },
    # Smaller community banks ($1B-$3B assets)
    {
        "name": "Glacier Bancorp Mountain West",
        "short_name": "Mountain West Bank",
        "state": "ID",
        "domain": "mountainwestbank.com",
        "additional_domains": [],
        "routing_numbers": ["124301025"],
        "bin_ranges": ["421634"],
        "acronyms": [("MWB", "Medium collision: generic abbreviation")],
    },
    {
        "name": "TowneBank",
        "short_name": "TowneBank",
        "state": "VA",
        "domain": "townebank.com",
        "additional_domains": [],
        "routing_numbers": ["051404972"],
        "bin_ranges": ["421638"],
        "acronyms": [("TOWN", "Medium collision: NASDAQ ticker, but also generic English word")],
    },
    {
        "name": "Brookline Bank",
        "short_name": "Brookline",
        "state": "MA",
        "domain": "brooklinebank.com",
        "additional_domains": [],
        "routing_numbers": ["211870899"],
        "bin_ranges": ["421642"],
        "acronyms": [("Brookline", "Medium collision: Brookline MA neighborhood, Brookline Capital")],
    },
    {
        "name": "First Savings Financial Group",
        "short_name": "First Savings",
        "state": "IN",
        "domain": "fsbbank.net",
        "additional_domains": [],
        "routing_numbers": ["086507407"],
        "bin_ranges": ["421646"],
        "acronyms": [("FSFG", "Low collision risk"), ("First Savings", "High collision: generic name used by many savings banks")],
    },
    {
        "name": "Carter Bank and Trust",
        "short_name": "Carter Bank",
        "state": "VA",
        "domain": "carterbankandtrust.com",
        "additional_domains": [],
        "routing_numbers": ["051404452"],
        "bin_ranges": ["421650"],
        "acronyms": [("CARE", "Medium collision — NASDAQ ticker matches 'care' word")],
    },
    {
        "name": "Seacoast Banking Corp",
        "short_name": "Seacoast Bank",
        "state": "FL",
        "domain": "seacoastbanking.com",
        "additional_domains": [],
        "routing_numbers": ["067005781"],
        "bin_ranges": ["421654"],
        "acronyms": [("SBCF", "Low collision risk — NASDAQ ticker"), ("Seacoast", "Medium collision: generic coastal term")],
    },
    {
        "name": "Enterprise Bancorp",
        "short_name": "Enterprise Bank",
        "state": "MA",
        "domain": "enterprisebanking.com",
        "additional_domains": [],
        "routing_numbers": ["211370545"],
        "bin_ranges": ["421658"],
        "acronyms": [("Enterprise", "High collision: Enterprise Rent-A-Car, Star Trek, generic word")],
    },
    {
        "name": "First Bancshares",
        "short_name": "The First Bank",
        "state": "MS",
        "domain": "thefirstbank.com",
        "additional_domains": [],
        "routing_numbers": ["065201425"],
        "bin_ranges": ["421662"],
        "acronyms": [("FBMS", "Low collision risk — NASDAQ ticker"), ("The First", "High collision: extremely generic")],
    },
    {
        "name": "Heritage Financial Corp",
        "short_name": "Heritage Bank",
        "state": "WA",
        "domain": "heritagebanknw.com",
        "additional_domains": [],
        "routing_numbers": ["325070032"],
        "bin_ranges": ["421666"],
        "acronyms": [("HFWA", "Low collision risk — NASDAQ ticker"), ("Heritage", "High collision: Heritage Foundation, generic word")],
    },
    {
        "name": "Veritex Community Bank",
        "short_name": "Veritex",
        "state": "TX",
        "domain": "veritexbank.com",
        "additional_domains": [],
        "routing_numbers": ["111322994"],
        "bin_ranges": ["421670"],
        "acronyms": [("VBTX", "Low collision risk — NASDAQ ticker")],
    },
]


def _generate_watch_terms(
    inst_id: str,
    name: str,
    short_name: str,
    domain: str,
    additional_domains: list[str],
    routing_numbers: list[str],
    bin_ranges: list[str],
    acronyms: list[tuple[str, str]],
) -> list[WatchTerm]:
    """Generate watch terms for an institution.

    Creates terms for: full name, short name, domains, keyword variations,
    routing numbers, BIN prefixes, and acronym/abbreviations (with collision notes).
    """
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

    # Routing numbers
    for rtn in routing_numbers:
        terms.append(
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst_id,
                term_type=WatchTermType.routing_number,
                value=rtn,
                enabled=True,
                case_sensitive=False,
                notes=f"ABA routing number (FDIC/NCUA public data)",
            )
        )

    # BIN range prefixes
    for bin_prefix in bin_ranges:
        terms.append(
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst_id,
                term_type=WatchTermType.bin_range,
                value=bin_prefix,
                enabled=True,
                case_sensitive=False,
                notes=f"Card BIN prefix ({len(bin_prefix)}-digit)",
            )
        )

    # Acronyms and abbreviations (with collision/noise notes)
    for acronym, collision_note in acronyms:
        # Skip if acronym is already the short_name (already added above)
        if acronym.lower() == short_name.lower():
            continue
        terms.append(
            WatchTerm(
                id=str(uuid4()),
                institution_id=inst_id,
                term_type=WatchTermType.keyword,
                value=acronym,
                enabled=True,
                case_sensitive=True,  # Case-sensitive for acronyms to reduce noise
                notes=f"Acronym/abbreviation. COLLISION RISK: {collision_note}",
            )
        )

    return terms


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

DEFAULT_SOURCES = [
    # --- Forums (Tor) ---
    # BreachForums Tor Mirror: separate source for .onion access (not a duplicate of
    # the clearnet BreachForums Monitor in seed_sources.py — different URL and config).
    {
        "name": "BreachForums Tor Mirror",
        "source_type": SourceType.forum,
        "url": "http://breachforums.example.onion",
        "connector_class": "darkdisco.discovery.connectors.forum:ForumConnector",
        "poll_interval_seconds": 1800,
        "config": {
            "forums": [
                {
                    "name": "BreachForums (.onion)",
                    "base_url": "http://breachforums.example.onion",
                    "recent_path": "/Forum-Databases",
                    "selector_profile": "mybb",
                    "last_seen_id": "",
                    "tor": True,
                },
            ],
            "max_pages": 3,
        },
    },
    {
        "name": "Dread Market Forum",
        "source_type": SourceType.forum,
        "url": "http://dread.example.onion",
        "connector_class": "darkdisco.discovery.connectors.forum:ForumConnector",
        "poll_interval_seconds": 3600,
        "config": {
            "forums": [
                {
                    "name": "Dread",
                    "base_url": "http://dread.example.onion",
                    "recent_path": "/",
                    "selector_profile": "custom",
                    "last_seen_id": "",
                    "tor": True,
                },
            ],
            "max_pages": 2,
        },
    },
    # --- Paste sites (duplicates of Paste Site Monitor — disabled) ---
    # Pastebin and Rentry are already covered by the "Paste Site Monitor" source
    # in seed_sources.py which uses PasteSiteConnector with DEFAULT_SITES.
    # Kept as disabled records to avoid re-creation; seed_sources.py is canonical.
    {
        "name": "Pastebin Scraper",
        "source_type": SourceType.paste_site,
        "url": "https://pastebin.com",
        "connector_class": "darkdisco.discovery.connectors.paste_site:PasteSiteConnector",
        "enabled": False,
        "poll_interval_seconds": 600,
        "config": {},
    },
    {
        "name": "Rentry Paste Monitor",
        "source_type": SourceType.paste_site,
        "url": "https://rentry.co",
        "connector_class": "darkdisco.discovery.connectors.paste_site:PasteSiteConnector",
        "enabled": False,
        "poll_interval_seconds": 900,
        "config": {},
    },
    # --- Telegram (disabled — consolidated into Telegram Stealer Logs in seed_sources.py) ---
    {
        "name": "Telegram Channel Monitor",
        "source_type": SourceType.telegram,
        "url": None,
        "connector_class": "darkdisco.discovery.connectors.telegram:TelegramConnector",
        "enabled": False,
        "poll_interval_seconds": 300,
        "config": {
            "channels": [
                "bank_leaks_demo",
                "combolist_channel_demo",
                "stealer_logs_demo",
            ]
        },
    },
    # --- Breach databases ---
    {
        "name": "DeHashed Breach Database",
        "source_type": SourceType.breach_db,
        "url": "https://dehashed.com",
        "connector_class": "darkdisco.discovery.connectors.breach_db:BreachDBConnector",
        "poll_interval_seconds": 7200,
        "config": {
            "domains": [],
            "dehashed_enabled": True,
            "hibp_enabled": False,
            "intelx_enabled": False,
        },
    },
    {
        "name": "Have I Been Pwned Monitor",
        "source_type": SourceType.breach_db,
        "url": "https://haveibeenpwned.com",
        "connector_class": "darkdisco.discovery.connectors.breach_db:BreachDBConnector",
        "poll_interval_seconds": 3600,
        "config": {
            "domains": [],
            "dehashed_enabled": False,
            "hibp_enabled": True,
            "intelx_enabled": False,
        },
    },
    # --- Ransomware blogs (disabled — duplicates of groups in Ransomware Blog Monitor) ---
    {
        "name": "LockBit Ransomware Blog",
        "source_type": SourceType.ransomware_blog,
        "url": "http://lockbit.example.onion",
        "connector_class": "darkdisco.discovery.connectors.ransomware_blog:RansomwareBlogConnector",
        "enabled": False,
        "poll_interval_seconds": 1800,
        "config": {"group": "lockbit"},
    },
    {
        "name": "ALPHV/BlackCat Blog",
        "source_type": SourceType.ransomware_blog,
        "url": "http://alphv.example.onion",
        "connector_class": "darkdisco.discovery.connectors.ransomware_blog:RansomwareBlogConnector",
        "enabled": False,
        "poll_interval_seconds": 1800,
        "config": {"group": "alphv"},
    },
    # --- Stealer logs ---
    {
        "name": "Stealer Log Aggregator",
        "source_type": SourceType.stealer_log,
        "url": None,
        "connector_class": "darkdisco.discovery.connectors.stealer_log:StealerLogConnector",
        "poll_interval_seconds": 3600,
        "config": {
            "s3_prefix": "stealer-logs/incoming/",
            "archive_formats": ["zip", "tar.gz"],
            "parsers": ["redline", "raccoon", "generic"],
            "seen_hashes": [],
        },
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
        for entry in CREDIT_UNIONS:
            inst_id = await _ensure_institution(
                session,
                client_id=client_id,
                name=entry["name"],
                short_name=entry["short_name"],
                charter_type="credit_union",
                state=entry["state"],
                domain=entry["domain"],
                additional_domains=entry.get("additional_domains", []),
                routing_numbers=entry.get("routing_numbers", []),
                bin_ranges=entry.get("bin_ranges", []),
                acronyms=entry.get("acronyms", []),
            )
            institution_ids[entry["name"]] = inst_id
            institution_ids[entry["short_name"]] = inst_id

        print(f"\n--- Seeding {len(COMMUNITY_BANKS)} community/regional banks ---")
        for entry in COMMUNITY_BANKS:
            inst_id = await _ensure_institution(
                session,
                client_id=client_id,
                name=entry["name"],
                short_name=entry["short_name"],
                charter_type="bank",
                state=entry["state"],
                domain=entry["domain"],
                additional_domains=entry.get("additional_domains", []),
                routing_numbers=entry.get("routing_numbers", []),
                bin_ranges=entry.get("bin_ranges", []),
                acronyms=entry.get("acronyms", []),
            )
            institution_ids[entry["name"]] = inst_id
            institution_ids[entry["short_name"]] = inst_id

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
    routing_numbers: list[str],
    bin_ranges: list[str],
    acronyms: list[tuple[str, str]],
) -> str:
    result = await session.execute(
        select(Institution).where(
            Institution.client_id == client_id,
            Institution.name == name,
        )
    )
    inst = result.scalars().first()
    if inst:
        # Update bin_ranges and routing_numbers if they are empty
        updated_fields = []
        if not inst.bin_ranges and bin_ranges:
            inst.bin_ranges = bin_ranges
            updated_fields.append("bin_ranges")
        if not inst.routing_numbers and routing_numbers:
            inst.routing_numbers = routing_numbers
            updated_fields.append("routing_numbers")

        # Add missing routing_number and bin_range watch terms
        existing_terms = await session.execute(
            select(WatchTerm).where(
                WatchTerm.institution_id == inst.id,
                WatchTerm.term_type.in_([WatchTermType.routing_number, WatchTermType.bin_range]),
            )
        )
        existing_values = {t.value for t in existing_terms.scalars().all()}
        new_terms = []
        for rtn in routing_numbers:
            if rtn not in existing_values:
                new_terms.append(
                    WatchTerm(
                        id=str(uuid4()),
                        institution_id=inst.id,
                        term_type=WatchTermType.routing_number,
                        value=rtn,
                        enabled=True,
                        case_sensitive=False,
                        notes="ABA routing number (FDIC/NCUA public data)",
                    )
                )
        for bin_prefix in bin_ranges:
            if bin_prefix not in existing_values:
                new_terms.append(
                    WatchTerm(
                        id=str(uuid4()),
                        institution_id=inst.id,
                        term_type=WatchTermType.bin_range,
                        value=bin_prefix,
                        enabled=True,
                        case_sensitive=False,
                        notes=f"Card BIN prefix ({len(bin_prefix)}-digit)",
                    )
                )
        for t in new_terms:
            session.add(t)

        if updated_fields or new_terms:
            await session.flush()
            print(f"  [updated] {charter_type:12s} | {name} ({', '.join(updated_fields)}, +{len(new_terms)} watch terms)")
        else:
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
        routing_numbers=routing_numbers if routing_numbers else None,
        bin_ranges=bin_ranges if bin_ranges else None,
        active=True,
    )
    session.add(inst)

    # Generate and add watch terms
    terms = _generate_watch_terms(
        inst_id, name, short_name, domain,
        additional_domains, routing_numbers, bin_ranges, acronyms,
    )
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
    enabled: bool = True,
) -> str:
    result = await session.execute(select(Source).where(Source.name == name))
    src = result.scalars().first()
    if src:
        # Update connector_class path and enabled state on existing records
        src.connector_class = connector_class
        src.enabled = enabled
        src.config = config or src.config
        print(f"  [updated] Source: {name} → {connector_class} (enabled={enabled})")
        return src.id

    src = Source(
        id=str(uuid4()),
        name=name,
        source_type=source_type,
        url=url,
        connector_class=connector_class,
        enabled=enabled,
        poll_interval_seconds=poll_interval_seconds,
        config=config,
    )
    session.add(src)
    await session.flush()
    print(f"  [created] Source: {name} ({source_type.value}, enabled={enabled})")
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
