"""Stealer log connector — parses Redline, Raccoon, and generic infostealer dumps.

Processes stealer log archives (zip/tar) from configured S3 paths or local
directories. Extracts credentials (URL/login/password triples), cookies,
autofill data, and system fingerprints. Matches against institution domains
and BIN ranges. Deduplicates via seen_hashes in Source.config.

Source config schema (stored in Source.config JSONB):
{
    "s3_prefix": "stealer-logs/incoming/",
    "archive_formats": ["zip", "tar.gz"],
    "parsers": ["redline", "raccoon", "generic"],
    "seen_hashes": ["<archive_sha256_prefix>", ...],
    "max_archive_size": 104857600,
    "max_credentials_per_archive": 50000,
    "request_delay_seconds": 1
}
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import re
import tarfile
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from darkdisco.config import settings
from darkdisco.discovery.connectors.base import BaseConnector, RawMention

logger = logging.getLogger(__name__)

# Max archive size default: 100 MB
_DEFAULT_MAX_ARCHIVE_SIZE = 100 * 1024 * 1024

# Max credentials to extract per archive to prevent memory exhaustion
_DEFAULT_MAX_CREDENTIALS = 50_000

# Max individual file size within an archive (5 MB)
_MAX_MEMBER_SIZE = 5 * 1024 * 1024


@dataclass
class Credential:
    """A single credential entry extracted from a stealer log."""

    url: str = ""
    username: str = ""
    password: str = ""
    application: str = ""  # e.g., "Chrome", "Firefox", "Outlook"
    metadata: dict = field(default_factory=dict)


@dataclass
class StealerLogArchive:
    """Parsed contents of a single stealer log archive."""

    archive_key: str  # S3 key or file path
    sha256: str
    stealer_family: str  # redline, raccoon, generic
    credentials: list[Credential] = field(default_factory=list)
    cookies_count: int = 0
    autofill_count: int = 0
    system_info: dict = field(default_factory=dict)
    raw_file_list: list[str] = field(default_factory=list)

    @property
    def content_hash(self) -> str:
        """Deterministic hash for deduplication."""
        return self.sha256[:16]


# ---------------------------------------------------------------------------
# Stealer log parsers — one per known family
# ---------------------------------------------------------------------------
# Each parser receives a dict of {filename: bytes_content} from the archive
# and returns a StealerLogArchive.
# ---------------------------------------------------------------------------


def _detect_family(file_names: list[str]) -> str:
    """Detect stealer family from archive file structure."""
    names_lower = {n.lower() for n in file_names}
    paths_str = " ".join(names_lower)

    # Redline: typically has Passwords.txt, Cookies/, AutoFill/, SystemInfo.txt
    if any("passwords.txt" in n for n in names_lower):
        if any("systeminfo.txt" in n or "system info.txt" in n for n in names_lower):
            return "redline"

    # Raccoon: typically has passwords.txt plus a specific folder structure
    # with cookies.txt, autofill.txt in flat layout
    if any("passwords.txt" in n for n in names_lower):
        if any("cookies.txt" in n for n in names_lower):
            return "raccoon"

    # Generic: anything with credential-looking files
    if any(
        kw in paths_str
        for kw in ("password", "credential", "login", "autofill", "cookie")
    ):
        return "generic"

    return "generic"


def _parse_redline(
    files: dict[str, bytes], archive_key: str, sha256: str
) -> StealerLogArchive:
    """Parse Redline stealer log format.

    Redline typically structures archives as:
    - Passwords.txt (URL / Login / Password triples, separated by blank lines)
    - Cookies/ (Netscape cookie files per browser)
    - AutoFill/ (form data)
    - SystemInfo.txt (OS, hardware, IP info)
    """
    archive = StealerLogArchive(
        archive_key=archive_key,
        sha256=sha256,
        stealer_family="redline",
        raw_file_list=list(files.keys()),
    )

    for name, content in files.items():
        name_lower = name.lower()
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception:
            continue

        if name_lower.endswith("passwords.txt"):
            archive.credentials = _parse_password_triples(text)

        elif "systeminfo" in name_lower and name_lower.endswith(".txt"):
            archive.system_info = _parse_system_info(text)

        elif "cookie" in name_lower:
            # Count cookie entries (lines that aren't comments/empty)
            archive.cookies_count += sum(
                1
                for line in text.splitlines()
                if line.strip() and not line.startswith("#")
            )

        elif "autofill" in name_lower:
            archive.autofill_count += sum(
                1 for line in text.splitlines() if line.strip()
            )

    return archive


def _parse_raccoon(
    files: dict[str, bytes], archive_key: str, sha256: str
) -> StealerLogArchive:
    """Parse Raccoon stealer log format.

    Raccoon v2 typically has a flatter structure:
    - passwords.txt (similar URL/Login/Password format or CSV)
    - cookies.txt
    - autofill.txt
    - sysinfo.txt
    """
    archive = StealerLogArchive(
        archive_key=archive_key,
        sha256=sha256,
        stealer_family="raccoon",
        raw_file_list=list(files.keys()),
    )

    for name, content in files.items():
        name_lower = name.lower()
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception:
            continue

        if "password" in name_lower and name_lower.endswith(".txt"):
            # Try CSV format first (Raccoon v2 sometimes uses it)
            creds = _parse_csv_credentials(text)
            if not creds:
                creds = _parse_password_triples(text)
            archive.credentials = creds

        elif "sysinfo" in name_lower or "system" in name_lower:
            archive.system_info = _parse_system_info(text)

        elif "cookie" in name_lower:
            archive.cookies_count += sum(
                1
                for line in text.splitlines()
                if line.strip() and not line.startswith("#")
            )

        elif "autofill" in name_lower:
            archive.autofill_count += sum(
                1 for line in text.splitlines() if line.strip()
            )

    return archive


def _parse_generic_stealer(
    files: dict[str, bytes], archive_key: str, sha256: str
) -> StealerLogArchive:
    """Generic parser for unknown stealer families.

    Scans all text files for URL/login/password patterns.
    """
    archive = StealerLogArchive(
        archive_key=archive_key,
        sha256=sha256,
        stealer_family="generic",
        raw_file_list=list(files.keys()),
    )

    all_credentials: list[Credential] = []

    for name, content in files.items():
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception:
            continue

        name_lower = name.lower()

        if any(
            kw in name_lower for kw in ("password", "credential", "login")
        ):
            creds = _parse_password_triples(text)
            if not creds:
                creds = _parse_csv_credentials(text)
            all_credentials.extend(creds)

        elif "cookie" in name_lower:
            archive.cookies_count += sum(
                1
                for line in text.splitlines()
                if line.strip() and not line.startswith("#")
            )

        elif "autofill" in name_lower:
            archive.autofill_count += sum(
                1 for line in text.splitlines() if line.strip()
            )

        elif any(kw in name_lower for kw in ("sysinfo", "system", "info.txt")):
            archive.system_info = _parse_system_info(text)

    archive.credentials = all_credentials
    return archive


# ---------------------------------------------------------------------------
# Shared text parsers
# ---------------------------------------------------------------------------


def _parse_password_triples(text: str) -> list[Credential]:
    """Parse URL/Login/Password triple blocks separated by blank lines.

    Common format across Redline, Raccoon, and many other stealers:

        URL: https://example.com/login
        Login: user@example.com
        Password: p4$$w0rd
        Application: Chrome

        URL: https://bank.com
        Login: john
        Password: secret123
    """
    credentials: list[Credential] = []
    # Split into blocks by blank lines
    blocks = re.split(r"\n\s*\n", text)

    for block in blocks:
        lines = block.strip().splitlines()
        if len(lines) < 2:
            continue

        cred = Credential()
        for line in lines:
            line = line.strip()
            # Handle "Key: Value" format
            if ":" in line:
                key, _, value = line.partition(":")
                key = key.strip().lower()
                value = value.strip()

                if key in ("url", "host", "hostname", "site"):
                    cred.url = value
                elif key in ("login", "username", "user", "email"):
                    cred.username = value
                elif key in ("password", "pass", "pwd"):
                    cred.password = value
                elif key in ("application", "app", "browser", "software"):
                    cred.application = value

        # Only include if we have at least URL+username or URL+password
        if cred.url and (cred.username or cred.password):
            credentials.append(cred)

        if len(credentials) >= _DEFAULT_MAX_CREDENTIALS:
            break

    return credentials


def _parse_csv_credentials(text: str) -> list[Credential]:
    """Parse CSV-formatted credential dumps.

    Some stealers export as CSV with headers like:
    url,username,password
    or
    URL,Login,Password,Application
    """
    credentials: list[Credential] = []

    try:
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return []

        # Normalize field names
        fields_lower = {f.lower().strip(): f for f in reader.fieldnames}

        url_field = None
        user_field = None
        pass_field = None
        app_field = None

        for key, original in fields_lower.items():
            if key in ("url", "host", "hostname", "site", "origin_url"):
                url_field = original
            elif key in ("login", "username", "user", "email", "username_value"):
                user_field = original
            elif key in ("password", "pass", "pwd", "password_value"):
                pass_field = original
            elif key in ("application", "app", "browser"):
                app_field = original

        if not url_field and not user_field:
            return []

        for row in reader:
            cred = Credential(
                url=row.get(url_field, "") if url_field else "",
                username=row.get(user_field, "") if user_field else "",
                password=row.get(pass_field, "") if pass_field else "",
                application=row.get(app_field, "") if app_field else "",
            )
            if cred.url and (cred.username or cred.password):
                credentials.append(cred)

            if len(credentials) >= _DEFAULT_MAX_CREDENTIALS:
                break

    except (csv.Error, UnicodeDecodeError):
        return []

    return credentials


def _parse_system_info(text: str) -> dict:
    """Parse system information from stealer logs.

    Extracts OS, IP, hardware, location info from key-value text.
    """
    info: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if ":" in line:
            key, _, value = line.partition(":")
            key = key.strip().lower()
            value = value.strip()
            if not value:
                continue

            if any(kw in key for kw in ("os", "windows", "system", "platform")):
                info["os"] = value
            elif any(kw in key for kw in ("ip", "address")):
                info.setdefault("ip", value)
            elif any(kw in key for kw in ("country", "location", "geo")):
                info["country"] = value
            elif any(kw in key for kw in ("hwid", "hardware", "machine")):
                info["hwid"] = value
            elif any(kw in key for kw in ("user", "username", "account")):
                info["local_user"] = value

    return info


# Map parser name → function
_PARSERS = {
    "redline": _parse_redline,
    "raccoon": _parse_raccoon,
    "generic": _parse_generic_stealer,
}


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class StealerLogConnector(BaseConnector):
    """Processes stealer log archives from S3 storage.

    Reads compressed archives from an S3 prefix, parses credential data
    using format-specific parsers (Redline, Raccoon, generic), and emits
    RawMention objects for the matching pipeline.

    Source config schema (stored in Source.config JSONB):
    {
        "s3_prefix": "stealer-logs/incoming/",
        "archive_formats": ["zip", "tar.gz"],
        "parsers": ["redline", "raccoon", "generic"],
        "seen_hashes": ["<sha256_prefix>", ...],
        "max_archive_size": 104857600,
        "max_credentials_per_archive": 50000,
        "request_delay_seconds": 1
    }
    """

    name = "stealer_log"
    source_type = "stealer_log"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self._s3_client = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Initialize S3 client."""
        self._s3_client = boto3.client(
            "s3",
            endpoint_url=self.config.get("s3_endpoint", settings.s3_endpoint),
            aws_access_key_id=self.config.get("s3_access_key", settings.s3_access_key),
            aws_secret_access_key=self.config.get(
                "s3_secret_key", settings.s3_secret_key
            ),
        )

    async def teardown(self) -> None:
        """Cleanup S3 client."""
        self._s3_client = None

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        """List new archives from S3 prefix, parse each, return credentials as mentions."""
        if not self._s3_client:
            await self.setup()

        s3_prefix = self.config.get("s3_prefix", "stealer-logs/incoming/")
        bucket = self.config.get("s3_bucket", settings.s3_bucket)
        max_archive_size = self.config.get(
            "max_archive_size", _DEFAULT_MAX_ARCHIVE_SIZE
        )
        seen_hashes: set[str] = set(self.config.get("seen_hashes", []))
        mentions: list[RawMention] = []

        # List objects under the configured prefix
        archive_keys = self._list_archives(bucket, s3_prefix, since)

        for key, last_modified in archive_keys:
            archive_data = self._download_archive(bucket, key, max_archive_size)
            if archive_data is None:
                continue

            # Compute SHA-256 for dedup
            sha256 = hashlib.sha256(archive_data).hexdigest()
            content_hash = sha256[:16]

            if content_hash in seen_hashes:
                logger.debug("Skipping already-seen archive %s (hash=%s)", key, content_hash)
                continue

            # Extract and parse
            parsed = self._parse_archive(archive_data, key, sha256)
            if parsed is None:
                logger.warning("Failed to parse archive %s", key)
                continue

            seen_hashes.add(content_hash)

            # Convert to mentions
            archive_mentions = self._archive_to_mentions(parsed, last_modified)
            mentions.extend(archive_mentions)

        # Persist seen hashes (cap at 10k)
        self.config["seen_hashes"] = list(seen_hashes)[-10000:]

        logger.info(
            "StealerLogConnector polled prefix '%s': %d archives → %d mentions",
            s3_prefix,
            len(archive_keys),
            len(mentions),
        )
        return mentions

    # ------------------------------------------------------------------
    # S3 operations
    # ------------------------------------------------------------------

    def _list_archives(
        self, bucket: str, prefix: str, since: datetime | None
    ) -> list[tuple[str, datetime]]:
        """List archive objects under S3 prefix, optionally filtered by date."""
        archives: list[tuple[str, datetime]] = []
        valid_extensions = tuple(
            self.config.get("archive_formats", ["zip", "tar.gz", "tgz"])
        )

        try:
            paginator = self._s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    last_modified = obj["LastModified"]

                    # Filter by modification date
                    if since and last_modified.replace(tzinfo=timezone.utc) < since:
                        continue

                    # Filter by extension
                    if any(key.lower().endswith(f".{ext}") for ext in valid_extensions):
                        archives.append((key, last_modified))

        except ClientError as exc:
            logger.error("S3 list failed for %s/%s: %s", bucket, prefix, exc)

        return archives

    def _download_archive(
        self, bucket: str, key: str, max_size: int
    ) -> bytes | None:
        """Download an archive from S3, respecting size limits."""
        try:
            # Check size first via HEAD
            head = self._s3_client.head_object(Bucket=bucket, Key=key)
            size = head.get("ContentLength", 0)
            if size > max_size:
                logger.warning(
                    "Archive %s too large (%d > %d), skipping", key, size, max_size
                )
                return None

            response = self._s3_client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()

        except ClientError as exc:
            logger.error("S3 download failed for %s/%s: %s", bucket, key, exc)
            return None

    # ------------------------------------------------------------------
    # Archive extraction and parsing
    # ------------------------------------------------------------------

    def _parse_archive(
        self, data: bytes, archive_key: str, sha256: str
    ) -> StealerLogArchive | None:
        """Extract archive contents and parse with appropriate stealer parser."""
        files = self._extract_archive(data, archive_key)
        if not files:
            return None

        # Detect family from file structure
        family = _detect_family(list(files.keys()))

        # Use configured parsers if specified, otherwise auto-detect
        allowed_parsers = self.config.get("parsers", list(_PARSERS.keys()))
        if family not in allowed_parsers:
            family = "generic"

        parser_fn = _PARSERS.get(family, _parse_generic_stealer)
        try:
            return parser_fn(files, archive_key, sha256)
        except Exception:
            logger.exception("Parser '%s' failed for archive %s", family, archive_key)
            return None

    @staticmethod
    def _extract_archive(data: bytes, archive_key: str) -> dict[str, bytes]:
        """Extract text files from a zip or tar archive.

        Returns {relative_path: file_bytes} for text-like files only.
        Skips binary files, oversized files, and symlinks.
        """
        files: dict[str, bytes] = {}
        key_lower = archive_key.lower()

        # Dangerous path patterns to reject (path traversal)
        def _is_safe_path(path: str) -> bool:
            return ".." not in path and not path.startswith("/")

        if key_lower.endswith(".zip"):
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        if not _is_safe_path(info.filename):
                            continue
                        if info.file_size > _MAX_MEMBER_SIZE:
                            continue
                        # Only extract text-like files
                        if _is_text_file(info.filename):
                            files[info.filename] = zf.read(info.filename)
            except (zipfile.BadZipFile, Exception) as exc:
                logger.warning("Failed to extract zip %s: %s", archive_key, exc)

        elif key_lower.endswith((".tar.gz", ".tgz", ".tar")):
            try:
                mode = "r:gz" if key_lower.endswith((".tar.gz", ".tgz")) else "r:"
                with tarfile.open(fileobj=io.BytesIO(data), mode=mode) as tf:
                    for member in tf.getmembers():
                        if not member.isfile():
                            continue
                        if not _is_safe_path(member.name):
                            continue
                        if member.size > _MAX_MEMBER_SIZE:
                            continue
                        if _is_text_file(member.name):
                            f = tf.extractfile(member)
                            if f:
                                files[member.name] = f.read()
            except (tarfile.TarError, Exception) as exc:
                logger.warning("Failed to extract tar %s: %s", archive_key, exc)

        return files

    # ------------------------------------------------------------------
    # Mention generation
    # ------------------------------------------------------------------

    def _archive_to_mentions(
        self, archive: StealerLogArchive, discovered_at: datetime
    ) -> list[RawMention]:
        """Convert a parsed stealer log archive into RawMention objects.

        Generates one mention per archive containing a summary of all
        extracted credentials, with full credential URLs in the content
        for domain/BIN matching by the pipeline matcher.
        """
        if not archive.credentials and not archive.cookies_count:
            return []

        # Extract unique domains from credential URLs for content matching
        domains: set[str] = set()
        for cred in archive.credentials:
            domain = _extract_domain(cred.url)
            if domain:
                domains.add(domain)

        # Build summary content that includes all credential URLs and usernames
        # so the matcher can find domain and BIN matches
        parts: list[str] = []
        parts.append(
            f"Stealer log ({archive.stealer_family}): "
            f"{len(archive.credentials)} credentials extracted"
        )

        if archive.cookies_count:
            parts.append(f"Cookies: {archive.cookies_count}")
        if archive.autofill_count:
            parts.append(f"Autofill entries: {archive.autofill_count}")
        if archive.system_info:
            si = archive.system_info
            info_parts = []
            if si.get("os"):
                info_parts.append(f"OS: {si['os']}")
            if si.get("ip"):
                info_parts.append(f"IP: {si['ip']}")
            if si.get("country"):
                info_parts.append(f"Country: {si['country']}")
            if info_parts:
                parts.append("System: " + ", ".join(info_parts))

        parts.append(f"Unique domains: {len(domains)}")
        parts.append("")

        # Include all credential URLs and usernames for matching
        # (the matcher scans content for domains, BINs, etc.)
        for cred in archive.credentials[:5000]:
            line_parts = []
            if cred.url:
                line_parts.append(cred.url)
            if cred.username:
                line_parts.append(cred.username)
            if cred.application:
                line_parts.append(f"[{cred.application}]")
            if line_parts:
                parts.append(" | ".join(line_parts))

        content = "\n".join(parts)

        # Build metadata
        meta = {
            "stealer_family": archive.stealer_family,
            "archive_key": archive.archive_key,
            "archive_sha256": archive.sha256,
            "content_hash": archive.content_hash,
            "total_credentials": len(archive.credentials),
            "cookies_count": archive.cookies_count,
            "autofill_count": archive.autofill_count,
            "unique_domains": sorted(domains)[:100],
            "file_count": len(archive.raw_file_list),
        }
        if archive.system_info:
            meta["system_info"] = archive.system_info

        title = (
            f"[Stealer:{archive.stealer_family}] "
            f"{len(archive.credentials)} credentials "
            f"from {len(domains)} domains"
        )

        return [
            RawMention(
                source_name=f"stealer_log:{archive.stealer_family}",
                source_url=None,  # S3 archives don't have public URLs
                title=title,
                content=content,
                author=None,
                discovered_at=discovered_at.replace(tzinfo=timezone.utc)
                if discovered_at.tzinfo is None
                else discovered_at,
                metadata=meta,
            )
        ]

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def health_check(self) -> dict:
        """Verify S3 connectivity and prefix accessibility."""
        if not self._s3_client:
            await self.setup()

        bucket = self.config.get("s3_bucket", settings.s3_bucket)
        prefix = self.config.get("s3_prefix", "stealer-logs/incoming/")

        try:
            # Try listing the prefix (even if empty, we should get a valid response)
            self._s3_client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, MaxKeys=1
            )
            return {
                "healthy": True,
                "message": f"S3 connected — bucket '{bucket}', prefix '{prefix}' accessible",
            }
        except ClientError as exc:
            return {
                "healthy": False,
                "message": f"S3 connection failed: {exc}",
            }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DOMAIN_RE = re.compile(r"https?://([^/:]+)")


def _extract_domain(url: str) -> str:
    """Extract domain from a URL."""
    m = _DOMAIN_RE.match(url)
    return m.group(1).lower() if m else ""


def _is_text_file(filename: str) -> bool:
    """Check if a filename looks like a text file (not binary)."""
    text_extensions = {
        ".txt", ".csv", ".log", ".json", ".xml", ".html", ".htm",
        ".ini", ".cfg", ".conf", ".dat",
    }
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in text_extensions)
