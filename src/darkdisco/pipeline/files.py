"""File download, extraction, and analysis for dark web attachments.

Handles:
- Downloading files from Telegram messages (via Telethon)
- Extracting ZIP/RAR archives, including password-protected ones
- Analyzing extracted contents for watch term matches
- Storing originals and extracts in S3/MinIO as FindingAttachments

Passwords are extracted from the message text accompanying the file.
Common patterns: "pass: xxx", "password: xxx", "pwd: xxx", "пароль: xxx"
"""

from __future__ import annotations

import hashlib
import io
import logging
import os
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from darkdisco.config import settings

logger = logging.getLogger(__name__)

# Max file size we'll download (50MB)
MAX_DOWNLOAD_SIZE = 50 * 1024 * 1024
# Max total extracted size (100MB)
MAX_EXTRACT_TOTAL = 100 * 1024 * 1024
# Max files to extract from a single archive
MAX_EXTRACT_FILES = 500
# Max individual extracted file size
MAX_EXTRACT_FILE_SIZE = 50 * 1024 * 1024

# Archive extensions we attempt to extract
ARCHIVE_EXTENSIONS = {".zip", ".rar", ".7z", ".tar", ".tar.gz", ".tgz", ".tar.bz2"}

# Password patterns — matches "pass: xyz", "password xyz", "pwd:xyz", etc.
PASSWORD_PATTERNS = [
    re.compile(r"(?:pass(?:word)?|pwd|пароль|парол)\s*[:\-=]\s*(\S+)", re.IGNORECASE),
    re.compile(r"(?:pass(?:word)?|pwd)\s+is\s+(\S+)", re.IGNORECASE),
]

# File types of interest for text extraction
TEXT_EXTENSIONS = {
    ".txt", ".csv", ".log", ".json", ".xml", ".html", ".htm",
    ".sql", ".cfg", ".conf", ".ini", ".env", ".yml", ".yaml",
}

CREDENTIAL_EXTENSIONS = {
    ".txt", ".csv", ".log", ".sql",
}


def _strip_markdown(s: str) -> str:
    """Strip common Telegram markdown formatting from a string."""
    # Remove bold/italic markers, backticks, and emoji modifiers
    s = s.strip()
    s = s.strip("*`_~")
    s = s.strip()
    return s


def extract_passwords(text: str) -> list[str]:
    """Extract potential archive passwords from message text.

    Returns deduplicated list of candidate passwords, most likely first.
    """
    if not text:
        return []

    passwords = []
    for pattern in PASSWORD_PATTERNS:
        for match in pattern.finditer(text):
            pw = _strip_markdown(match.group(1).strip().rstrip(".,;!)"))
            if pw and len(pw) < 100:
                passwords.append(pw)

    # Deduplicate while preserving order
    seen = set()
    result = []
    for pw in passwords:
        if pw not in seen:
            seen.add(pw)
            result.append(pw)

    return result


def is_archive(filename: str) -> bool:
    """Check if a filename looks like an extractable archive."""
    if not filename:
        return False
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in ARCHIVE_EXTENSIONS)


def extract_archive(
    data: bytes,
    filename: str,
    passwords: list[str] | None = None,
) -> list[ExtractedFile]:
    """Extract files from an archive (ZIP or RAR).

    Tries without password first, then each candidate password.
    Returns list of extracted files with content and metadata.
    """
    lower = filename.lower()

    if lower.endswith(".zip"):
        return _extract_zip(data, passwords or [])
    elif lower.endswith(".rar"):
        return _extract_rar(data, passwords or [])
    elif lower.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2")):
        return _extract_tar(data, filename)
    else:
        return []


def extract_archive_from_path(
    path: str,
    filename: str,
    passwords: list[str] | None = None,
) -> list[ExtractedFile]:
    """Extract files from an archive on disk without reading the entire file into memory.

    For ZIP files, zipfile.ZipFile can open a file path directly.
    For RAR files, rarfile already works with file paths.
    For tar files, tarfile can open file paths directly.
    """
    lower = filename.lower()

    if lower.endswith(".zip"):
        return _extract_zip_from_path(path, passwords or [])
    elif lower.endswith(".rar"):
        return _extract_rar_from_path(path, passwords or [])
    elif lower.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2")):
        return _extract_tar_from_path(path, filename)
    else:
        return []


class ExtractedFile:
    """A single file extracted from an archive."""

    __slots__ = ("filename", "content", "size", "sha256")

    def __init__(self, filename: str, content: bytes):
        self.filename = filename
        self.content = content
        self.size = len(content)
        self.sha256 = hashlib.sha256(content).hexdigest()

    @property
    def extension(self) -> str:
        return Path(self.filename).suffix.lower()

    @property
    def is_text(self) -> bool:
        return self.extension in TEXT_EXTENSIONS

    @property
    def is_credential_file(self) -> bool:
        return self.extension in CREDENTIAL_EXTENSIONS

    def text_content(self, encoding: str = "utf-8") -> str:
        """Decode content as text, with fallback encodings."""
        for enc in (encoding, "utf-8", "latin-1", "cp1251"):
            try:
                return self.content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return self.content.decode("utf-8", errors="replace")


def _extract_zip(data: bytes, passwords: list[str]) -> list[ExtractedFile]:
    """Extract a ZIP archive, trying passwords if encrypted."""
    results: list[ExtractedFile] = []
    total_size = 0

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            # Check if any file is encrypted
            has_encrypted = any(
                info.flag_bits & 0x1 for info in zf.infolist()
            )

            # Build password list: None first (no password), then candidates
            pwd_list: list[bytes | None] = [None]
            if has_encrypted:
                pwd_list.extend(p.encode() for p in passwords)
                # Also try common defaults
                for common in ["infected", "malware", "virus", "123", "1234", "12345", "123456", "password"]:
                    pwd_bytes = common.encode()
                    if pwd_bytes not in pwd_list:
                        pwd_list.append(pwd_bytes)

            for info in zf.infolist():
                if info.is_dir():
                    continue
                if len(results) >= MAX_EXTRACT_FILES:
                    logger.warning("Archive file limit reached (%d)", MAX_EXTRACT_FILES)
                    break
                if info.file_size > MAX_EXTRACT_FILE_SIZE:
                    logger.debug("Skipping oversized file: %s (%d bytes)", info.filename, info.file_size)
                    continue
                if total_size + info.file_size > MAX_EXTRACT_TOTAL:
                    logger.warning("Archive total size limit reached")
                    break

                # Try each password
                extracted = False
                for pwd in pwd_list:
                    try:
                        content = zf.read(info.filename, pwd=pwd)
                        results.append(ExtractedFile(info.filename, content))
                        total_size += len(content)
                        extracted = True
                        break
                    except (RuntimeError, zipfile.BadZipFile):
                        continue

                if not extracted:
                    logger.debug("Could not extract %s (wrong password?)", info.filename)

    except zipfile.BadZipFile:
        logger.debug("Invalid ZIP file")
    except Exception:
        logger.exception("ZIP extraction failed")

    return results


def _extract_rar(data: bytes, passwords: list[str]) -> list[ExtractedFile]:
    """Extract a RAR archive using rarfile (requires unrar binary)."""
    results: list[ExtractedFile] = []

    try:
        import rarfile
    except ImportError:
        logger.warning("rarfile not installed — cannot extract RAR archives")
        return results

    total_size = 0

    with tempfile.NamedTemporaryFile(suffix=".rar", delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name

    try:
        rf = rarfile.RarFile(tmp_path)

        # Build password list
        pwd_list: list[str | None] = [None]
        pwd_list.extend(passwords)
        for common in ["infected", "malware", "virus", "123", "1234", "12345", "123456", "password"]:
            if common not in pwd_list:
                pwd_list.append(common)

        for info in rf.infolist():
            if info.is_dir():
                continue
            if len(results) >= MAX_EXTRACT_FILES:
                break
            if info.file_size > MAX_EXTRACT_FILE_SIZE:
                continue
            if total_size + info.file_size > MAX_EXTRACT_TOTAL:
                break

            extracted = False
            for pwd in pwd_list:
                try:
                    content = rf.read(info.filename, pwd=pwd)
                    results.append(ExtractedFile(info.filename, content))
                    total_size += len(content)
                    extracted = True
                    break
                except (rarfile.BadRarFile, RuntimeError):
                    continue

            if not extracted:
                logger.debug("Could not extract %s from RAR", info.filename)

        rf.close()
    except Exception:
        logger.exception("RAR extraction failed")
    finally:
        os.unlink(tmp_path)

    return results


def _extract_tar(data: bytes, filename: str) -> list[ExtractedFile]:
    """Extract a tar archive (optionally gzipped or bz2)."""
    import tarfile

    results: list[ExtractedFile] = []
    total_size = 0

    lower = filename.lower()
    if lower.endswith((".tar.gz", ".tgz")):
        mode = "r:gz"
    elif lower.endswith(".tar.bz2"):
        mode = "r:bz2"
    else:
        mode = "r:"

    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode=mode) as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue
                # Security: reject path traversal
                if member.name.startswith("/") or ".." in member.name:
                    continue
                if len(results) >= MAX_EXTRACT_FILES:
                    break
                if member.size > MAX_EXTRACT_FILE_SIZE:
                    continue
                if total_size + member.size > MAX_EXTRACT_TOTAL:
                    break

                f = tf.extractfile(member)
                if f is None:
                    continue
                content = f.read()
                results.append(ExtractedFile(member.name, content))
                total_size += len(content)
    except Exception:
        logger.exception("Tar extraction failed")

    return results


def _extract_zip_from_path(path: str, passwords: list[str]) -> list[ExtractedFile]:
    """Extract a ZIP archive from a file path (avoids reading entire archive into memory)."""
    results: list[ExtractedFile] = []
    total_size = 0

    try:
        with zipfile.ZipFile(path) as zf:
            has_encrypted = any(
                info.flag_bits & 0x1 for info in zf.infolist()
            )

            pwd_list: list[bytes | None] = [None]
            if has_encrypted:
                pwd_list.extend(p.encode() for p in passwords)
                for common in ["infected", "malware", "virus", "123", "1234", "12345", "123456", "password"]:
                    pwd_bytes = common.encode()
                    if pwd_bytes not in pwd_list:
                        pwd_list.append(pwd_bytes)

            for info in zf.infolist():
                if info.is_dir():
                    continue
                if len(results) >= MAX_EXTRACT_FILES:
                    logger.warning("Archive file limit reached (%d)", MAX_EXTRACT_FILES)
                    break
                if info.file_size > MAX_EXTRACT_FILE_SIZE:
                    logger.debug("Skipping oversized file: %s (%d bytes)", info.filename, info.file_size)
                    continue
                if total_size + info.file_size > MAX_EXTRACT_TOTAL:
                    logger.warning("Archive total size limit reached")
                    break

                extracted = False
                for pwd in pwd_list:
                    try:
                        content = zf.read(info.filename, pwd=pwd)
                        results.append(ExtractedFile(info.filename, content))
                        total_size += len(content)
                        extracted = True
                        break
                    except (RuntimeError, zipfile.BadZipFile):
                        continue

                if not extracted:
                    logger.debug("Could not extract %s (wrong password?)", info.filename)

    except zipfile.BadZipFile:
        logger.debug("Invalid ZIP file")
    except Exception:
        logger.exception("ZIP extraction failed")

    return results


def _extract_rar_from_path(path: str, passwords: list[str]) -> list[ExtractedFile]:
    """Extract a RAR archive from a file path."""
    results: list[ExtractedFile] = []

    try:
        import rarfile
    except ImportError:
        logger.warning("rarfile not installed — cannot extract RAR archives")
        return results

    total_size = 0

    try:
        rf = rarfile.RarFile(path)

        pwd_list: list[str | None] = [None]
        pwd_list.extend(passwords)
        for common in ["infected", "malware", "virus", "123", "1234", "12345", "123456", "password"]:
            if common not in pwd_list:
                pwd_list.append(common)

        for info in rf.infolist():
            if info.is_dir():
                continue
            if len(results) >= MAX_EXTRACT_FILES:
                break
            if info.file_size > MAX_EXTRACT_FILE_SIZE:
                continue
            if total_size + info.file_size > MAX_EXTRACT_TOTAL:
                break

            extracted = False
            for pwd in pwd_list:
                try:
                    content = rf.read(info.filename, pwd=pwd)
                    results.append(ExtractedFile(info.filename, content))
                    total_size += len(content)
                    extracted = True
                    break
                except (rarfile.BadRarFile, RuntimeError):
                    continue

            if not extracted:
                logger.debug("Could not extract %s from RAR", info.filename)

        rf.close()
    except Exception:
        logger.exception("RAR extraction failed")

    return results


def _extract_tar_from_path(path: str, filename: str) -> list[ExtractedFile]:
    """Extract a tar archive from a file path."""
    import tarfile

    results: list[ExtractedFile] = []
    total_size = 0

    lower = filename.lower()
    if lower.endswith((".tar.gz", ".tgz")):
        mode = "r:gz"
    elif lower.endswith(".tar.bz2"):
        mode = "r:bz2"
    else:
        mode = "r:"

    try:
        with tarfile.open(path, mode=mode) as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue
                if member.name.startswith("/") or ".." in member.name:
                    continue
                if len(results) >= MAX_EXTRACT_FILES:
                    break
                if member.size > MAX_EXTRACT_FILE_SIZE:
                    continue
                if total_size + member.size > MAX_EXTRACT_TOTAL:
                    break

                f = tf.extractfile(member)
                if f is None:
                    continue
                content = f.read()
                results.append(ExtractedFile(member.name, content))
                total_size += len(content)
    except Exception:
        logger.exception("Tar extraction failed")

    return results


def analyze_extracted_files(files: list[ExtractedFile]) -> FileAnalysis:
    """Analyze extracted files and produce a summary with searchable text.

    Returns structured analysis including:
    - File inventory (names, sizes, types)
    - Concatenated text content for watch term matching
    - Credential indicators
    """
    text_parts: list[str] = []
    file_inventory: list[dict[str, Any]] = []
    credential_indicators: list[str] = []
    total_size = 0

    for ef in files:
        total_size += ef.size
        entry: dict[str, Any] = {
            "filename": ef.filename,
            "size": ef.size,
            "sha256": ef.sha256,
            "extension": ef.extension,
        }
        file_inventory.append(entry)

        if ef.is_text:
            text = ef.text_content()
            # Strip null bytes — PostgreSQL can't store \x00 in text/jsonb
            text = text.replace("\x00", "")
            # Truncate very large text files for matching
            if len(text) > 100_000:
                text = text[:100_000]
            text_parts.append(f"--- {ef.filename} ---\n{text}")

            # Check for credential patterns
            if ef.is_credential_file:
                cred_hits = _scan_for_credentials(text, ef.filename)
                credential_indicators.extend(cred_hits)

    return FileAnalysis(
        file_count=len(files),
        total_size=total_size,
        file_inventory=file_inventory,
        text_content="\n\n".join(text_parts),
        credential_indicators=credential_indicators,
    )


class FileAnalysis:
    """Result of analyzing extracted archive contents."""

    __slots__ = (
        "file_count", "total_size", "file_inventory",
        "text_content", "credential_indicators",
    )

    def __init__(
        self,
        file_count: int,
        total_size: int,
        file_inventory: list[dict[str, Any]],
        text_content: str,
        credential_indicators: list[str],
    ):
        self.file_count = file_count
        self.total_size = total_size
        self.file_inventory = file_inventory
        self.text_content = text_content
        self.credential_indicators = credential_indicators

    def to_dict(self) -> dict:
        return {
            "file_count": self.file_count,
            "total_size": self.total_size,
            "files": self.file_inventory[:100],  # Cap for storage
            "has_credentials": len(self.credential_indicators) > 0,
            "credential_count": len(self.credential_indicators),
            "credential_samples": self.credential_indicators[:20],
        }


# Patterns for credential detection in extracted text
_CRED_PATTERNS = [
    re.compile(r"(?:user(?:name)?|login|email)\s*[:\t]\s*\S+.*?(?:pass(?:word)?|pwd)\s*[:\t]\s*\S+", re.IGNORECASE),
    re.compile(r"\S+@\S+\.\S+\s*[:\|;]\s*\S+"),  # email:password
    re.compile(r"https?://\S+:\S+@\S+"),  # URL with embedded creds
]


def _scan_for_credentials(text: str, filename: str) -> list[str]:
    """Scan text content for credential-like patterns."""
    hits: list[str] = []
    lines = text.split("\n")

    # Check for bulk credential patterns (email:pass format)
    email_pass_count = 0
    for line in lines[:5000]:  # Cap line scanning
        line = line.strip()
        if not line:
            continue
        for pattern in _CRED_PATTERNS:
            if pattern.search(line):
                email_pass_count += 1
                if email_pass_count <= 5:
                    # Store redacted sample
                    redacted = line[:40] + "..." if len(line) > 40 else line
                    hits.append(f"{filename}: {redacted}")
                break

    if email_pass_count > 5:
        hits.append(f"{filename}: ~{email_pass_count} credential-like lines total")

    return hits


def upload_to_s3(s3_key: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
    """Upload a file to S3/MinIO. Returns True on success."""
    try:
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=Config(signature_version="s3v4"),
        )
        client.put_object(
            Bucket=settings.s3_bucket,
            Key=s3_key,
            Body=data,
            ContentType=content_type,
        )
        return True
    except Exception:
        logger.exception("S3 upload failed for %s", s3_key)
        return False
