"""File extraction utilities — archive handling, S3 upload, and content analysis.

Provides the functions imported by worker.py for processing file attachments
on incoming mentions. Supports ZIP, RAR, TAR archives with password handling.
Also provides stream_extract_from_s3() for disk-based extraction of large
archives that can't fit in memory.
"""

from __future__ import annotations

import hashlib
import io
import logging
import mimetypes
import os
import re
import shutil
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass, field
import boto3
from botocore.exceptions import ClientError

from darkdisco.config import settings

logger = logging.getLogger(__name__)

# Max individual extracted file size (50 MB)
_MAX_MEMBER_SIZE = 50 * 1024 * 1024

# Max recursion depth for nested archives
_MAX_ARCHIVE_DEPTH = 3

# Text-like extensions for content extraction
_TEXT_EXTENSIONS = frozenset({
    ".txt", ".csv", ".log", ".json", ".xml", ".html", ".htm",
    ".ini", ".cfg", ".conf", ".dat", ".sql", ".md", ".yaml", ".yml",
    ".tsv", ".lst",
})

# Archive extensions
_ARCHIVE_EXTENSIONS = frozenset({
    ".zip", ".rar", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2",
    ".7z", ".gz", ".bz2",
})

# Password patterns commonly found in Telegram/forum messages
_PASSWORD_PATTERNS = [
    re.compile(r"(?:pass(?:word)?|pwd|пароль)\s*[:=]\s*(.+)", re.IGNORECASE),
    re.compile(r"🔑\s*(.+)"),
    re.compile(r"(?:key|unlock)\s*[:=]\s*(.+)", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# MIME detection
# ---------------------------------------------------------------------------

# Additional MIME types not always in the system registry
_EXTRA_MIME_TYPES = {
    ".7z": "application/x-7z-compressed",
    ".rar": "application/x-rar-compressed",
    ".tgz": "application/gzip",
    ".tbz2": "application/x-bzip2",
    ".csv": "text/csv",
    ".log": "text/plain",
    ".cfg": "text/plain",
    ".conf": "text/plain",
    ".ini": "text/plain",
    ".lst": "text/plain",
    ".dat": "application/octet-stream",
    ".sql": "application/sql",
    ".md": "text/markdown",
    ".yaml": "text/yaml",
    ".yml": "text/yaml",
    ".tsv": "text/tab-separated-values",
}

# Magic bytes for common file types (checked when extension is ambiguous)
_MAGIC_SIGNATURES: list[tuple[bytes, int, str]] = [
    (b"PK\x03\x04", 0, "application/zip"),
    (b"PK\x05\x06", 0, "application/zip"),
    (b"Rar!\x1a\x07", 0, "application/x-rar-compressed"),
    (b"\x1f\x8b", 0, "application/gzip"),
    (b"BZh", 0, "application/x-bzip2"),
    (b"7z\xbc\xaf\x27\x1c", 0, "application/x-7z-compressed"),
    (b"\x89PNG\r\n\x1a\n", 0, "image/png"),
    (b"\xff\xd8\xff", 0, "image/jpeg"),
    (b"GIF87a", 0, "image/gif"),
    (b"GIF89a", 0, "image/gif"),
    (b"RIFF", 0, "image/webp"),  # RIFF....WEBP
    (b"%PDF", 0, "application/pdf"),
]


def detect_mime_type(filename: str, content: bytes | None = None) -> str:
    """Detect MIME type from filename extension and magic bytes."""
    # Try extension first
    ext = ""
    if "." in filename:
        ext = "." + filename.rsplit(".", 1)[-1].lower()
        # Handle double extensions like .tar.gz
        lower = filename.lower()
        if lower.endswith(".tar.gz"):
            ext = ".tar.gz"
        elif lower.endswith(".tar.bz2"):
            ext = ".tar.bz2"

    if ext in _EXTRA_MIME_TYPES:
        return _EXTRA_MIME_TYPES[ext]

    guessed, _ = mimetypes.guess_type(filename)
    if guessed:
        return guessed

    # Fall back to magic bytes
    if content and len(content) >= 8:
        for sig, offset, mime in _MAGIC_SIGNATURES:
            if content[offset:offset + len(sig)] == sig:
                return mime

    return "application/octet-stream"


# ---------------------------------------------------------------------------
# Hex dump utility
# ---------------------------------------------------------------------------

_HEX_DUMP_LIMIT = 4096  # 4KB default


def hex_dump(data: bytes, limit: int = _HEX_DUMP_LIMIT) -> str:
    """Generate xxd-style hex dump with ASCII sidebar.

    Returns a string like:
    00000000: 504b 0304 1400 0000  PK......
    """
    chunk = data[:limit]
    lines: list[str] = []
    for offset in range(0, len(chunk), 16):
        row = chunk[offset:offset + 16]
        hex_part = " ".join(f"{b:02x}" for b in row)
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in row)
        lines.append(f"{offset:08x}: {hex_part:<48s}  {ascii_part}")
    if len(data) > limit:
        lines.append(f"... truncated at {limit} bytes (total: {len(data)} bytes)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ExtractedFileInfo:
    """Metadata and content for a single file extracted from an archive."""

    filename: str
    content: bytes  # raw bytes
    sha256: str = ""
    size: int = 0
    is_text: bool = False
    depth: int = 0  # nesting level (0=top-level, 1=nested once, etc.)
    mime_type: str = ""  # detected MIME type

    def __post_init__(self):
        if not self.sha256:
            self.sha256 = hashlib.sha256(self.content).hexdigest()
        if not self.size:
            self.size = len(self.content)
        if not self.mime_type:
            self.mime_type = detect_mime_type(self.filename, self.content)


@dataclass
class FileAnalysis:
    """Analysis results from scanning extracted archive contents."""

    total_files: int = 0
    text_files: int = 0
    text_content: str = ""
    credential_indicators: list[str] = field(default_factory=list)
    file_types: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "total_files": self.total_files,
            "text_files": self.text_files,
            "text_content_length": len(self.text_content),
            "credential_indicators": self.credential_indicators,
            "file_types": self.file_types,
        }


# ---------------------------------------------------------------------------
# Public API (imported by worker.py)
# ---------------------------------------------------------------------------


def is_archive(filename: str | None) -> bool:
    """Check if a filename looks like a supported archive format."""
    if not filename:
        return False
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in _ARCHIVE_EXTENSIONS)


def extract_passwords(text: str) -> list[str]:
    """Extract potential archive passwords from message text."""
    passwords: list[str] = []
    for pattern in _PASSWORD_PATTERNS:
        for match in pattern.finditer(text):
            pw = match.group(1).strip().strip("\"'`")
            if pw and len(pw) < 200:
                passwords.append(pw)
    return passwords


def extract_archive(
    data: bytes,
    filename: str,
    passwords: list[str] | None = None,
    *,
    _depth: int = 0,
) -> list[ExtractedFileInfo]:
    """Extract files from an in-memory archive, recursing into nested archives.

    Supports ZIP (with optional passwords) and TAR variants.
    Returns a list of ExtractedFileInfo for each extracted member.
    Nested archives are recursively extracted up to _MAX_ARCHIVE_DEPTH levels.
    """
    lower = filename.lower()

    if lower.endswith(".zip"):
        files = _extract_zip(io.BytesIO(data), passwords or [])
    elif lower.endswith((".tar.gz", ".tgz")):
        files = _extract_tar(io.BytesIO(data), mode="r:gz")
    elif lower.endswith((".tar.bz2", ".tbz2")):
        files = _extract_tar(io.BytesIO(data), mode="r:bz2")
    elif lower.endswith(".tar"):
        files = _extract_tar(io.BytesIO(data), mode="r:")
    elif lower.endswith(".rar"):
        files = _extract_rar(io.BytesIO(data))
    else:
        logger.warning("Unsupported archive format: %s", filename)
        return []

    # Set depth on all extracted files
    for f in files:
        f.depth = _depth

    # Recursively extract nested archives
    if _depth < _MAX_ARCHIVE_DEPTH:
        extra: list[ExtractedFileInfo] = []
        for f in files:
            if is_archive(f.filename):
                try:
                    nested = extract_archive(
                        f.content, f.filename, passwords, _depth=_depth + 1
                    )
                    if nested:
                        # Prefix nested filenames with parent archive path
                        parent = f.filename
                        for nf in nested:
                            nf.filename = f"{parent}/{nf.filename}"
                        extra.extend(nested)
                        logger.debug(
                            "Recursively extracted %d files from %s (depth %d)",
                            len(nested), f.filename, _depth + 1,
                        )
                except Exception:
                    logger.warning(
                        "Recursive extraction failed for %s at depth %d",
                        f.filename, _depth + 1,
                    )
        files.extend(extra)

    return files


def analyze_extracted_files(files: list[ExtractedFileInfo]) -> FileAnalysis:
    """Analyze extracted files for text content and credential indicators."""
    analysis = FileAnalysis(total_files=len(files))

    text_parts: list[str] = []
    credential_indicators: list[str] = []

    for ef in files:
        # Track file types
        ext = _get_extension(ef.filename)
        analysis.file_types[ext] = analysis.file_types.get(ext, 0) + 1

        if ef.is_text and ef.content:
            analysis.text_files += 1
            try:
                text = (
                    ef.content.decode("utf-8", errors="replace")
                    if isinstance(ef.content, bytes)
                    else ef.content
                )
            except Exception:
                continue

            text_parts.append(text[:100_000])

            # Scan for credential indicators
            text_lower = text.lower()
            if any(
                kw in text_lower
                for kw in ("password", "login", "credential", "passwd")
            ):
                credential_indicators.append(ef.filename)

    analysis.text_content = "\n\n".join(text_parts)[:500_000]
    analysis.credential_indicators = credential_indicators
    return analysis


def upload_to_s3(s3_key: str, data: bytes) -> bool:
    """Upload bytes to the configured S3 bucket. Returns True on success."""
    try:
        client = _get_s3_client()
        client.put_object(
            Bucket=settings.s3_bucket,
            Key=s3_key,
            Body=data,
        )
        return True
    except Exception:
        logger.exception("S3 upload failed for key %s", s3_key)
        return False


# ---------------------------------------------------------------------------
# Streaming extraction from S3 (for large archives)
# ---------------------------------------------------------------------------


def stream_extract_from_s3(
    s3_key: str,
    archive_filename: str,
    passwords: list[str] | None = None,
) -> list[ExtractedFileInfo]:
    """Download an archive from S3 to temp disk, extract, and return files.

    This avoids loading the entire archive into memory — essential for
    large stealer log archives (1-4 GB). The archive is downloaded to a
    temp directory, extracted on disk, and each member is read individually.

    Returns extracted files as ExtractedFileInfo list. Caller is responsible
    for uploading extracted files to S3 and creating ExtractedFile rows.
    """
    tmpdir = tempfile.mkdtemp(prefix="darkdisco_extract_")
    archive_path = os.path.join(tmpdir, archive_filename)
    extract_dir = os.path.join(tmpdir, "extracted")
    os.makedirs(extract_dir, exist_ok=True)

    try:
        # 1. Stream-download archive from S3 to disk
        logger.info("Downloading s3://%s/%s to disk", settings.s3_bucket, s3_key)
        client = _get_s3_client()
        client.download_file(settings.s3_bucket, s3_key, archive_path)
        archive_size = os.path.getsize(archive_path)
        logger.info("Downloaded %s (%.1f MB)", s3_key, archive_size / 1024 / 1024)

        # 2. Extract archive to disk
        lower = archive_filename.lower()
        if lower.endswith(".zip"):
            _extract_zip_to_disk(archive_path, extract_dir, passwords or [])
        elif lower.endswith((".tar.gz", ".tgz")):
            _extract_tar_to_disk(archive_path, extract_dir, mode="r:gz")
        elif lower.endswith((".tar.bz2", ".tbz2")):
            _extract_tar_to_disk(archive_path, extract_dir, mode="r:bz2")
        elif lower.endswith(".tar"):
            _extract_tar_to_disk(archive_path, extract_dir, mode="r:")
        elif lower.endswith(".rar"):
            _extract_rar_to_disk(archive_path, extract_dir)
        else:
            logger.warning("Unsupported archive format for streaming: %s", archive_filename)
            return []

        # 3. Read extracted files into ExtractedFileInfo objects,
        #    recursing into nested archives (up to _MAX_ARCHIVE_DEPTH).
        files: list[ExtractedFileInfo] = []
        for root, _dirs, filenames in os.walk(extract_dir):
            for fname in filenames:
                filepath = os.path.join(root, fname)
                relpath = os.path.relpath(filepath, extract_dir)

                # Skip oversized files
                try:
                    fsize = os.path.getsize(filepath)
                except OSError:
                    continue
                if fsize > _MAX_MEMBER_SIZE:
                    logger.debug("Skipping oversized file: %s (%d bytes)", relpath, fsize)
                    continue
                if fsize == 0:
                    continue

                try:
                    with open(filepath, "rb") as f:
                        content = f.read()
                except OSError:
                    logger.warning("Failed to read extracted file: %s", relpath)
                    continue

                is_text = _is_text_file(relpath)
                files.append(
                    ExtractedFileInfo(
                        filename=relpath,
                        content=content,
                        is_text=is_text,
                    )
                )

                # Recurse into nested archives (reuse in-memory extraction
                # since nested members are already bounded by _MAX_MEMBER_SIZE)
                if is_archive(relpath):
                    try:
                        nested = extract_archive(
                            content, fname, passwords, _depth=1
                        )
                        if nested:
                            for nf in nested:
                                nf.filename = f"{relpath}/{nf.filename}"
                            files.extend(nested)
                            logger.debug(
                                "Recursively extracted %d files from nested archive %s",
                                len(nested), relpath,
                            )
                    except Exception:
                        logger.warning(
                            "Recursive extraction failed for nested archive %s",
                            relpath,
                        )

        logger.info(
            "Stream-extracted %d files from %s (%d text, %d from nested archives)",
            len(files),
            archive_filename,
            sum(1 for f in files if f.is_text),
            sum(1 for f in files if f.depth > 0),
        )
        return files

    except ClientError:
        logger.exception("S3 download failed for %s", s3_key)
        return []
    except Exception:
        logger.exception("Stream extraction failed for %s", s3_key)
        return []
    finally:
        # Always clean up temp directory
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# In-memory extraction helpers
# ---------------------------------------------------------------------------


def _extract_zip(fileobj, passwords: list[str]) -> list[ExtractedFileInfo]:
    """Extract files from a ZIP archive in memory."""
    files: list[ExtractedFileInfo] = []
    try:
        with zipfile.ZipFile(fileobj) as zf:
            # Try passwords: None first, then provided ones
            pwd_list = [None] + [p.encode() for p in passwords]

            for info in zf.infolist():
                if info.is_dir():
                    continue
                if not _is_safe_path(info.filename):
                    continue
                if info.file_size > _MAX_MEMBER_SIZE:
                    continue

                content = None
                for pwd in pwd_list:
                    try:
                        content = zf.read(info.filename, pwd=pwd)
                        break
                    except (RuntimeError, zipfile.BadZipFile):
                        continue

                if content is None:
                    continue

                files.append(
                    ExtractedFileInfo(
                        filename=info.filename,
                        content=content,
                        is_text=_is_text_file(info.filename),
                    )
                )
    except (zipfile.BadZipFile, Exception) as exc:
        logger.warning("ZIP extraction failed: %s", exc)

    return files


def _extract_tar(fileobj, mode: str = "r:gz") -> list[ExtractedFileInfo]:
    """Extract files from a TAR archive in memory."""
    files: list[ExtractedFileInfo] = []
    try:
        with tarfile.open(fileobj=fileobj, mode=mode) as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue
                if not _is_safe_path(member.name):
                    continue
                if member.size > _MAX_MEMBER_SIZE:
                    continue

                f = tf.extractfile(member)
                if f is None:
                    continue
                content = f.read()
                files.append(
                    ExtractedFileInfo(
                        filename=member.name,
                        content=content,
                        is_text=_is_text_file(member.name),
                    )
                )
    except (tarfile.TarError, Exception) as exc:
        logger.warning("TAR extraction failed: %s", exc)

    return files


def _extract_rar(fileobj) -> list[ExtractedFileInfo]:
    """Extract files from a RAR archive in memory (requires rarfile package)."""
    try:
        import rarfile
    except ImportError:
        logger.warning("rarfile package not installed — RAR extraction unavailable")
        return []

    files: list[ExtractedFileInfo] = []
    try:
        with rarfile.RarFile(fileobj) as rf:
            for info in rf.infolist():
                if info.is_dir():
                    continue
                if not _is_safe_path(info.filename):
                    continue
                if info.file_size > _MAX_MEMBER_SIZE:
                    continue

                content = rf.read(info.filename)
                files.append(
                    ExtractedFileInfo(
                        filename=info.filename,
                        content=content,
                        is_text=_is_text_file(info.filename),
                    )
                )
    except Exception as exc:
        logger.warning("RAR extraction failed: %s", exc)

    return files


# ---------------------------------------------------------------------------
# Disk-based extraction helpers (for streaming large archives)
# ---------------------------------------------------------------------------


def _extract_zip_to_disk(
    archive_path: str, extract_dir: str, passwords: list[str]
) -> None:
    """Extract ZIP archive to disk directory."""
    with zipfile.ZipFile(archive_path) as zf:
        pwd_list = [None] + [p.encode() for p in passwords]

        for info in zf.infolist():
            if info.is_dir():
                continue
            if not _is_safe_path(info.filename):
                continue
            if info.file_size > _MAX_MEMBER_SIZE:
                continue

            dest = os.path.join(extract_dir, info.filename)
            dest_dir = os.path.dirname(dest)
            os.makedirs(dest_dir, exist_ok=True)

            for pwd in pwd_list:
                try:
                    data = zf.read(info.filename, pwd=pwd)
                    with open(dest, "wb") as f:
                        f.write(data)
                    break
                except (RuntimeError, zipfile.BadZipFile):
                    continue


def _extract_tar_to_disk(
    archive_path: str, extract_dir: str, mode: str = "r:gz"
) -> None:
    """Extract TAR archive to disk directory safely."""
    with tarfile.open(archive_path, mode=mode) as tf:
        for member in tf.getmembers():
            if not member.isfile():
                continue
            if not _is_safe_path(member.name):
                continue
            if member.size > _MAX_MEMBER_SIZE:
                continue

            dest = os.path.join(extract_dir, member.name)
            dest_dir = os.path.dirname(dest)
            os.makedirs(dest_dir, exist_ok=True)

            src = tf.extractfile(member)
            if src is None:
                continue
            with open(dest, "wb") as f:
                shutil.copyfileobj(src, f)


def _extract_rar_to_disk(archive_path: str, extract_dir: str) -> None:
    """Extract RAR archive to disk directory."""
    try:
        import rarfile
    except ImportError:
        logger.warning("rarfile package not installed — RAR extraction unavailable")
        return

    with rarfile.RarFile(archive_path) as rf:
        for info in rf.infolist():
            if info.is_dir():
                continue
            if not _is_safe_path(info.filename):
                continue
            if info.file_size > _MAX_MEMBER_SIZE:
                continue

            rf.extract(info.filename, path=extract_dir)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_s3_client = None


def _get_s3_client():
    """Lazily create a boto3 S3 client."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
        )
    return _s3_client


def _is_safe_path(path: str) -> bool:
    """Reject path traversal and absolute paths."""
    return ".." not in path and not path.startswith("/")


def _is_text_file(filename: str) -> bool:
    """Check if a filename looks like a text file."""
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in _TEXT_EXTENSIONS)


def _get_extension(filename: str) -> str:
    """Get the lowercase file extension."""
    if "." in filename:
        return filename.rsplit(".", 1)[-1].lower()
    return ""
