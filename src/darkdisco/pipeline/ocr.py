"""OCR processing for image analysis — extract text from screenshots and photos.

Actors frequently share screenshots of bank dashboards, phishing panel results,
and OTP interception successes. This module runs OCR on image attachments to
extract text for watch term matching and finding creation.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass

from darkdisco.config import settings

logger = logging.getLogger(__name__)

# Image extensions we attempt OCR on
_IMAGE_EXTENSIONS = frozenset({
    ".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".tif", ".webp", ".gif",
})

# Max image size for OCR processing (10 MB)
_MAX_OCR_SIZE = 10 * 1024 * 1024


@dataclass
class OCRResult:
    """Result of OCR processing on a single image."""

    text: str
    confidence: float  # 0.0-100.0, -1 if unavailable
    engine: str = "tesseract"

    @property
    def has_text(self) -> bool:
        return bool(self.text and self.text.strip())


def is_image(filename: str) -> bool:
    """Check if a filename looks like a supported image format."""
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in _IMAGE_EXTENSIONS)


def is_image_media_type(media_type: str | None) -> bool:
    """Check if a Telegram media type indicates a photo/image."""
    if not media_type:
        return False
    return media_type in ("MessageMediaPhoto", "Photo")


def extract_text_from_image(image_data: bytes, filename: str = "image.png") -> OCRResult | None:
    """Run OCR on image bytes and return extracted text.

    Returns None if OCR is not available (tesseract not installed) or fails.
    """
    if not image_data:
        return None

    if len(image_data) > _MAX_OCR_SIZE:
        logger.debug("Image too large for OCR: %d bytes (max %d)", len(image_data), _MAX_OCR_SIZE)
        return None

    if not settings.ocr_enabled:
        return None

    try:
        from PIL import Image
        import pytesseract
    except ImportError:
        logger.warning("OCR dependencies not installed (Pillow, pytesseract) — skipping OCR")
        return None

    try:
        image = Image.open(io.BytesIO(image_data))

        # Convert to RGB if necessary (handles RGBA, palette, etc.)
        if image.mode not in ("L", "RGB"):
            image = image.convert("RGB")

        # Run tesseract with OSD (orientation/script detection) for confidence
        data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)

        # Extract text and compute average confidence
        words = []
        confidences = []
        for i, text in enumerate(data["text"]):
            text = text.strip()
            if text:
                words.append(text)
                conf = data["conf"][i]
                if isinstance(conf, (int, float)) and conf >= 0:
                    confidences.append(float(conf))

        full_text = " ".join(words)
        avg_confidence = sum(confidences) / len(confidences) if confidences else -1.0

        # Filter out noise — if confidence is very low, the image likely
        # doesn't contain readable text
        if avg_confidence >= 0 and avg_confidence < settings.ocr_min_confidence:
            logger.debug(
                "OCR confidence too low (%.1f < %.1f) for %s, discarding",
                avg_confidence, settings.ocr_min_confidence, filename,
            )
            return OCRResult(text="", confidence=avg_confidence)

        if full_text.strip():
            logger.info(
                "OCR extracted %d chars (confidence=%.1f) from %s",
                len(full_text), avg_confidence, filename,
            )

        return OCRResult(text=full_text, confidence=avg_confidence)

    except Exception:
        logger.exception("OCR failed for %s", filename)
        return None
