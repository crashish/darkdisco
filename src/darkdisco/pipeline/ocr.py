"""OCR processing for image analysis — extract text from screenshots and photos.

Uses EasyOCR as the primary engine (handles rotated/skewed text from phone
camera photos). Falls back to Tesseract if EasyOCR is unavailable.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass

from darkdisco.config import settings

logger = logging.getLogger(__name__)

_IMAGE_EXTENSIONS = frozenset({
    ".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".tif", ".webp", ".gif",
})

_MAX_OCR_SIZE = 10 * 1024 * 1024  # 10 MB

# Lazy-loaded EasyOCR reader (heavy init, reuse across calls)
_easyocr_reader = None


@dataclass
class OCRResult:
    text: str
    confidence: float  # 0.0-1.0, -1 if unavailable
    engine: str = "easyocr"

    @property
    def has_text(self) -> bool:
        return bool(self.text and self.text.strip())


def is_image(filename: str | None) -> bool:
    if not filename:
        return False
    return any(filename.lower().endswith(ext) for ext in _IMAGE_EXTENSIONS)


def is_image_media_type(media_type: str | None) -> bool:
    if not media_type:
        return False
    return media_type in ("MessageMediaPhoto", "Photo")


def _get_easyocr_reader():
    """Lazy-load EasyOCR reader (downloads models on first use)."""
    global _easyocr_reader
    if _easyocr_reader is None:
        try:
            import easyocr
            _easyocr_reader = easyocr.Reader(["en"], gpu=False, verbose=False)
            logger.info("EasyOCR reader initialized")
        except ImportError:
            logger.warning("EasyOCR not installed — OCR will use Tesseract fallback")
            return None
    return _easyocr_reader


def extract_text_from_image(image_data: bytes, filename: str = "image.png") -> OCRResult | None:
    """Run OCR on image bytes and return extracted text.

    Tries EasyOCR first (handles rotated/skewed text), falls back to Tesseract.
    Returns None if OCR is disabled or all engines fail.
    """
    if not image_data or len(image_data) > _MAX_OCR_SIZE:
        return None

    if not settings.ocr_enabled:
        return None

    # Try EasyOCR first (handles rotation/skew natively)
    result = _try_easyocr(image_data, filename)
    if result is not None:
        return result

    # Fallback to Tesseract
    result = _try_tesseract(image_data, filename)
    if result is not None:
        return result

    logger.debug("All OCR engines failed for %s", filename)
    return None


def _try_easyocr(image_data: bytes, filename: str) -> OCRResult | None:
    """Extract text using EasyOCR (handles rotated/skewed images)."""
    reader = _get_easyocr_reader()
    if reader is None:
        return None

    try:
        from PIL import Image
        import numpy as np

        image = Image.open(io.BytesIO(image_data))
        if image.mode not in ("L", "RGB"):
            image = image.convert("RGB")

        # EasyOCR works on numpy arrays
        img_array = np.array(image)
        results = reader.readtext(img_array)

        if not results:
            return OCRResult(text="", confidence=0.0, engine="easyocr")

        # results is list of (bbox, text, confidence)
        texts = []
        confidences = []
        for _bbox, text, conf in results:
            text = text.strip()
            if text:
                texts.append(text)
                confidences.append(conf)

        full_text = " ".join(texts)
        avg_conf = sum(confidences) / len(confidences) if confidences else 0.0

        if avg_conf < settings.ocr_min_confidence:
            logger.debug("EasyOCR confidence %.2f below threshold %.2f for %s",
                         avg_conf, settings.ocr_min_confidence, filename)
            return OCRResult(text="", confidence=avg_conf, engine="easyocr")

        if full_text.strip():
            logger.info("EasyOCR extracted %d chars (conf=%.2f) from %s",
                        len(full_text), avg_conf, filename)

        return OCRResult(text=full_text, confidence=avg_conf, engine="easyocr")

    except Exception:
        logger.debug("EasyOCR failed for %s", filename, exc_info=True)
        return None


def _try_tesseract(image_data: bytes, filename: str) -> OCRResult | None:
    """Extract text using Tesseract (fallback, requires aligned text)."""
    try:
        from PIL import Image
        import pytesseract
    except ImportError:
        return None

    try:
        image = Image.open(io.BytesIO(image_data))
        if image.mode not in ("L", "RGB"):
            image = image.convert("RGB")

        data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)

        words = []
        confidences = []
        for i, text in enumerate(data["text"]):
            text = text.strip()
            if text:
                words.append(text)
                conf = data["conf"][i]
                if isinstance(conf, (int, float)) and conf >= 0:
                    confidences.append(float(conf) / 100.0)  # Normalize to 0-1

        full_text = " ".join(words)
        avg_conf = sum(confidences) / len(confidences) if confidences else -1.0

        if 0 <= avg_conf < settings.ocr_min_confidence:
            return OCRResult(text="", confidence=avg_conf, engine="tesseract")

        if full_text.strip():
            logger.info("Tesseract extracted %d chars (conf=%.2f) from %s",
                        len(full_text), avg_conf, filename)

        return OCRResult(text=full_text, confidence=avg_conf, engine="tesseract")

    except Exception:
        logger.debug("Tesseract failed for %s", filename, exc_info=True)
        return None
