"""Tests for OCR processing of image attachments."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from darkdisco.pipeline.ocr import (
    OCRResult,
    extract_text_from_image,
    is_image,
    is_image_media_type,
)


class TestIsImage:
    def test_common_image_extensions(self):
        assert is_image("screenshot.png") is True
        assert is_image("photo.jpg") is True
        assert is_image("image.jpeg") is True
        assert is_image("scan.bmp") is True
        assert is_image("capture.tiff") is True
        assert is_image("pic.webp") is True

    def test_case_insensitive(self):
        assert is_image("PHOTO.PNG") is True
        assert is_image("Image.JPG") is True

    def test_non_image_extensions(self):
        assert is_image("document.pdf") is False
        assert is_image("archive.zip") is False
        assert is_image("data.txt") is False
        assert is_image("unknown") is False


class TestIsImageMediaType:
    def test_telegram_photo_types(self):
        assert is_image_media_type("MessageMediaPhoto") is True
        assert is_image_media_type("Photo") is True

    def test_non_image_types(self):
        assert is_image_media_type("MessageMediaDocument") is False
        assert is_image_media_type(None) is False
        assert is_image_media_type("") is False


class TestOCRResult:
    def test_has_text_with_content(self):
        result = OCRResult(text="Hello World", confidence=85.0)
        assert result.has_text is True

    def test_has_text_empty(self):
        result = OCRResult(text="", confidence=0.0)
        assert result.has_text is False

    def test_has_text_whitespace_only(self):
        result = OCRResult(text="   \n  ", confidence=10.0)
        assert result.has_text is False


class TestExtractTextFromImage:
    def test_empty_data_returns_none(self):
        assert extract_text_from_image(b"", "test.png") is None

    def test_oversized_image_returns_none(self):
        # 11 MB of data
        data = b"\x00" * (11 * 1024 * 1024)
        assert extract_text_from_image(data, "big.png") is None

    @patch("darkdisco.pipeline.ocr.settings")
    def test_ocr_disabled_returns_none(self, mock_settings):
        mock_settings.ocr_enabled = False
        result = extract_text_from_image(b"\x89PNG\r\n", "test.png")
        assert result is None

    @patch("darkdisco.pipeline.ocr.settings")
    def test_successful_ocr_extraction(self, mock_settings):
        """Test OCR with mocked PIL and pytesseract."""
        mock_settings.ocr_enabled = True
        mock_settings.ocr_min_confidence = 25.0

        mock_image = MagicMock()
        mock_image.mode = "RGB"

        ocr_data = {
            "text": ["First", "National", "Bank", "Balance:", "$5,000"],
            "conf": [90, 88, 92, 85, 87],
        }

        with patch.dict("sys.modules", {
            "PIL": MagicMock(),
            "PIL.Image": MagicMock(),
            "pytesseract": MagicMock(),
        }):
            import sys
            mock_pil = sys.modules["PIL.Image"]
            mock_pil.open.return_value = mock_image

            mock_tesseract = sys.modules["pytesseract"]
            mock_tesseract.Output.DICT = "dict"
            mock_tesseract.image_to_data.return_value = ocr_data

            result = extract_text_from_image(b"\x89PNG\r\n\x1a\n", "screenshot.png")

        assert result is not None
        assert result.has_text
        assert "First" in result.text
        assert "National" in result.text
        assert "Bank" in result.text
        assert result.confidence > 80
        assert result.engine == "tesseract"

    @patch("darkdisco.pipeline.ocr.settings")
    def test_low_confidence_discards_text(self, mock_settings):
        """OCR results below confidence threshold are discarded."""
        mock_settings.ocr_enabled = True
        mock_settings.ocr_min_confidence = 50.0

        mock_image = MagicMock()
        mock_image.mode = "RGB"

        ocr_data = {
            "text": ["garbled", "noise"],
            "conf": [10, 15],
        }

        with patch.dict("sys.modules", {
            "PIL": MagicMock(),
            "PIL.Image": MagicMock(),
            "pytesseract": MagicMock(),
        }):
            import sys
            mock_pil = sys.modules["PIL.Image"]
            mock_pil.open.return_value = mock_image

            mock_tesseract = sys.modules["pytesseract"]
            mock_tesseract.Output.DICT = "dict"
            mock_tesseract.image_to_data.return_value = ocr_data

            result = extract_text_from_image(b"\x89PNG\r\n\x1a\n", "noise.png")

        assert result is not None
        assert not result.has_text  # text was discarded due to low confidence


class TestOCRInPipeline:
    """Test OCR integration in _process_file_mentions."""

    def test_image_mention_gets_ocr_text_appended(self):
        """Image attachments should have OCR text appended to content."""
        from darkdisco.pipeline.worker import _process_file_mentions

        class FakeMention:
            content = "Check this screenshot"
            metadata = {
                "file_data": b"\x89PNG\r\n\x1a\n",
                "file_name": "screenshot.png",
                "media_type": "MessageMediaPhoto",
                "has_media": True,
            }

        mock_ocr_result = OCRResult(
            text="First National Bank Account Balance $50,000",
            confidence=90.0,
        )

        # Patch _ocr_with_dedup (wraps extract_text_from_image with cache)
        with patch("darkdisco.pipeline.files.upload_to_s3", return_value=True), \
             patch("darkdisco.pipeline.worker._ocr_with_dedup", return_value=mock_ocr_result):
            result = _process_file_mentions([FakeMention()])

        mention = result[0]
        assert "ocr_text" in mention.metadata
        assert "First National Bank" in mention.metadata["ocr_text"]
        assert mention.metadata["ocr_confidence"] == 90.0
        assert "OCR text from screenshot.png" in mention.content

    def test_non_image_non_archive_no_ocr(self):
        """Non-image, non-archive files should not trigger OCR."""
        from darkdisco.pipeline.worker import _process_file_mentions

        class FakeMention:
            content = "Here is a PDF"
            metadata = {
                "file_data": b"%PDF-1.4",
                "file_name": "document.pdf",
                "media_type": "MessageMediaDocument",
            }

        with patch("darkdisco.pipeline.files.upload_to_s3", return_value=True):
            result = _process_file_mentions([FakeMention()])

        assert "ocr_text" not in result[0].metadata


class TestOCRDedup:
    """Test image deduplication before OCR processing."""

    def test_cache_hit_skips_ocr(self):
        """When image hash is in cache, OCR engine should not run."""
        from darkdisco.pipeline.worker import _ocr_with_dedup

        cached_result = MagicMock()
        cached_result.ocr_text = "Cached OCR text"
        cached_result.confidence = 85.0
        cached_result.engine = "easyocr"

        mock_session = MagicMock()
        mock_session.get.return_value = cached_result

        with patch("darkdisco.pipeline.worker._get_sync_session", return_value=mock_session), \
             patch("darkdisco.pipeline.ocr.extract_text_from_image") as mock_ocr:
            result = _ocr_with_dedup(b"\x89PNG", "test.png", "abc123hash")

        # OCR engine should NOT have been called
        mock_ocr.assert_not_called()
        assert result.text == "Cached OCR text"
        assert result.confidence == 85.0

    def test_cache_miss_runs_ocr_and_stores(self):
        """When image hash is not in cache, run OCR and store result."""
        from darkdisco.pipeline.worker import _ocr_with_dedup

        mock_session = MagicMock()
        mock_session.get.return_value = None  # Cache miss

        mock_ocr_result = OCRResult(text="New OCR text", confidence=90.0)

        with patch("darkdisco.pipeline.worker._get_sync_session", return_value=mock_session), \
             patch("darkdisco.pipeline.ocr.extract_text_from_image", return_value=mock_ocr_result):
            result = _ocr_with_dedup(b"\x89PNG", "test.png", "abc123hash")

        assert result.text == "New OCR text"
        # Should have committed the cache entry
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_duplicate_image_in_pipeline_uses_cache(self):
        """Same image posted in two mentions should OCR only once."""
        from darkdisco.pipeline.worker import _process_file_mentions

        image_data = b"\x89PNG\r\n\x1a\nSAMEIMAGE"

        class FakeMention1:
            content = "First post"
            metadata = {
                "file_data": image_data,
                "file_name": "screenshot.png",
                "media_type": "MessageMediaPhoto",
            }

        class FakeMention2:
            content = "Second post of same image"
            metadata = {
                "file_data": image_data,
                "file_name": "screenshot2.png",
                "media_type": "MessageMediaPhoto",
            }

        mock_ocr_result = OCRResult(
            text="Bank Account Balance",
            confidence=88.0,
        )

        with patch("darkdisco.pipeline.files.upload_to_s3", return_value=True), \
             patch("darkdisco.pipeline.worker._ocr_with_dedup", return_value=mock_ocr_result) as mock_dedup:
            result = _process_file_mentions([FakeMention1(), FakeMention2()])

        # Both mentions should have called _ocr_with_dedup with the same hash
        assert mock_dedup.call_count == 2
        # Both calls should use the same sha256
        hash1 = mock_dedup.call_args_list[0][0][2]
        hash2 = mock_dedup.call_args_list[1][0][2]
        assert hash1 == hash2  # Same image = same hash

        # Both mentions should have OCR text
        assert "ocr_text" in result[0].metadata
        assert "ocr_text" in result[1].metadata
