"""BIN lookup enrichment tests.

Tests extract_bin_prefixes, _is_plausible_bin, and the BINEnrichmentResult serialization.
"""

from __future__ import annotations

from darkdisco.enrichment.bin_lookup import (
    BINEnrichmentResult,
    BINLookupResult,
    _is_plausible_bin,
    extract_bin_prefixes,
)


class TestExtractBinPrefixes:
    def test_full_card_number(self):
        prefixes = extract_bin_prefixes("Card: 4532015112830366")
        assert any(p.startswith("4532") for p in prefixes)

    def test_card_with_separators(self):
        prefixes = extract_bin_prefixes("4532-0151-1283-0366")
        assert any(p.startswith("4532") for p in prefixes)

    def test_dump_format(self):
        prefixes = extract_bin_prefixes("4532015112830366=2512101xxxxx")
        assert any(p.startswith("4532") for p in prefixes)

    def test_standalone_bin(self):
        prefixes = extract_bin_prefixes("BIN 453201 for sale")
        assert "453201" in prefixes

    def test_no_bins_in_text(self):
        prefixes = extract_bin_prefixes("No card numbers here at all")
        assert len(prefixes) == 0

    def test_multiple_cards(self):
        content = "4532015112830366 and 5412751234567890"
        prefixes = extract_bin_prefixes(content)
        assert any(p.startswith("4532") for p in prefixes)
        assert any(p.startswith("5412") for p in prefixes)

    def test_deduplication(self):
        content = "4532015112830366 4532015112830366"
        prefixes = extract_bin_prefixes(content)
        # Should not have duplicate prefixes
        assert len(prefixes) == len(set(prefixes))


class TestIsPlausibleBin:
    def test_visa_range(self):
        assert _is_plausible_bin("453201") is True

    def test_mastercard_range(self):
        assert _is_plausible_bin("541275") is True

    def test_invalid_first_digit(self):
        assert _is_plausible_bin("112345") is False

    def test_too_short(self):
        assert _is_plausible_bin("4532") is False

    def test_date_like_pattern_rejected(self):
        assert _is_plausible_bin("202603") is False

    def test_expiry_like_pattern_rejected(self):
        # 26/03 looks like an expiry date
        assert _is_plausible_bin("260312") is False


class TestBINEnrichmentResult:
    def test_to_dict_empty(self):
        result = BINEnrichmentResult()
        d = result.to_dict()
        assert d["bins_found"] == []
        assert d["unique_issuers"] == []
        assert d["card_count"] == 0

    def test_to_dict_with_data(self):
        result = BINEnrichmentResult(
            bins_found=[BINLookupResult(bin_prefix="453201", found=True, issuer_name="Test Bank")],
            unique_issuers=["Test Bank"],
            unique_brands=["visa"],
            card_count=3,
        )
        d = result.to_dict()
        assert len(d["bins_found"]) == 1
        assert d["unique_issuers"] == ["Test Bank"]
        assert d["card_count"] == 3
