import xml.etree.ElementTree as ET

import pytest

from src.edu_cti.sources.rss import international_rss


def test_international_rss_current_feed_urls_are_configured():
    feeds = {entry[0]: entry[1] for entry in international_rss.INTERNATIONAL_FEEDS}

    assert feeds["ncsc_fi"] == "https://www.kyberturvallisuuskeskus.fi/feed/rss/en"
    assert (
        feeds["cccs_ca"]
        == "https://www.cyber.gc.ca/api/cccs/atom/v1/get?feed=alerts_advisories&lang=en"
    )


def test_international_rss_parser_recovers_malformed_feed_xml():
    malformed = b"""<?xml version="1.0" encoding="UTF-8"?>
    <rss><channel><item><title>University ransomware & breach</title></item></channel></rss>
    """

    with pytest.raises(ET.ParseError):
        ET.fromstring(malformed)

    root = international_rss._parse_feed_xml(
        malformed,
        malformed.decode("utf-8", errors="replace"),
        "utf-8",
        "test_feed",
    )

    assert root.findall(".//item")
    assert root.find(".//title").text.startswith("University ransomware")
